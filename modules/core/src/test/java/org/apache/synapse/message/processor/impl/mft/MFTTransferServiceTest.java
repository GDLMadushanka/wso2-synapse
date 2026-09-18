/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * you may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.synapse.message.processor.impl.mft;

import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2SynapseEnvironment;
import org.apache.synapse.message.processor.impl.AbstractMessageProcessor;
import org.apache.synapse.stream.Transfer;
import org.apache.synapse.stream.TransferStatus;
import org.apache.synapse.stream.TransferStore;
import org.apache.synapse.stream.TransferStores;
import org.apache.synapse.stream.StreamContext;
import org.apache.synapse.stream.StreamException;
import org.apache.synapse.stream.StreamSink;
import org.apache.synapse.stream.StreamSource;
import org.apache.synapse.stream.pipeline.StreamPipeline;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * What the processor does with a transfer it claimed.
 *
 * <p>The interesting behaviour is all in the outcome: a queue that is put back, or deliberately not put
 * back, decides whether a failing transfer recovers, spins, or is quietly abandoned.
 */
public class MFTTransferServiceTest {

    private static final String QUEUE = "mft-queue";

    /** The artifact the transfer was submitted through, which the run's invocation path starts with. */
    private static final String SUBMITTER = "mft-jobs";

    private SynapseConfiguration config;
    private SynapseEnvironment environment;
    private FakeStore queue;

    @Before
    public void setUp() throws Exception {
        config = new SynapseConfiguration();
        AxisConfiguration axisConfiguration = config.getAxisConfiguration();
        ConfigurationContext cfgCtx = new ConfigurationContext(axisConfiguration);
        environment = new Axis2SynapseEnvironment(cfgCtx, config);
        axisConfiguration.addParameter(
                new org.apache.axis2.description.Parameter(SynapseConstants.SYNAPSE_ENV, environment));
        axisConfiguration.addParameter(
                new org.apache.axis2.description.Parameter(SynapseConstants.SYNAPSE_CONFIG, config));
        config.setAxisConfiguration(axisConfiguration);

        queue = new FakeStore();
        TransferStores.register(queue);
    }

    @After
    public void tearDown() {
        TransferStores.register(null);
    }

    /** The ordinary case: it runs, the row leaves the queue by finishing, and nothing is put back. */
    @Test
    public void aTransferThatSucceedsIsNotRequeued() throws Exception {
        RecordingSink sink = new RecordingSink();
        deploy("copy", sink, null);
        queue.waiting.add(transfer(1, Map.of("path", "in/a.csv")));

        runOnce();

        assertEquals("the pipeline must have run", 1, sink.consumed.get());
        assertTrue("a transfer that finished has nothing to put back", queue.requeued.isEmpty());
    }

    /**
     * The descriptor's parameters are how a queued run configures its operators: it has no seed and no
     * incoming message, so a connector operation has nowhere else to read a path from.
     */
    @Test
    public void theDescriptorsParametersReachThePipelineAsProperties() throws Exception {
        RecordingSink sink = new RecordingSink();
        deploy("copy", sink, null);
        Map<String, String> parameters = new LinkedHashMap<>();
        parameters.put("connection", "conn1");
        parameters.put("source.path", "in/a.csv");
        queue.waiting.add(transfer(1, parameters));

        runOnce();

        assertEquals("conn1", sink.properties.get("mft.connection"));
        assertEquals("in/a.csv", sink.properties.get("mft.source.path"));
    }

    /** A retryable failure comes back — that is what makes a transfer survive a server being down. */
    @Test
    public void aRetryableFailureIsRequeuedAfterTheRetryInterval() throws Exception {
        deploy("copy", new FailingSink(new StreamException("connection refused", true)), null);
        queue.waiting.add(transfer(1, Map.of()));

        long before = System.currentTimeMillis();
        runOnce();

        assertEquals(1, queue.requeued.size());
        assertEquals(1L, queue.requeued.get(0).jobId);
        assertTrue("a retry that is claimable immediately spins against whatever failed it",
                queue.requeued.get(0).notBefore >= before + 500L);
    }

    /**
     * A poison transfer must not be retried. An unparseable file or a rejected configuration fails the
     * same way on every attempt, and requeueing it burns the queue for as long as it exists.
     */
    @Test
    public void aNonRetryableFailureIsLeftFailed() throws Exception {
        deploy("copy", new FailingSink(new StreamException("column 3 is not a number", false)), null);
        queue.waiting.add(transfer(1, Map.of()));

        runOnce();

        assertTrue("nothing about this transfer will succeed on a retry", queue.requeued.isEmpty());
    }

    /** Retrying for ever is its own failure mode; the bound is what stops it. */
    @Test
    public void aTransferThatHasExhaustedItsAttemptsIsNotRequeued() throws Exception {
        deploy("copy", new FailingSink(new StreamException("still refused", true)), null);
        // Four attempts already made, and max.delivery.attempts defaults to four.
        queue.waiting.add(transfer(1, Map.of(), 4));

        runOnce();

        assertTrue("the bound must actually bind", queue.requeued.isEmpty());
    }

    /**
     * A processor pointed at a pipeline that is not deployed is a recoverable state: deploy it and the
     * next attempt runs. Failing the transfer outright would make a deployment-ordering mistake
     * permanent.
     */
    @Test
    public void aMissingPipelineIsRequeuedRatherThanFailed() throws Exception {
        queue.waiting.add(transfer(1, Map.of()));

        runOnce(Map.of("pipeline", "not-deployed"));

        assertEquals(1, queue.requeued.size());
        assertEquals(1L, queue.requeued.get(0).jobId);
    }

    /** One poll must not claim more than the node can run, or the pool queues work nothing is watching. */
    @Test
    public void aPollClaimsNoMoreThanTheConfiguredConcurrency() throws Exception {
        deploy("copy", new RecordingSink(), null);
        for (int i = 0; i < 5; i++) {
            queue.waiting.add(transfer(i, Map.of()));
        }

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("concurrency", "2");
        MFTTransferService service = serviceWith(parameters);
        service.execute();

        assertEquals("claimed more than it can run", 3, queue.waiting.size());
    }


    // ------------------------------------------------------------------ hooks and deactivation

    /** Where "email me when the transfer finishes" lives. Nothing in a pipeline can express it. */
    @Test
    public void aSuccessfulTransferRunsTheSuccessSequence() throws Exception {
        deploy("copy", new RecordingSink(), null);
        RecordingSequence done = deploySequence("transferDone");
        queue.waiting.add(transfer(1, Map.of()));

        runOnce(Map.of("sequence", "transferDone"));

        assertEquals(1, done.runs.size());
        assertEquals("1", done.runs.get(0).get("mft.jobId"));
        assertEquals("DONE", done.runs.get(0).get("mft.status"));
    }

    /**
     * A failure that will be retried is a log line, not an event. Four retries producing four alert
     * emails is how an alerting channel gets muted.
     */
    @Test
    public void aRetryableFailureDoesNotRunTheFaultSequence() throws Exception {
        deploy("copy", new FailingSink(new StreamException("connection refused", true)), null);
        RecordingSequence alert = deploySequence("transferGivenUp");
        queue.waiting.add(transfer(1, Map.of()));

        runOnce(Map.of("fault.sequence", "transferGivenUp"));

        assertEquals("it is being retried, so nothing has been given up yet", 0, alert.runs.size());
        assertEquals(1, queue.requeued.size());
    }

    /** Given up means given up: that is the moment worth telling someone about. */
    @Test
    public void aTransferThatIsGivenUpRunsTheFaultSequence() throws Exception {
        deploy("copy", new FailingSink(new StreamException("column 3 is not a number", false)), null);
        RecordingSequence alert = deploySequence("transferGivenUp");
        queue.waiting.add(transfer(1, Map.of()));

        runOnce(Map.of("fault.sequence", "transferGivenUp"));

        assertEquals(1, alert.runs.size());
        assertEquals("FAILED", alert.runs.get(0).get("mft.status"));
        assertEquals("column 3 is not a number", alert.runs.get(0).get("mft.error.message"));
        assertEquals("false", alert.runs.get(0).get("mft.retryable"));
    }

    /**
     * The hazard this exists for: a dead destination fails every transfer identically, and without a
     * stop the processor empties a queue of thousands into FAILED in minutes.
     */
    @Test
    public void enoughConsecutiveGiveUpsDeactivateTheProcessorAndSaySo() throws Exception {
        deploy("copy", new FailingSink(new StreamException("host unreachable", false)), null);
        RecordingSequence alert = deploySequence("queueStalled");
        for (int i = 0; i < 3; i++) {
            queue.waiting.add(transfer(i, Map.of()));
        }

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("deactivate.after", "3");
        parameters.put("message.processor.deactivate.sequence", "queueStalled");
        parameters.put("concurrency", "1");
        MFTTransferService service = serviceWith(parameters);
        FakeProcessor processor = (FakeProcessor) processorOf(service);
        for (int i = 0; i < 3; i++) {
            service.execute();
            waitForQuiet(service);
        }

        assertTrue("three in a row is a destination that is down, not a bad file",
                processor.deactivated);
        assertEquals(1, alert.runs.size());
        assertNotNull("the hook must say why the queue stalled",
                alert.runs.get(0).get("mft.deactivated.reason"));
    }

    /** One bad file must not stop nine thousand good ones. */
    @Test
    public void oneGiveUpDoesNotDeactivateByDefault() throws Exception {
        deploy("copy", new FailingSink(new StreamException("one bad file", false)), null);
        queue.waiting.add(transfer(1, Map.of()));

        Map<String, Object> parameters = new HashMap<>();
        MFTTransferService service = serviceWith(parameters);
        FakeProcessor processor = (FakeProcessor) processorOf(service);
        service.execute();
        waitForQuiet(service);

        assertFalse("MFT transfers are independent, unlike a forwarding queue", processor.deactivated);
    }

    /** A hook that throws must not undo a transfer that moved its bytes. */
    @Test
    public void aFailingHookDoesNotFailTheTransfer() throws Exception {
        deploy("copy", new RecordingSink(), null);
        config.addSequence("transferDone", new ThrowingSequence());
        queue.waiting.add(transfer(1, Map.of()));

        runOnce(Map.of("sequence", "transferDone"));

        assertTrue("a completed transfer must not be retried because an email failed",
                queue.requeued.isEmpty());
    }

    // ------------------------------------------------------------------ harness

    private void runOnce() throws Exception {
        runOnce(Map.of());
    }

    private void runOnce(Map<String, Object> extraParameters) throws Exception {
        MFTTransferService service = serviceWith(new HashMap<>(extraParameters));
        service.execute();
        waitForQuiet(service);
    }

    /** The processor the service was built with, so a test can ask whether it was deactivated. */
    private org.apache.synapse.message.processor.MessageProcessor processorOf(MFTTransferService service) {
        return lastProcessor;
    }

    private RecordingSequence deploySequence(String name) {
        RecordingSequence sequence = new RecordingSequence();
        config.addSequence(name, sequence);
        return sequence;
    }

    private FakeProcessor lastProcessor;

    private MFTTransferService serviceWith(Map<String, Object> parameters) {
        parameters.putIfAbsent("client.retry.interval", "1000");
        parameters.putIfAbsent("pipeline", "copy");
        lastProcessor = new FakeProcessor(parameters);
        return new MFTTransferService(lastProcessor, environment, "concurrency", "max.delivery.attempts",
                "client.retry.interval");
    }

    /** The transfers run on a pool, so an assertion straight after execute() would race them. */
    private void waitForQuiet(MFTTransferService service) throws Exception {
        long deadline = System.currentTimeMillis() + 5000L;
        while (System.currentTimeMillis() < deadline) {
            if (service.inFlight() == 0) {
                return;
            }
            Thread.sleep(20L);
        }
        fail("the transfer did not finish within five seconds");
    }

    private void deploy(String name, StreamSink sink, String ignored) throws Exception {
        StreamPipeline pipeline = new StreamPipeline();
        pipeline.setName(name);
        pipeline.addOperator(new BytesSource(), true);
        pipeline.addOperator(sink, true);
        pipeline.validate();
        config.addStreamPipeline(name, pipeline);
    }

    private static Transfer transfer(long jobId, Map<String, String> parameters, int attempt) {
        return new Transfer(jobId, QUEUE, parameters, TransferStatus.QUEUED.name(), attempt, 0L, null,
                SUBMITTER, null, null, 0L, 0L, 0L);
    }

    private static Transfer transfer(long jobId, Map<String, String> parameters) {
        return transfer(jobId, parameters, 0);
    }

    // ------------------------------------------------------------------ doubles

    /** Records what the service decided, which is the whole behaviour under test. */
    private static final class FakeStore implements TransferStore {
        private final List<Long> stoppedCalls = java.util.Collections.synchronizedList(new ArrayList<>());

        private final List<Transfer> waiting = new ArrayList<>();
        private final List<Requeue> requeued = java.util.Collections.synchronizedList(new ArrayList<>());
        private final List<Finish> finished = java.util.Collections.synchronizedList(new ArrayList<>());
        private final List<String> linked = java.util.Collections.synchronizedList(new ArrayList<>());

        @Override
        public org.apache.synapse.stream.Submission submit(String storeName,
                                                           Map<String, String> parameters,
                                                           String idempotencyKey,
                                                           String submitter) {
            long jobId = waiting.size() + 1L;
            waiting.add(transfer(jobId, parameters, 0));
            return new org.apache.synapse.stream.Submission(jobId, true);
        }

        @Override
        public synchronized Transfer claim(String storeName) {
            return waiting.isEmpty() ? null : waiting.remove(0);
        }

        @Override
        public org.apache.synapse.stream.TransferStatus requestPause(long jobId) {
            return null;
        }

        @Override
        public org.apache.synapse.stream.TransferStatus requestStop(long jobId) {
            return null;
        }

        @Override
        public boolean start(long jobId) {
            return false;
        }

        @Override
        public org.apache.synapse.stream.TransferStatus stopped(long jobId) {
            stoppedCalls.add(jobId);
            return null;
        }

        @Override
        public void linkRun(long jobId, String runId) {
            linked.add(jobId + "=" + runId);
        }

        @Override
        public synchronized boolean requeue(long jobId, long notBefore) {
            requeued.add(new Requeue(jobId, notBefore));
            return true;
        }

        @Override
        public void finish(long jobId, TransferStatus status, String errorCode, String errorMessage) {
            finished.add(new Finish(jobId, status, errorCode));
        }

        @Override
        public boolean cancel(long jobId) {
            return true;
        }

        @Override
        public Transfer find(long jobId) {
            return null;
        }

        @Override
        public List<Transfer> list(String storeName, String status, int limit) {
            return new ArrayList<>(waiting);
        }

        @Override
        public int queued(String storeName) {
            return waiting.size();
        }

        private record Requeue(long jobId, long notBefore) {
        }

        private record Finish(long jobId, TransferStatus status, String errorCode) {
        }
    }

    /** Enough of a processor to be asked its name, its store and its parameters. */
    private static final class FakeProcessor extends AbstractMessageProcessor {

        private FakeProcessor(Map<String, Object> parameters) {
            setName("mft-runner");
            setMessageStoreName(QUEUE);
            setParameters(parameters);
        }

        @Override
        public void destroy() {
        }

        @Override
        public boolean start() {
            return true;
        }

        @Override
        public boolean stop() {
            return true;
        }

        private volatile boolean deactivated;

        @Override
        public boolean deactivate() {
            deactivated = true;
            return true;
        }

        @Override
        public boolean activate() {
            return true;
        }

        @Override
        public boolean isDeactivated() {
            return false;
        }

        @Override
        public boolean isServerShuttingDown() {
            return false;
        }

        @Override
        public void pauseService() {
        }

        @Override
        public void resumeService() {
        }

        @Override
        public boolean isPaused() {
            return false;
        }

        @Override
        public void cleanupLocalResources() {
        }

        @Override
        public void cleanUpDeactivatedProcessors() {
        }

        @Override
        public void pauseMessageProcessorTemporarily() {
        }

        @Override
        public void resumeRemotely() {
        }
    }

    /** A sequence that records the properties it was given, which is the hook's whole contract. */
    private static final class RecordingSequence extends org.apache.synapse.mediators.AbstractMediator {

        private final List<Map<String, String>> runs =
                java.util.Collections.synchronizedList(new ArrayList<>());

        @Override
        public boolean mediate(org.apache.synapse.MessageContext msg) {
            Map<String, String> seen = new LinkedHashMap<>();
            for (Object key : msg.getPropertyKeySet()) {
                Object value = msg.getProperty(String.valueOf(key));
                if (value != null) {
                    seen.put(String.valueOf(key), String.valueOf(value));
                }
            }
            runs.add(seen);
            return true;
        }
    }

    private static final class ThrowingSequence extends org.apache.synapse.mediators.AbstractMediator {

        @Override
        public boolean mediate(org.apache.synapse.MessageContext msg) {
            throw new IllegalStateException("the mail server is down");
        }
    }

    private static final class BytesSource implements StreamSource {

        @Override
        public String name() {
            return "src";
        }

        @Override
        public InputStream open(StreamContext ctx) {
            return new ByteArrayInputStream("hello".getBytes());
        }
    }

    private static final class RecordingSink implements StreamSink {

        private final java.util.concurrent.atomic.AtomicInteger consumed =
                new java.util.concurrent.atomic.AtomicInteger();
        private final Map<String, Object> properties = new java.util.concurrent.ConcurrentHashMap<>();

        @Override
        public String name() {
            return "sink";
        }

        @Override
        public void consume(InputStream in, StreamContext ctx) throws IOException {
            org.apache.synapse.MessageContext msg = ctx.message();
            if (msg != null) {
                for (Object key : msg.getPropertyKeySet()) {
                    Object value = msg.getProperty(String.valueOf(key));
                    if (value != null) {
                        properties.put(String.valueOf(key), value);
                    }
                }
            }
            while (in.read() != -1) {
                // drain
            }
            consumed.incrementAndGet();
        }
    }

    private static final class FailingSink implements StreamSink {

        private final StreamException failure;

        private FailingSink(StreamException failure) {
            this.failure = failure;
        }

        @Override
        public String name() {
            return "sink";
        }

        @Override
        public void consume(InputStream in, StreamContext ctx) throws StreamException {
            throw failure;
        }
    }
}
