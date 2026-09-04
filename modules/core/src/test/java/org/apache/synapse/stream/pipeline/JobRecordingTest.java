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

package org.apache.synapse.stream.pipeline;

import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.TestMessageContext;
import org.apache.synapse.rest.RESTConstants;
import org.apache.synapse.stream.JobContext;
import org.apache.synapse.stream.JobRecord;
import org.apache.synapse.stream.JobRun;
import org.apache.synapse.stream.JobStore;
import org.apache.synapse.stream.JobStores;
import org.apache.synapse.stream.SourceIdentityPolicy;
import org.apache.synapse.stream.StreamException;
import org.apache.synapse.stream.StreamOrigin;
import org.apache.synapse.stream.StreamSeed;
import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests that every run is recorded, that recording never costs a transfer its bytes, and that a retry
 * finding a different source does not resume into the previous attempt's artifacts.
 */
public class JobRecordingTest {

    private static final byte[] DATA = "hello job record".getBytes(StandardCharsets.UTF_8);

    @After
    public void withdrawProvider() {
        JobStores.register(null);
    }

    private static StreamPipeline pipeline(String name) {
        StreamPipeline p = new StreamPipeline();
        p.setName(name);
        return p;
    }

    private static StreamSeed seed(String sourceId, long size, long lastModified) {
        return new StreamSeed(new ByteArrayInputStream(DATA), sourceId, size, lastModified,
                StreamOrigin.REOPENABLE);
    }

    /** A store that remembers what it was told, and can be made to fail on demand. */
    private static final class Recording implements JobStore {

        final List<JobRun> started = new ArrayList<>();
        final List<String> events = new ArrayList<>();
        JobRecord prior;
        RuntimeException failOnStart;
        RuntimeException failOnEverythingElse;

        @Override
        public JobRecord start(JobRun run) throws StreamException {
            if (failOnStart != null) {
                throw new StreamException("store refused the run", failOnStart, true);
            }
            started.add(run);
            return prior;
        }

        @Override
        public void progress(String runId, String stage, long bytes, long records) {
            raise();
            events.add("progress:" + stage + ":" + bytes);
        }

        @Override
        public void stageFinished(String runId, String stage, int index, long in, long out, long ns) {
            raise();
            events.add("stage:" + index + ":" + stage);
        }

        @Override
        public void stageSkipped(String runId, String stage, int index) {
            raise();
            events.add("skipped:" + index + ":" + stage);
        }

        @Override
        public void succeeded(String runId, long bytes, long records) {
            raise();
            events.add("succeeded:" + bytes);
        }

        @Override
        public void failed(String runId, String stage, String code, String message, boolean retry) {
            raise();
            events.add("failed:" + stage + ":" + code);
        }

        private void raise() {
            if (failOnEverythingElse != null) {
                throw failOnEverythingElse;
            }
        }

        void install() {
            JobStores.register(pipelineName -> this);
        }
    }

    /**
     * A resumed run records the stages it skipped, rather than simply omitting them.
     *
     * <p>When a stage short-circuits to a complete artifact, everything above it was constructed and
     * never read — so reporting it as finished would be a lie and reporting nothing leaves the
     * per-stage table sparse with no explanation of why. The status column already existed for this.
     */
    @Test
    public void recordsTheStagesAResumedRunSkipped() throws Exception {
        Recording store = new Recording();
        store.install();

        StreamPipeline p = pipeline("resume-reporting");
        p.setWorkspaceRoot(System.getProperty("java.io.tmpdir"));
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.ResumesFromArtifact("mat", DATA), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        p.validate();

        p.execute(null, jobId("job-resumed"));

        assertTrue("the released upstream must be recorded as skipped, not left out: " + store.events,
                store.events.contains("skipped:0:src"));
        assertFalse("and not reported as if it had done work",
                store.events.contains("stage:0:src"));
    }

    // ------------------------------------------------------------------ every run is recorded

    @Test
    public void recordsTheRunEvenWhenTheCallerWantsNoReporting() throws Exception {
        Recording store = new Recording();
        store.install();

        StreamPipeline p = pipeline("recorded");
        p.setSourceProvided(true);
        p.addOperator(new Mocks.Sink("sink"), true);
        p.validate();

        p.execute(null, JobContext.NOOP, seed("file:/in/a.csv", 16L, 1234L));

        assertEquals(1, store.started.size());
        JobRun run = store.started.get(0);
        assertEquals("recorded", run.pipelineName());
        assertEquals("DIRECT", run.invokerType());
        assertNull("nothing named an invoker", run.invokerName());
        assertEquals("file:/in/a.csv", run.sourceId());
        assertEquals(16L, run.sourceSize());
        assertEquals(1234L, run.sourceLastModified());
        assertNotNull(run.runId());
        assertTrue(store.events.contains("succeeded:0"));
    }

    // ------------------------------------------------------------------ the invoker

    /** Runs a one-stage pipeline over a message context carrying the given Synapse properties. */
    private JobRun runWith(String pipelineName, JobContext job, String... properties) throws Exception {
        Recording store = new Recording();
        store.install();

        StreamPipeline p = pipeline(pipelineName);
        p.setSourceProvided(true);
        p.addOperator(new Mocks.Sink("sink"), true);
        p.validate();

        MessageContext msg = null;
        if (properties.length > 0) {
            msg = new TestMessageContext();
            for (int i = 0; i < properties.length; i += 2) {
                msg.setProperty(properties[i], properties[i + 1]);
            }
        }
        p.execute(msg, job, seed("file:/in/a.csv", 16L, 1234L));
        return store.started.get(0);
    }

    /**
     * The case the whole change exists for. An inbound endpoint stamps its name on the context before
     * injecting — verified in the prior art at AbstractInjectHandler:153 — so the framework can see it
     * without anyone declaring anything.
     */
    @Test
    public void derivesAnInboundEndpoint() throws Exception {
        JobRun run = runWith("inbound", JobContext.NOOP,
                SynapseConstants.INBOUND_ENDPOINT_NAME, "fileInbound1");

        assertEquals("INBOUND", run.invokerType());
        assertEquals("fileInbound1", run.invokerName());
    }

    /**
     * API.getName() returns "name:vX" for a versioned API, so that is what lands in the property and
     * what gets stored. Anything filtering on it later has to match the same string.
     */
    @Test
    public void derivesAnApiIncludingItsVersion() throws Exception {
        JobRun run = runWith("api", JobContext.NOOP,
                RESTConstants.SYNAPSE_REST_API, "TransferAPI:v1.0.0");

        assertEquals("API", run.invokerType());
        assertEquals("TransferAPI:v1.0.0", run.invokerName());
    }

    @Test
    public void derivesAProxyService() throws Exception {
        JobRun run = runWith("proxy", JobContext.NOOP,
                SynapseConstants.PROXY_SERVICE, "TransferProxy");

        assertEquals("PROXY", run.invokerType());
        assertEquals("TransferProxy", run.invokerName());
    }

    /** Cascade order is Synapse's own, from TimeoutHandler: proxy, then API, then inbound. */
    @Test
    public void prefersTheProxyWhenSeveralPropertiesAreSet() throws Exception {
        JobRun run = runWith("cascade", JobContext.NOOP,
                SynapseConstants.INBOUND_ENDPOINT_NAME, "fileInbound1",
                RESTConstants.SYNAPSE_REST_API, "TransferAPI",
                SynapseConstants.PROXY_SERVICE, "TransferProxy");

        assertEquals("PROXY", run.invokerType());
        assertEquals("TransferProxy", run.invokerName());
    }

    @Test
    public void fallsBackToDirectWhenNothingNamesAnInvoker() throws Exception {
        JobRun run = runWith("bare", JobContext.NOOP);

        assertEquals("DIRECT", run.invokerType());
        assertNull(run.invokerName());
    }

    @Test
    public void ignoresABlankProperty() throws Exception {
        JobRun run = runWith("blank", JobContext.NOOP,
                SynapseConstants.PROXY_SERVICE, "   ",
                SynapseConstants.INBOUND_ENDPOINT_NAME, "fileInbound1");

        assertEquals("a whitespace property must not win the cascade", "INBOUND", run.invokerType());
        assertEquals("fileInbound1", run.invokerName());
    }

    /**
     * The seam a message processor needs: its context is synthesised and names none of the artifacts
     * above, so it is the one invoker that has to declare itself.
     */
    @Test
    public void anExplicitJobContextBeatsDerivation() throws Exception {
        JobContext processor = new JobContext() {
            @Override
            public String invokerType() {
                return "PROCESSOR";
            }

            @Override
            public String invokerName() {
                return "MFTProcessor";
            }
        };

        JobRun run = runWith("declared", processor,
                SynapseConstants.INBOUND_ENDPOINT_NAME, "fileInbound1");

        assertEquals("PROCESSOR", run.invokerType());
        assertEquals("MFTProcessor", run.invokerName());
    }

    /**
     * The sink is the one stage with no StageStream around it, so it was silently missing from the
     * per-stage rows: a source -> sink pipeline produced one row instead of two.
     */
    @Test
    public void recordsTheSinkAsAStageToo() throws Exception {
        Recording store = new Recording();
        store.install();

        StreamPipeline p = pipeline("with-sink");
        p.addOperator(new Mocks.Source("read", DATA), true);
        p.addOperator(new Mocks.Sink("write"), true);
        p.validate();

        p.execute(null, JobContext.NOOP);

        List<String> stages = store.events.stream().filter(e -> e.startsWith("stage:")).toList();
        assertEquals("a source and a sink are two stages, not one", 2, stages.size());
        assertEquals("stage:0:read", stages.get(0));
        assertEquals("stage:1:write", stages.get(1));
    }

    @Test
    public void reportsTheSinkEvenWhenTheRunFails() {
        Recording store = new Recording();
        store.install();

        StreamPipeline p = pipeline("failing-with-sink");
        p.addOperator(new Mocks.Source("read", DATA), true);
        p.addOperator(new Mocks.FailingTransform("bad"), true);
        p.addOperator(new Mocks.Sink("write"), true);
        try {
            p.validate();
            p.execute(null, JobContext.NOOP);
            fail("expected the run to fail");
        } catch (StreamException expected) {
            // a sink that died twenty minutes in is exactly what someone wants to see afterwards
        }
        assertTrue(store.events.toString(),
                store.events.stream().anyMatch(e -> e.endsWith(":write")));
    }

    @Test
    public void recordsStagesInChainOrder() throws Exception {
        Recording store = new Recording();
        store.install();

        StreamPipeline p = pipeline("staged");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.PassThrough("mid"), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        p.validate();

        p.execute(null, JobContext.NOOP);

        List<String> stages = store.events.stream().filter(e -> e.startsWith("stage:")).toList();
        assertEquals(3, stages.size());
        assertEquals("stage:0:src", stages.get(0));
        assertEquals("stage:1:mid", stages.get(1));
        assertEquals("the sink is reported last, after the stages it read through",
                "stage:2:sink", stages.get(2));
    }

    @Test
    public void recordsAFailureWithItsAttributedStage() {
        Recording store = new Recording();
        store.install();

        StreamPipeline p = pipeline("broken");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.FailingTransform("bad"), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        try {
            p.validate();
            p.execute(null, JobContext.NOOP);
            fail("expected the run to fail");
        } catch (StreamException expected) {
            // the point is what was recorded
        }
        assertTrue(store.events.toString(),
                store.events.contains("failed:bad:STREAM_PIPELINE_FAILED"));
        assertFalse(store.events.stream().anyMatch(e -> e.startsWith("succeeded")));
    }

    @Test
    public void runsWithNoProviderAtAll() throws Exception {
        JobStores.register(null);
        assertFalse(JobStores.isAvailable());

        StreamPipeline p = pipeline("unrecorded");
        Mocks.Sink sink = new Mocks.Sink("sink");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(sink, true);
        p.validate();

        p.execute(null, JobContext.NOOP);
        assertEquals(DATA.length, sink.received.size());
    }

    // ------------------------------------------------------------------ recording never costs bytes

    @Test
    public void aStoreThatFailsMidRunDoesNotFailTheTransfer() throws Exception {
        Recording store = new Recording();
        store.failOnEverythingElse = new IllegalStateException("database went away");
        store.install();

        StreamPipeline p = pipeline("resilient");
        Mocks.Sink sink = new Mocks.Sink("sink");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(sink, true);
        p.validate();

        p.execute(null, JobContext.NOOP);

        // The bytes are what matter; the record is the thing allowed to be lost.
        assertEquals(DATA.length, sink.received.size());
    }

    @Test
    public void aStoreThatCannotStartTheRunFailsIt() {
        Recording store = new Recording();
        store.failOnStart = new IllegalStateException("no connection");
        store.install();

        StreamPipeline p = pipeline("unstartable");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        try {
            p.validate();
            p.execute(null, JobContext.NOOP);
            fail("a run that cannot be recorded cannot be recovered, so it must not start");
        } catch (StreamException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("store refused the run"));
        }
    }

    @Test
    public void theCallersOwnContextStillSeesEverything() throws Exception {
        new Recording().install();

        AtomicInteger stages = new AtomicInteger();
        List<String> failures = new ArrayList<>();
        JobContext caller = new JobContext() {
            @Override
            public void stageFinished(String stage, long in, long out, long ns) {
                stages.incrementAndGet();
            }

            @Override
            public void failed(String stage, String code, String message) {
                failures.add(stage);
            }
        };

        StreamPipeline p = pipeline("delegating");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.PassThrough("mid"), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        p.validate();
        p.execute(null, caller);

        assertEquals("source, transform and sink", 3, stages.get());
        assertTrue(failures.isEmpty());
    }

    // ------------------------------------------------------------------ scratch reclamation

    /**
     * A scratch run's spill is garbage the moment the run ends: nothing can resume from it, because
     * the run has no stable identity to resume into. Leaving it accumulated a gigabyte per copy.
     */
    @Test
    public void reclaimsTheWorkspaceOfAScratchRun() throws Exception {
        new Recording().install();

        Path root = Files.createTempDirectory("mft-scratch");
        StreamPipeline p = pipeline("scratch");
        p.setWorkspaceRoot(root.toString());
        p.addOperator(new Mocks.Source("read", DATA), true);
        p.addOperator(new Mocks.LazyMaterialising("spill"), true);
        p.addOperator(new Mocks.Sink("write"), true);
        p.validate();

        // No job id and no seed, so the run id is a fresh UUID and the run cannot be resumed.
        p.execute(null, JobContext.NOOP);

        Path pipelineDir = root.resolve("scratch");
        assertTrue("the pipeline directory itself stays", Files.isDirectory(pipelineDir));
        try (var runs = Files.list(pipelineDir)) {
            assertEquals("the scratch run left nothing behind", 0L, runs.count());
        }
    }

    @Test
    public void reclaimsAScratchWorkspaceAfterAFailureToo() throws Exception {
        new Recording().install();

        Path root = Files.createTempDirectory("mft-scratch-failed");
        StreamPipeline p = pipeline("scratch-failed");
        p.setWorkspaceRoot(root.toString());
        p.addOperator(new Mocks.Source("read", DATA), true);
        p.addOperator(new Mocks.LazyMaterialising("spill"), true);
        p.addOperator(new Mocks.FailingTransform("bad"), true);
        p.addOperator(new Mocks.Sink("write"), true);
        try {
            p.validate();
            p.execute(null, JobContext.NOOP);
            fail("expected the run to fail");
        } catch (StreamException expected) {
            // nothing resumes a scratch run, so its remains are garbage either way
        }

        try (var runs = Files.list(root.resolve("scratch-failed"))) {
            assertEquals(0L, runs.count());
        }
    }

    /**
     * A resumable run keeps its artifacts. Whether a *completed* resumable run should keep them is
     * retention policy, and deliberately not decided from inside a run.
     */
    @Test
    public void keepsTheWorkspaceOfAResumableRun() throws Exception {
        new Recording().install();

        Path root = Files.createTempDirectory("mft-resumable");
        StreamPipeline p = pipeline("resumable");
        p.setWorkspaceRoot(root.toString());
        p.setSourceProvided(true);
        p.addOperator(new Mocks.LazyMaterialising("spill"), true);
        p.addOperator(new Mocks.Sink("write"), true);
        p.validate();

        // A caller-supplied job id makes the run id stable, and the seed is re-openable.
        p.execute(null, jobId("job-keep"), seed("file:/in/a.csv", 16L, 1234L));

        assertTrue("a resumable run's artifacts are resume candidates",
                Files.isDirectory(root.resolve("resumable").resolve("job-keep")));
    }

    /**
     * A pipeline with no sink ends in a materialising transform doing its work through side effects —
     * forEach calling a backend per record. Its artifact is spill like any other stage's, because the
     * workspace never holds output (ADR-0029). An earlier version kept it, on the reading that the
     * artifact was the deliverable; ADR-0015 in fact drains that stage and discards the bytes.
     */
    @Test
    public void reclaimsAScratchRunThatHasNoSink() throws Exception {
        new Recording().install();

        Path root = Files.createTempDirectory("mft-terminal");
        StreamPipeline p = pipeline("terminal");
        p.setWorkspaceRoot(root.toString());
        p.addOperator(new Mocks.Source("read", DATA), true);
        p.addOperator(new Mocks.LazyMaterialising("spill"), true);
        p.validate();

        p.execute(null, JobContext.NOOP);

        try (var runs = Files.list(root.resolve("terminal"))) {
            assertEquals("a sinkless pipeline's spill is still spill", 0L, runs.count());
        }
    }

    // ------------------------------------------------------------------ the source-identity guard

    /** A pipeline with a materialising stage, so the run actually has a workspace to discard. */
    private static StreamPipeline materialising(String name, Path root) {
        StreamPipeline p = pipeline(name);
        p.setWorkspaceRoot(root.toString());
        p.setSourceProvided(true);
        p.addOperator(new Mocks.LazyMaterialising("mat"), true);
        return p;
    }

    @Test
    public void anUnchangedSourceKeepsItsWorkspace() throws Exception {
        Recording store = new Recording();
        store.prior = new JobRecord("job-1", "FAILED", 1, "file:/in/a.csv", 16L, 1234L);
        store.install();

        Path root = Files.createTempDirectory("mft-guard-same");
        StreamPipeline p = materialising("same", root);
        p.validate();

        Path stage = root.resolve("same").resolve("job-1").resolve("mat");
        Files.createDirectories(stage);
        Path marker = stage.resolve("artifact.out");
        Files.writeString(marker, "earlier attempt");

        p.execute(null, jobId("job-1"), seed("file:/in/a.csv", 16L, 1234L));

        assertTrue("an unchanged source must not lose its resume candidate", Files.exists(marker));
    }

    @Test
    public void warnDiscardsTheWorkspaceOfAChangedSource() throws Exception {
        Recording store = new Recording();
        store.prior = new JobRecord("job-2", "FAILED", 1, "file:/in/a.csv", 16L, 1234L);
        store.install();

        Path root = Files.createTempDirectory("mft-guard-warn");
        StreamPipeline p = materialising("warned", root);
        assertEquals(SourceIdentityPolicy.WARN, p.getSourceIdentity());
        p.validate();

        Path stage = root.resolve("warned").resolve("job-2").resolve("mat");
        Files.createDirectories(stage);
        Path stale = stage.resolve("artifact.out");
        Files.writeString(stale, "bytes of a file that is no longer there");

        // Same job id, different file underneath it — the retry case the guard exists for.
        p.execute(null, jobId("job-2"), seed("file:/in/a.csv", 99L, 5678L));

        assertFalse("resuming into another source's artifact is the corruption this prevents",
                Files.exists(stale));
        assertTrue("the run still needs a workspace afterwards", Files.isDirectory(stage));
    }

    @Test
    public void strictRefusesAChangedSource() throws Exception {
        Recording store = new Recording();
        store.prior = new JobRecord("job-3", "FAILED", 1, "file:/in/a.csv", 16L, 1234L);
        store.install();

        Path root = Files.createTempDirectory("mft-guard-strict");
        StreamPipeline p = materialising("strict", root);
        p.setSourceIdentity(SourceIdentityPolicy.STRICT);
        p.validate();

        try {
            p.execute(null, jobId("job-3"), seed("file:/in/a.csv", 99L, 5678L));
            fail("expected strict to refuse a changed source");
        } catch (StreamException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("previously read"));
            assertTrue(e.getMessage(), e.getMessage().contains("refuses to continue"));
        }
    }

    @Test
    public void offLeavesTheWorkspaceAlone() throws Exception {
        Recording store = new Recording();
        store.prior = new JobRecord("job-4", "FAILED", 1, "file:/in/a.csv", 16L, 1234L);
        store.install();

        Path root = Files.createTempDirectory("mft-guard-off");
        StreamPipeline p = materialising("off", root);
        p.setSourceIdentity(SourceIdentityPolicy.OFF);
        p.validate();

        Path stage = root.resolve("off").resolve("job-4").resolve("mat");
        Files.createDirectories(stage);
        Path kept = stage.resolve("artifact.out");
        Files.writeString(kept, "kept deliberately");

        p.execute(null, jobId("job-4"), seed("file:/in/a.csv", 99L, 5678L));

        assertTrue(Files.exists(kept));
    }

    @Test
    public void aFirstAttemptHasNothingToGuardAgainst() throws Exception {
        Recording store = new Recording();
        store.prior = null;
        store.install();

        Path root = Files.createTempDirectory("mft-guard-first");
        StreamPipeline p = materialising("first", root);
        p.setSourceIdentity(SourceIdentityPolicy.STRICT);
        p.validate();

        p.execute(null, jobId("job-5"), seed("file:/in/a.csv", 16L, 1234L));
        assertEquals(1, store.started.size());
    }

    @Test
    public void aPriorRecordWithNoIdentityIsNotTreatedAsAChange() throws Exception {
        Recording store = new Recording();
        store.prior = new JobRecord("job-6", "FAILED", 1, null, StreamSeed.UNKNOWN,
                StreamSeed.UNKNOWN);
        store.install();

        Path root = Files.createTempDirectory("mft-guard-blank");
        StreamPipeline p = materialising("blank", root);
        p.setSourceIdentity(SourceIdentityPolicy.STRICT);
        p.validate();

        p.execute(null, jobId("job-6"), seed("file:/in/a.csv", 16L, 1234L));
        assertNull(store.prior.sourceId());
    }

    private static JobContext jobId(String id) {
        return new JobContext() {
            @Override
            public String jobId() {
                return id;
            }
        };
    }
}
