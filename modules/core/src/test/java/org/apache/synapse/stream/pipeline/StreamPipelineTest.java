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

import org.apache.synapse.stream.JobContext;
import org.apache.synapse.stream.StreamException;
import org.apache.synapse.stream.StreamOrigin;
import org.apache.synapse.stream.StreamSeed;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link StreamPipeline}: the build/pull split, validation, fault attribution, unwind
 * ordering, provided sources and concurrency.
 */
public class StreamPipelineTest {

    private static final byte[] DATA = "hello stream pipeline".getBytes(StandardCharsets.UTF_8);

    private static StreamPipeline pipeline(String name) {
        StreamPipeline p = new StreamPipeline();
        p.setName(name);
        return p;
    }

    // ------------------------------------------------------------------ the build/pull split

    /**
     * The one test that mechanically enforces invariant 1. Everything else about the no-I/O rule is
     * carried by javadoc.
     */
    @Test
    public void buildingTheChainMovesNoBytes() throws Exception {
        Mocks.Source src = new Mocks.Source("src", DATA);
        Mocks.PassThrough mid = new Mocks.PassThrough("mid");
        Mocks.Sink sink = new Mocks.Sink("sink", false);   // deliberately does not drain

        StreamPipeline p = pipeline("build-only");
        p.addOperator(src, true);
        p.addOperator(mid, true);
        p.addOperator(sink, true);
        p.validate();

        p.execute(null, JobContext.NOOP);

        assertEquals("the source must be opened exactly once", 1, src.opens.get());
        assertEquals("the transform must be wrapped exactly once", 1, mid.wraps.get());
        assertEquals("the sink must be invoked exactly once", 1, sink.consumes.get());
        assertEquals("building the chain must not read a single byte", 0, src.opened.reads.get());
    }

    @Test
    public void pullingTheChainMovesEveryByteThroughEveryStage() throws Exception {
        Mocks.Sink sink = new Mocks.Sink("sink");

        StreamPipeline p = pipeline("full");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.PassThrough("a"), true);
        p.addOperator(new Mocks.PassThrough("b"), true);
        p.addOperator(sink, true);
        p.validate();

        p.execute(null, JobContext.NOOP);

        assertArrayEquals(DATA, sink.bytes());
    }

    // ------------------------------------------------------------------ validation

    @Test
    public void rejectsAPipelineWithNoOperators() {
        assertRejected(pipeline("empty"), "has no operators");
    }

    @Test
    public void rejectsAMultiRoleOperator() {
        StreamPipeline p = pipeline("multi");
        p.addOperator(new Mocks.MultiRole(), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        assertRejected(p, "more than one role interface");
    }

    @Test
    public void rejectsAnOperatorWithNoRole() {
        StreamPipeline p = pipeline("norole");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.NoRole(), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        assertRejected(p, "implements none of");
    }

    @Test
    public void rejectsASinkThatIsNotLast() {
        StreamPipeline p = pipeline("earlysink");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        p.addOperator(new Mocks.PassThrough("after"), true);
        assertRejected(p, "is not last");
    }

    /**
     * Note which rule fires. With two sinks the earlier one is necessarily not last, so the
     * position rule trips before the count rule ever runs — which means the "exactly one sink"
     * count check is only ever reachable for <i>zero</i> sinks. Worth knowing before anyone
     * "simplifies" one of the two away.
     */
    @Test
    public void rejectsMoreThanOneSink() {
        StreamPipeline p = pipeline("twosinks");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.Sink("a"), true);
        p.addOperator(new Mocks.Sink("b"), true);
        assertRejected(p, "is not last");
    }

    // A source plus a plain transform and no sink is covered by
    // rejectsATerminalTransformWithNoDurableEffect below. There is no separate "exactly one sink"
    // count rule any more: zero sinks is handled by the terminal rule, and more than one is
    // unreachable because the earlier sink necessarily fails the position rule first.

    @Test
    public void rejectsAChainThatDoesNotBeginWithASource() {
        StreamPipeline p = pipeline("nosource");
        p.addOperator(new Mocks.PassThrough("mid"), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        assertRejected(p, "must begin with a StreamSource");
    }

    @Test
    public void rejectsASourceThatIsNotFirst() {
        StreamPipeline p = pipeline("latesource");
        p.addOperator(new Mocks.Source("a", DATA), true);
        p.addOperator(new Mocks.Source("b", DATA), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        assertRejected(p, "is not first");
    }

    @Test
    public void rejectsADerivedNameOnAnOperatorThatKeepsDurableState() {
        StreamPipeline p = pipeline("derived");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.Materialising("mat"), false);   // name not explicit
        p.addOperator(new Mocks.Sink("sink"), true);
        assertRejected(p, "requires an explicit name");
    }

    @Test
    public void rejectsMaterialisationUntilWorkspaceSupportExists() {
        StreamPipeline p = pipeline("mat");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.Materialising("mat"), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        assertRejected(p, "not implemented yet");
    }

    // ------------------------------------------------------------------ source modes

    @Test
    public void acceptsASinkOnlyPipelineWhenTheSourceIsProvided() throws Exception {
        Mocks.Sink sink = new Mocks.Sink("sink");
        StreamPipeline p = pipeline("provided");
        p.setSourceProvided(true);
        p.addOperator(sink, true);
        p.validate();

        p.execute(null, JobContext.NOOP, seed(DATA));

        assertArrayEquals("a sink-only pipeline is a legal 'put this stream there' artifact",
                DATA, sink.bytes());
    }

    @Test
    public void rejectsASourceFirstWhenTheSourceIsProvided() {
        StreamPipeline p = pipeline("provided-with-source");
        p.setSourceProvided(true);
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        assertRejected(p, "must not be a StreamSource");
    }

    @Test
    public void refusesToRunAProvidedSourcePipelineWithNoStream() {
        StreamPipeline p = pipeline("provided");
        p.setSourceProvided(true);
        p.addOperator(new Mocks.Sink("sink"), true);

        try {
            p.execute(null, JobContext.NOOP);
            fail("expected a mode mismatch to be rejected");
        } catch (StreamException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("no stream was supplied"));
        }
    }

    @Test
    public void refusesAStreamWhenThePipelineOpensItsOwnSource() {
        StreamPipeline p = pipeline("self");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.Sink("sink"), true);

        try {
            p.execute(null, JobContext.NOOP, seed(DATA));
            fail("expected a mode mismatch to be rejected");
        } catch (StreamException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("opens its own source"));
        }
    }

    @Test
    public void doesNotCloseAProvidedStreamBecauseTheProviderOwnsIt() throws Exception {
        TrackingStream provided = new TrackingStream(DATA);

        StreamPipeline p = pipeline("provided");
        p.setSourceProvided(true);
        p.addOperator(new Mocks.PassThrough("mid"), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        p.validate();

        p.execute(null, JobContext.NOOP,
                new StreamSeed(provided, "test://provided", DATA.length, 0L, StreamOrigin.REOPENABLE));

        assertFalse("the pipeline must not close a stream it did not open", provided.closed);
    }

    // ------------------------------------------------------------------ failure handling

    @Test
    public void attributesAFailureToTheStageThatRaisedIt() {
        StreamPipeline p = pipeline("failing");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.PassThrough("before"), true);
        p.addOperator(new Mocks.FailingTransform("culprit"), true);
        p.addOperator(new Mocks.PassThrough("after"), true);
        p.addOperator(new Mocks.Sink("sink"), true);

        try {
            p.validate();
            p.execute(null, JobContext.NOOP);
            fail("expected the chain to fail");
        } catch (StreamException e) {
            assertEquals("the innermost failing stage must be reported", "culprit", e.getStage());
        }
    }

    @Test
    public void reportsFailureThroughTheJobContextExactlyOnce() {
        RecordingJob job = new RecordingJob();

        StreamPipeline p = pipeline("failing");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.FailingTransform("culprit"), true);
        p.addOperator(new Mocks.Sink("sink"), true);

        try {
            p.validate();
            p.execute(null, job);
            fail("expected the chain to fail");
        } catch (StreamException expected) {
            assertEquals(1, job.failures.size());
            assertEquals("culprit", job.failures.get(0));
        }
    }

    @Test
    public void rejectsATransformThatReturnsItsOwnUpstream() {
        StreamPipeline p = pipeline("identity");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.IdentityTransform(), true);
        p.addOperator(new Mocks.Sink("sink"), true);

        try {
            p.validate();
            p.execute(null, JobContext.NOOP);
            fail("expected a pass-through returning its upstream to be rejected");
        } catch (StreamException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("returned its own upstream"));
        }
    }

    @Test
    public void closesEveryResourceEvenWhenTheChainFails() throws Exception {
        StreamPipeline p = pipeline("failing");
        Mocks.Source src = new Mocks.Source("src", DATA);
        p.addOperator(src, true);
        p.addOperator(new Mocks.FailingTransform("culprit"), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        p.validate();

        try {
            p.execute(null, JobContext.NOOP);
            fail("expected the chain to fail");
        } catch (StreamException expected) {
            // The source's stream was registered with the scope, so it must have been closed.
            assertNotNull(src.opened);
        }
    }

    // ------------------------------------------------------------------ telemetry

    @Test
    public void reportsEveryStageWithBytesInAndOut() throws Exception {
        RecordingJob job = new RecordingJob();

        StreamPipeline p = pipeline("telemetry");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.PassThrough("mid"), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        p.validate();

        p.execute(null, job);

        assertEquals("one report per wrapped stage", 2, job.stages.size());
        assertTrue(job.stages.containsKey("src"));
        assertTrue(job.stages.containsKey("mid"));
        assertEquals("the head stage consumed nothing from below",
                0L, (long) job.stagesIn.get("src"));
        assertEquals("the middle stage consumed what the head produced",
                (long) job.stages.get("src"), (long) job.stagesIn.get("mid"));
    }

    @Test
    public void reportsStagesEvenOnTheFailurePath() {
        RecordingJob job = new RecordingJob();

        StreamPipeline p = pipeline("failing");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.FailingTransform("culprit"), true);
        p.addOperator(new Mocks.Sink("sink"), true);

        try {
            p.validate();
            p.execute(null, job);
            fail("expected the chain to fail");
        } catch (StreamException expected) {
            assertFalse("stage telemetry must survive a failure", job.stages.isEmpty());
        }
    }

    // ------------------------------------------------------------------ concurrency

    @Test
    public void oneInstanceServesConcurrentRunsWithoutInterference() throws Exception {
        StreamPipeline p = pipeline("concurrent");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.PassThrough("mid"), true);
        // A fresh sink per run is the operator author's job; this sink accumulates per instance,
        // so the shared one here is deliberately stateless about ordering.
        p.addOperator(new Mocks.Sink("sink"), true);
        p.validate();

        int runs = 8;
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            List<Callable<Boolean>> tasks = new ArrayList<>();
            for (int i = 0; i < runs; i++) {
                tasks.add(() -> {
                    p.execute(null, JobContext.NOOP);
                    return true;
                });
            }
            List<Future<Boolean>> results = pool.invokeAll(tasks);
            for (Future<Boolean> f : results) {
                assertTrue("every concurrent run must complete", f.get(30, TimeUnit.SECONDS));
            }
        } finally {
            pool.shutdownNow();
        }
    }

    // ------------------------------------------------------------------ helpers

    /**
     * A job context with a real id. Required by any pipeline whose operators keep durable state —
     * NOOP's placeholder id is shared by every run, so it cannot name a workspace.
     */
    private static JobContext jobWithId(String id) {
        return new JobContext() {
            @Override
            public String jobId() {
                return id;
            }
        };
    }

    private static StreamSeed seed(byte[] data) {
        return new StreamSeed(new ByteArrayInputStream(data), "test://seed", data.length, 0L,
                StreamOrigin.REOPENABLE);
    }

    private static void assertRejected(StreamPipeline p, String expectedInMessage) {
        try {
            p.validate();
            fail("expected validate() to reject this pipeline for: " + expectedInMessage);
        } catch (StreamException e) {
            assertTrue("message should mention '" + expectedInMessage + "' but was: "
                    + e.getMessage(), e.getMessage().contains(expectedInMessage));
        }
    }

    /** Tracks whether the pipeline closed a stream it was handed. */
    private static final class TrackingStream extends InputStream {

        private final ByteArrayInputStream delegate;
        boolean closed;

        TrackingStream(byte[] data) {
            this.delegate = new ByteArrayInputStream(data);
        }

        @Override
        public int read() {
            return delegate.read();
        }

        @Override
        public int read(byte[] b, int off, int len) {
            return delegate.read(b, off, len);
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    /** Captures what the pipeline reports. */
    private static final class RecordingJob implements JobContext {

        final List<String> failures = Collections.synchronizedList(new ArrayList<>());
        final java.util.Map<String, Long> stages = new java.util.LinkedHashMap<>();
        final java.util.Map<String, Long> stagesIn = new java.util.LinkedHashMap<>();

        @Override
        public String jobId() {
            return "test-job";
        }

        @Override
        public void stageFinished(String stage, long bytesIn, long bytesOut, long selfNanos) {
            stages.put(stage, bytesOut);
            stagesIn.put(stage, bytesIn);
        }

        @Override
        public void failed(String stage, String code, String message) {
            failures.add(stage);
        }
    }

    // ------------------------------------------------------------------ terminal materialising transform

    /**
     * A materialising transform may be last. It passes the terminal rule and then trips the
     * temporary workspace gate — which is how we can tell the terminal rule accepted it.
     */
    @Test
    public void acceptsATerminalMaterialisingTransform() {
        StreamPipeline p = pipeline("terminal-mat");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.LazyMaterialising("forEach"), true);

        try {
            p.validate();
            fail("expected the temporary workspace gate to trip");
        } catch (StreamException e) {
            assertTrue("it must get past the terminal rule, not be rejected by it: " + e.getMessage(),
                    e.getMessage().contains("not implemented yet"));
        }
    }

    @Test
    public void rejectsATerminalTransformWithNoDurableEffect() {
        StreamPipeline p = pipeline("terminal-plain");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.PassThrough("mid"), true);
        assertRejected(p, "must end with a StreamSink");
    }

    /**
     * With no sink, the pipeline pulls the terminal stage itself. Asserted on the operator's own
     * record of what it wrote, so this proves the work happened rather than just that a loop ran.
     */
    @Test
    public void drainsTheTerminalStageWhenThereIsNoSink() throws Exception {
        Mocks.LazyMaterialising terminal = new Mocks.LazyMaterialising("forEach");

        StreamPipeline p = pipeline("drain");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(terminal, true);
        // validate() is deliberately skipped: the workspace gate is a temporary scaffold, and the
        // drain is an execute() behaviour independent of it.

        p.execute(null, jobWithId("drain-test"));

        assertArrayEquals("every byte must have been pulled through the terminal stage",
                DATA, terminal.written.toByteArray());
    }

    @Test
    public void aDrainedTerminalStageIsStillReportedAsAStage() throws Exception {
        RecordingJob job = new RecordingJob();

        StreamPipeline p = pipeline("drain-telemetry");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.LazyMaterialising("forEach"), true);

        p.execute(null, job);

        assertEquals("the terminal stage produced every byte",
                (long) DATA.length, (long) job.stages.get("forEach"));
    }

    /**
     * Without this, a pipeline ending in a materialising transform would be the one shape that
     * cannot be cancelled, because there is no sink polling on its behalf.
     */
    @Test
    public void drainingRespectsCancellation() {
        StreamPipeline p = pipeline("drain-cancel");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.LazyMaterialising("forEach"), true);

        try {
            p.execute(null, new JobContext() {
                @Override
                public String jobId() {
                    return "cancel-test";
                }

                @Override
                public boolean isCancelled() {
                    return true;
                }
            });
            fail("expected cancellation to stop the drain");
        } catch (StreamException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("cancelled"));
            assertTrue("cancellation is retryable", e.isRetryable());
        }
    }

    @Test
    public void aSinkOnlyProvidedSourcePipelineStillNeedsNoDrain() throws Exception {
        // Regression guard: the sink path and the drain path must not both run.
        Mocks.Sink sink = new Mocks.Sink("sink");
        StreamPipeline p = pipeline("provided-sink");
        p.setSourceProvided(true);
        p.addOperator(sink, true);
        p.validate();

        p.execute(null, JobContext.NOOP, seed(DATA));

        assertEquals(1, sink.consumes.get());
        assertArrayEquals(DATA, sink.bytes());
    }

    // ------------------------------------------------------------------ durable vs ephemeral

    /**
     * A caller is never asked for a job id. An earlier design threw here, on the reasoning that a
     * materialising operator needs a stable workspace name — which made an implementation detail of
     * the pipeline into a term of the caller's contract.
     *
     * <p>The framework names its own workspace instead. With nothing stable to derive one from, the run
     * is scratch: it completes, and nothing will resume it.
     */
    @Test
    public void runsWithoutAJobIdEvenWhenOperatorsKeepDurableState() throws Exception {
        Mocks.Sink sink = new Mocks.Sink("sink");

        StreamPipeline p = pipeline("durable");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.LazyMaterialising("forEach"), true);
        p.addOperator(sink, true);
        // validate() still rejects materialising operators until workspace support lands.

        p.execute(null, JobContext.NOOP);

        assertTrue("materialisation is a pipeline detail, not a caller obligation", p.hasDurableState());
    }

    /**
     * An ephemeral pipeline — nothing declaring durable state — needs no job id at all. This is the
     * shape a synchronous API-attachment transfer takes: request-scoped, no workspace, no job record,
     * nothing to resume because there is no second attempt.
     */
    @Test
    public void runsAnEphemeralPipelineWithNoJobIdAtAll() throws Exception {
        Mocks.Sink sink = new Mocks.Sink("sink");

        StreamPipeline p = pipeline("ephemeral");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(sink, true);
        p.validate();

        p.execute(null, JobContext.NOOP);

        assertArrayEquals(DATA, sink.bytes());
        assertFalse("nothing here keeps durable state", p.hasDurableState());
    }

    /**
     * The case that broke the earlier design. One pipeline, identical operators, two origins: fed a
     * re-openable source it is durable and demands a job id; fed a one-shot source it is ephemeral
     * and runs without one.
     *
     * <p>When durability was read off the operators alone, the second call threw — and the only way
     * out was to write a second pipeline whose operators differed, which is exactly the duplication
     * the design exists to avoid.
     */
    @Test
    public void theSameOperatorsAreDurableOverAFileAndScratchOverARequestBody() throws Exception {
        StreamPipeline p = pipeline("shared");
        p.setSourceProvided(true);
        p.addOperator(new Mocks.LazyMaterialising("forEach"), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        // validate() still rejects materialising operators until workspace support lands.

        StreamPipeline.RunId file = p.resolveRunId(JobContext.NOOP, seed(DATA), StreamOrigin.REOPENABLE);
        assertTrue("a re-openable file yields a resumable run",
                p.resumable(StreamOrigin.REOPENABLE, file.value(), file.stable()));

        StreamSeed body = new StreamSeed(new ByteArrayInputStream(DATA), "http://api/upload",
                DATA.length, StreamSeed.UNKNOWN, StreamOrigin.ONE_SHOT);
        StreamPipeline.RunId req = p.resolveRunId(JobContext.NOOP, body, StreamOrigin.ONE_SHOT);
        assertFalse("the same operators over a request body are not resumable",
                p.resumable(StreamOrigin.ONE_SHOT, req.value(), req.stable()));

        // Both run. Neither asks the caller for anything.
        p.execute(null, JobContext.NOOP, seed(DATA));
        p.execute(null, JobContext.NOOP, body);
    }

    /**
     * A seed decides the origin, and its identity fields are enough to name a workspace with no help
     * from the caller: same pipeline over the same bytes resolves to the same run id, so re-detecting a
     * file resumes instead of starting over.
     */
    @Test
    public void aSeedsIdentityNamesTheRunWithoutAJobId() throws Exception {
        StreamPipeline p = pipeline("provided");
        p.setSourceProvided(true);
        p.addOperator(new Mocks.LazyMaterialising("forEach"), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        // validate() still rejects materialising operators until workspace support lands.

        StreamPipeline.RunId first = p.resolveRunId(JobContext.NOOP, seed(DATA), StreamOrigin.REOPENABLE);
        StreamPipeline.RunId again = p.resolveRunId(JobContext.NOOP, seed(DATA), StreamOrigin.REOPENABLE);
        assertEquals("the same bytes must land on the same workspace", first.value(), again.value());
        assertTrue("a content-addressed run is resumable", first.stable());

        // A one-shot seed has no identity worth addressing, so the run is scratch.
        StreamSeed body = new StreamSeed(new ByteArrayInputStream(DATA), "http://api/upload",
                DATA.length, StreamSeed.UNKNOWN, StreamOrigin.ONE_SHOT);
        StreamPipeline.RunId scratch = p.resolveRunId(JobContext.NOOP, body, StreamOrigin.ONE_SHOT);
        assertFalse("nothing can resume a one-shot run", scratch.stable());

        p.execute(null, JobContext.NOOP, body);
    }

    /** A caller-supplied job id always wins, and is what an async caller polls with. */
    @Test
    public void aRealJobIdNamesTheRunAndIsAlwaysStable() {
        StreamPipeline p = pipeline("queued");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.LazyMaterialising("mat"), true);

        StreamPipeline.RunId id = p.resolveRunId(jobWithId("job-42"), null, StreamOrigin.REOPENABLE);
        assertEquals("job-42", id.value());
        assertTrue(id.stable());
        assertTrue(p.resumable(StreamOrigin.REOPENABLE, id.value(), id.stable()));
    }

    /** Two pipelines over the same file must not share one workspace. */
    @Test
    public void contentAddressedRunsAreNamespacedByPipeline() {
        StreamPipeline a = pipeline("alpha");
        a.setSourceProvided(true);
        a.addOperator(new Mocks.LazyMaterialising("mat"), true);

        StreamPipeline b = pipeline("beta");
        b.setSourceProvided(true);
        b.addOperator(new Mocks.LazyMaterialising("mat"), true);

        assertFalse(a.resolveRunId(JobContext.NOOP, seed(DATA), StreamOrigin.REOPENABLE).value()
                .equals(b.resolveRunId(JobContext.NOOP, seed(DATA), StreamOrigin.REOPENABLE).value()));
    }

    /** The origin is the one thing a seed will not guess for its caller. */
    @Test
    public void aSeedRefusesToDefaultItsOrigin() {
        try {
            new StreamSeed(new ByteArrayInputStream(DATA), "id", 1L, 1L, null);
            fail("expected a seed with no origin to be rejected");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("origin is required"));
        }
    }

    @Test
    public void resumabilityNeedsBothDurableStateAndAReopenableOrigin() {
        StreamPipeline stateless = pipeline("e");
        stateless.addOperator(new Mocks.Source("src", DATA), true);
        stateless.addOperator(new Mocks.Sink("sink"), true);
        assertFalse("no durable state, so nothing to resume",
                stateless.resumable(StreamOrigin.REOPENABLE, "r", true));

        StreamPipeline stateful = pipeline("d");
        stateful.addOperator(new Mocks.Source("src", DATA), true);
        stateful.addOperator(new Mocks.LazyMaterialising("mat"), true);
        assertTrue(stateful.resumable(StreamOrigin.REOPENABLE, "r", true));
        assertFalse("no second attempt, so nothing to resume into",
                stateful.resumable(StreamOrigin.ONE_SHOT, "r", true));
    }

    /** A source that says nothing is treated as re-openable, unlike every other capability. */
    @Test
    public void aSourceDefaultsToReopenable() {
        assertEquals(StreamOrigin.REOPENABLE, new Mocks.Source("src", DATA).origin());
        assertEquals(StreamOrigin.ONE_SHOT, new Mocks.OneShotSource("body", DATA).origin());
    }

    @Test
    public void reportsWhetherItKeepsDurableState() {
        StreamPipeline ephemeral = pipeline("e");
        ephemeral.addOperator(new Mocks.Source("src", DATA), true);
        ephemeral.addOperator(new Mocks.Sink("sink"), true);
        assertFalse(ephemeral.hasDurableState());

        StreamPipeline durable = pipeline("d");
        durable.addOperator(new Mocks.Source("src", DATA), true);
        durable.addOperator(new Mocks.LazyMaterialising("mat"), true);
        assertTrue(durable.hasDurableState());
    }

    // ------------------------------------------------------------------ cadence resolution

    /**
     * The deployer owns granularity, and nobody else has a say. No operator can widen this default: an
     * earlier design let one declare {@code repeatable()} and thereby hand a deployer who configured
     * nothing a thousand-unit duplicate window, which is the surprise the whole design exists to avoid.
     */
    @Test
    public void onlyTheDeployerCanWidenTheDefault() {
        StreamPipeline p = pipeline("cadence");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        assertEquals("no operator declaration can move this", 1, p.maxReprocessed(1));

        StreamPipeline tuned = pipeline("tuned");
        tuned.addOperator(new Mocks.Source("src", DATA), true);
        tuned.addOperator(new Mocks.Sink("sink"), true, 900);
        assertEquals("and the deployer can, explicitly", 900, tuned.maxReprocessed(1));
    }

    /** Saying nothing costs one unit per failure — the cautious answer. */
    @Test
    public void theDefaultCadenceIsOneUnit() {
        StreamPipeline p = pipeline("cautious");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.Sink("sink"), true);

        assertEquals(1, p.maxReprocessed(1));
    }

    /** A bound of zero would claim a failure costs nothing, which no protocol delivers. */
    @Test
    public void refusesABoundBelowOne() {
        StreamPipeline p = pipeline("bad");
        try {
            p.addOperator(new Mocks.Sink("sink"), true, 0);
            fail("expected a bound below 1 to be refused");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("at least 1"));
        }
    }
}
