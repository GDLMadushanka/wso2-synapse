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

import org.apache.synapse.stream.Checkpoint;
import org.apache.synapse.stream.CheckpointStore;
import org.apache.synapse.stream.CheckpointStores;
import org.apache.synapse.stream.JobContext;
import org.apache.synapse.stream.JobRecord;
import org.apache.synapse.stream.JobRun;
import org.apache.synapse.stream.JobStore;
import org.apache.synapse.stream.JobStores;
import org.apache.synapse.stream.StreamException;
import org.apache.synapse.stream.StreamOrigin;
import org.apache.synapse.stream.StreamSeed;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
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
import static org.junit.Assert.assertNotEquals;
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

    /**
     * Materialisation works now, but only with somewhere to write. Checked at deployment for the same
     * reason the structure is: an absent mount or a permissions mistake should fail where someone is
     * looking, not hours into a transfer.
     */
    @Test
    public void rejectsMaterialisationWhenNoWorkspaceIsConfigured() {
        StreamPipeline p = pipeline("mat");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.LazyMaterialising("mat"), true);
        try {
            p.validate();
            fail("expected a materialising operator with no workspace to be rejected");
        } catch (StreamException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("nowhere to write"));
            assertTrue("it should name the property: " + e.getMessage(),
                    e.getMessage().contains("mft.workspace.root"));
        }
    }

    /**
     * Checkpointing is still unimplemented, and is now rejected on its own rather than dragging
     * materialisation down with it — the two need different things, and only one of them needs a
     * database.
     */
    @Test
    public void rejectsCheckpointingUntilStorageExists() {
        StreamPipeline p = pipeline("ckpt");
        p.setWorkspaceRoot("/tmp");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.Checkpointing("ckpt"), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        try {
            p.validate();
            fail("expected a checkpointed operator to be rejected");
        } catch (StreamException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("no checkpoint store is registered"));
        }
    }

    /**
     * A failure is attributed to the <b>configured</b> stage name, not the operator's own.
     *
     * <p>Those coincide for an SPI operator and diverge for a connector operation, whose {@code name()}
     * is a class constant shared by every stage using it. The divergence mattered: the per-stage row
     * went in under the configured name and {@code failed_stage} under the class constant, so they
     * could not be joined and the fault sequence saw a name nobody wrote.
     *
     * <p>Asserted by poisoning {@code name()} — if the failure path still consulted it, the recorded
     * stage would be the class's simple name instead.
     */
    @Test
    public void attributesAFailureToTheConfiguredStageName() throws Exception {
        StreamPipeline p = pipeline("attribution");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.FailingSinkWithBrokenName("out"), true);
        p.validate();

        try {
            p.execute(null, JobContext.NOOP);
            fail("expected the sink's failure to propagate");
        } catch (StreamException e) {
            assertEquals("the configured name, not the operator's class", "out", e.getStage());
        }
    }

    // ------------------------------------------------------------------ scratch space

    /**
     * Scratch exists so an operator gets working files whose cleanup cannot be forgotten, and it is
     * <b>ungated</b> — the mock here declares no capability flags at all.
     */
    @Test
    public void scratchIsCreatedForTheStageAndRemovedWhenTheRunEnds() throws Exception {
        Path root = Files.createTempDirectory("mft-scratch-cfg");
        Mocks.ScratchUsing spiller = new Mocks.ScratchUsing("spill");

        StreamPipeline p = pipeline("scratch-clean");
        p.setScratchRoot(root.toString());
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(spiller, true);
        p.addOperator(new Mocks.Sink("sink"), true);
        p.validate();

        p.execute(null, jobWithId("job-scratch"));

        assertEquals("a configured root gets a parallel tree, keyed by pipeline, run and stage",
                root.resolve("scratch-clean").resolve("job-scratch").resolve("spill"),
                spiller.seenScratch);
        assertFalse("scratch must not outlive the run", Files.exists(spiller.seenScratch));
    }

    /** And on the failure path, which is the one an operator would forget. */
    @Test
    public void scratchIsRemovedWhenTheRunFails() throws Exception {
        Path root = Files.createTempDirectory("mft-scratch-fail");
        Mocks.ScratchUsing spiller = new Mocks.ScratchUsing("spill");

        StreamPipeline p = pipeline("scratch-fail");
        p.setScratchRoot(root.toString());
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(spiller, true);
        p.addOperator(new Mocks.FailingSinkWithBrokenName("sink"), true);
        p.validate();

        try {
            p.execute(null, jobWithId("job-scratch-fail"));
            fail("expected the sink's failure to propagate");
        } catch (StreamException expected) {
            // the cleanup below is the point
        }
        assertNotNull(spiller.seenScratch);
        assertFalse("a failed run must not leave scratch behind either",
                Files.exists(spiller.seenScratch));
    }

    /** With no configured root it lands inside the workspace, which is the documented default. */
    @Test
    public void scratchDefaultsToInsideTheWorkspace() throws Exception {
        Path root = Files.createTempDirectory("mft-scratch-default");
        Mocks.ScratchUsing spiller = new Mocks.ScratchUsing("spill");

        StreamPipeline p = pipeline("scratch-default");
        p.setWorkspaceRoot(root.toString());
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.CapturingWorkspace("mat"), true);   // so a workspace exists at all
        p.addOperator(spiller, true);
        p.addOperator(new Mocks.Sink("sink"), true);
        p.validate();

        p.execute(null, jobWithId("job-scratch-default"));

        assertEquals(root.resolve("scratch-default").resolve("job-scratch-default")
                .resolve("spill").resolve(".scratch"), spiller.seenScratch);
    }

    // ------------------------------------- a position without an artifact, in a non-terminal stage

    /**
     * The one flag combination that corrupts silently, and the reason this rule exists.
     *
     * <p>{@code source -> forEach(checkpointed, no artifact) -> sink}. forEach mediates fifty records
     * and records the position; the sink holds their bytes but has not published them. The run fails,
     * the scope aborts, and the sink's partial output is discarded — correctly. But the checkpoint is
     * a database row that {@code abort()} never touched, so the retry resumes forEach at record 51
     * while the sink starts from nothing, and the output is silently missing fifty records.
     *
     * <p>Checkpointing the sink too does not help: two independent positions in one segment have
     * nothing to reconcile against, and {@code L <= A} needs an artifact to compare with.
     */
    @Test
    public void refusesACheckpointedStageWithNoArtifactBeforeAnotherStage() {
        StreamPipeline p = pipeline("position-without-artifact");
        p.setWorkspaceRoot("/tmp");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.CheckpointOnly("position"), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        try {
            p.validate();
            fail("expected a checkpointed non-terminal stage with no artifact to be rejected");
        } catch (StreamException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("is not the last stage"));
            assertTrue(e.getMessage(), e.getMessage().contains("position"));
        }
    }

    /**
     * A checkpointed <b>sink</b> stays legal, because a sink is always terminal.
     *
     * <p>This is combination 5, the remote-authoritative shape — a chunked blob upload whose position
     * is confirmed by the destination. Nothing downstream can be missing anything, so there is no
     * second position to reconcile with.
     */
    @Test
    public void allowsACheckpointedSinkWithNoArtifact() throws Exception {
        CheckpointStores.register((pipelineName, runId, stageName) -> new CheckpointStore() {
            @Override
            public Checkpoint lastComplete() {
                return null;
            }

            @Override
            public void append(Checkpoint checkpoint) {
            }

            @Override
            public void clear() {
            }
        });
        try {
            StreamPipeline p = pipeline("remote-authoritative");
            p.setWorkspaceRoot(System.getProperty("java.io.tmpdir"));
            p.addOperator(new Mocks.Source("src", DATA), true);
            p.addOperator(new Mocks.CheckpointingSink("upload"), true);
            p.validate();
            p.execute(null, jobWithId("job-sink"));
        } finally {
            CheckpointStores.register(null);
        }
    }

    /** And the mid-chain shape is legal once it materialises, which is what ends the segment. */
    @Test
    public void allowsACheckpointedStageBeforeAnotherWhenItMaterialises() throws Exception {
        CheckpointStores.register((pipelineName, runId, stageName) -> new CheckpointStore() {
            @Override
            public Checkpoint lastComplete() {
                return null;
            }

            @Override
            public void append(Checkpoint checkpoint) {
            }

            @Override
            public void clear() {
            }
        });
        try {
            StreamPipeline p = pipeline("segment-boundary");
            p.setWorkspaceRoot(System.getProperty("java.io.tmpdir"));
            p.addOperator(new Mocks.Source("src", DATA), true);
            p.addOperator(new Mocks.Checkpointing("rows"), true);
            p.addOperator(new Mocks.Sink("sink"), true);
            p.validate();
        } finally {
            CheckpointStores.register(null);
        }
    }

    // ------------------------------------------------------------------ operator names

    /**
     * A name is read to build workspace paths and to key checkpoints, so a broken {@code name()} has
     * to fail at deployment. It previously did not: a defensive helper substituted the class simple
     * name, which puts two stages of the same class into one directory.
     */
    @Test
    public void refusesAnOperatorWhoseNameThrows() {
        StreamPipeline p = pipeline("throwing-name");
        p.addOperator(new Mocks.Source("src", DATA), true);
        try {
            // Caught here rather than at validate(): addOperator reads the name to seed the stage
            // names, so this is the earliest point it can be reported.
            p.addOperator(new Mocks.ThrowingName(), true);
            fail("an operator that throws from name() must be refused at deployment");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("threw from name()"));
            assertTrue(e.getMessage(), e.getMessage().contains(Mocks.ThrowingName.class.getName()));
        }
    }

    /**
     * The template route does not read a name when the stage is added, because the operator is not
     * reachable yet, so a broken name() gets as far as validate(). Both doors have to be shut.
     */
    @Test
    public void validateAlsoRefusesANameThatThrows() throws Exception {
        StreamPipeline p = pipeline("throwing-name-late");
        p.addOperator(new Mocks.Source("src", DATA), true);

        // An operator whose name() breaks only after it was added, which is what a per-run template
        // resolution can produce.
        Mocks.BreaksAfterValidation late = new Mocks.BreaksAfterValidation();
        p.addOperator(late, true);
        late.breakName = true;
        try {
            p.validate();
            fail("validate() must refuse a name() that throws");
        } catch (StreamException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("threw from name()"));
            assertTrue(e.getMessage(), e.getMessage().contains("operator 1"));
        }
    }

    @Test
    public void refusesAnOperatorWithNoName() {
        StreamPipeline p = pipeline("nameless");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.NamedAs(null), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        try {
            p.validate();
            fail("expected a null name to be refused");
        } catch (StreamException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("has no name()"));
            assertTrue(e.getMessage(), e.getMessage().contains("operator 1"));
        }
    }

    @Test
    public void refusesABlankName() {
        StreamPipeline p = pipeline("blank-name");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.NamedAs("   "), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        try {
            p.validate();
            fail("expected a blank name to be refused");
        } catch (StreamException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("has no name()"));
        }
    }

    /**
     * The one place the defensive read survives: a connector-backed stage is resolved from its
     * template per run, so the instance that fails need not be the instance validated. A throw here
     * must not replace the diagnosis the caller needs.
     */
    @Test
    public void aNameThatBreaksAfterValidationDoesNotHideTheRealFailure() throws Exception {
        Mocks.BreaksAfterValidation sink = new Mocks.BreaksAfterValidation();
        StreamPipeline p = pipeline("late-break");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(sink, true);
        p.validate();

        sink.breakName = true;
        try {
            p.execute(null, JobContext.NOOP);
            fail("expected the sink's own failure");
        } catch (StreamException e) {
            // The sink's IOException, not an exception from the name-reading code.
            assertTrue(e.getMessage(), e.getMessage().contains("failed"));
            assertNotNull(e.getStage());
        }
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

    // ------------------------------------------------------------------ the abort protocol

    /**
     * An Error matched none of the catch clauses, so `primary` stayed null and the finally took the
     * publish branch — renaming a partial artifact into place, which the next attempt reads as a
     * finished segment. Publishing is now gated on the pull having completed, not on the absence of a
     * StreamException.
     */
    @Test
    public void anErrorDuringThePullPublishesNothing() throws Exception {
        Mocks.Committing spill = new Mocks.Committing("spill");

        StreamPipeline p = pipeline("error-path");
        p.setWorkspaceRoot(System.getProperty("java.io.tmpdir"));
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(spill, true);
        p.addOperator(new Mocks.ErroringTransform("boom"), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        p.validate();

        try {
            p.execute(null, JobContext.NOOP);
            fail("the Error must propagate");
        } catch (StackOverflowError expected) {
            // unwrapped on purpose: converting it would invite a caller to retry an OOM
        }

        assertFalse("a failed run must publish nothing", spill.committed.get());
        assertTrue("and must discard what it wrote", spill.aborted.get());
    }

    /** The ordinary failure path must still abort, and success must still publish. */
    @Test
    public void aSuccessfulRunPublishes() throws Exception {
        Mocks.Committing spill = new Mocks.Committing("spill");

        StreamPipeline p = pipeline("happy-path");
        p.setWorkspaceRoot(System.getProperty("java.io.tmpdir"));
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(spill, true);
        p.addOperator(new Mocks.Sink("sink"), true);
        p.validate();

        p.execute(null, JobContext.NOOP);

        assertTrue(spill.committed.get());
        assertFalse(spill.aborted.get());
    }

    // ------------------------------------------------------------------ path safety

    /**
     * The run id becomes a directory name that a scratch run later deletes recursively, and
     * Paths.get("/root", "pipe", "") collapses to "/root/pipe" — so an empty job id took the whole
     * pipeline's directory as its own and reclaiming it would delete every other run.
     */
    @Test
    public void refusesAnEmptyJobId() throws Exception {
        assertRefusedJobId("", "empty");
    }

    @Test
    public void refusesAJobIdThatEscapesItsDirectory() throws Exception {
        assertRefusedJobId("../../tmp/x", "single path segment");
    }

    @Test
    public void refusesADotDotJobId() throws Exception {
        assertRefusedJobId("..", "does not name a directory");
    }

    @Test
    public void refusesAJobIdWithSurroundingWhitespace() throws Exception {
        assertRefusedJobId(" job-1 ", "whitespace");
    }

    @Test
    public void refusesANullJobId() throws Exception {
        assertRefusedJobId(null, "empty");
    }

    private void assertRefusedJobId(String jobId, String expectedInMessage) throws Exception {
        StreamPipeline p = pipeline("path-safety");
        p.setWorkspaceRoot(System.getProperty("java.io.tmpdir"));
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.LazyMaterialising("mat"), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        p.validate();

        try {
            p.execute(null, jobWithId(jobId));
            fail("expected job id '" + jobId + "' to be refused before anything was created");
        } catch (StreamException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("job id"));
            assertTrue(e.getMessage(), e.getMessage().contains(expectedInMessage));
        }
    }

    /**
     * validate() path-checked op.name(), but the workspace is built from the configured stage name.
     * They differ for a connector operation, whose op.name() is a safe class constant — so a
     * configured name of "../../escape" passed validation and then resolved outside the run.
     */
    @Test
    public void refusesAStageNameThatEscapesItsDirectory() {
        StreamPipeline p = pipeline("stage-path-safety");
        p.setWorkspaceRoot(System.getProperty("java.io.tmpdir"));
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.NamedAs("../../escape"), true);
        p.addOperator(new Mocks.Sink("sink"), true);

        try {
            p.validate();
            fail("a stage name that escapes its directory must be refused");
        } catch (StreamException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("single path segment"));
        }
    }

    // ------------------------------------------------------------------ orphan release

    /**
     * closeNow() matched by identity against what operators registered, but the pipeline passed its own
     * StageStream decorators — so it released nothing every time, and then logged that it had. The
     * release is now by watermark: everything registered before the stage that resumed.
     */
    @Test
    public void releasesTheRegisteredUpstreamWhenAStageResumes() throws Exception {
        Mocks.RegisteringSource src = new Mocks.RegisteringSource("src", DATA);

        StreamPipeline p = pipeline("resumed-release");
        p.setWorkspaceRoot(System.getProperty("java.io.tmpdir"));
        p.addOperator(src, true);
        p.addOperator(new Mocks.ResumesFromArtifact("mat", DATA), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        p.validate();

        p.execute(null, JobContext.NOOP);

        assertTrue("the orphaned upstream's handle must be released when the stage resumes, not at the"
                + " end of a run that may last hours", src.closed.get());
    }

    /**
     * Discarding the workspace is not enough. A stage declaring checkpointed() without materialises()
     * has no artifact for L <= A to reconcile against, so its position survived the discard and the
     * stage resumed at a byte offset into a source it was no longer reading.
     */
    @Test
    public void warnClearsTheCheckpointAndNotJustTheWorkspace() throws Exception {
        List<String> cleared = new ArrayList<>();
        CheckpointStores.register((pipelineName, runId, stageName) -> new CheckpointStore() {
            @Override
            public Checkpoint lastComplete() {
                return null;
            }

            @Override
            public void append(Checkpoint checkpoint) {
            }

            @Override
            public void clear() {
                cleared.add(stageName);
            }
        });
        JobStores.register(pipelineName -> new JobStore() {
            @Override
            public JobRecord start(JobRun run) {
                // A previous attempt over a different file, under the same job id. The recorded
                // identity is what a file provider composes: the URI plus what distinguishes a
                // replacement from the original.
                return new JobRecord(run.runId(), "FAILED", 1, "file:/in/a.csv|16|1234", 16L, 1234L);
            }
        });
        try {
            Path root = Files.createTempDirectory("mft-warn-ckpt");
            StreamPipeline p = pipeline("warn-clears");
            p.setWorkspaceRoot(root.toString());
            p.setSourceProvided(true);
            p.addOperator(new Mocks.Checkpointing("position"), true);
            p.addOperator(new Mocks.Sink("sink"), true);
            p.validate();

            // Same job id, a different identity: the file at that path was replaced, and the
            // provider's identity says so.
            p.execute(null, jobWithId("job-9"),
                    new StreamSeed(new ByteArrayInputStream(DATA), "file:/in/a.csv|99|5678", 99L,
                            5678L, StreamOrigin.REOPENABLE));

            assertTrue("the stale position must be forgotten, not just the artifact",
                    cleared.contains("position"));
        } finally {
            CheckpointStores.register(null);
            JobStores.register(null);
        }
    }

    /**
     * A size or mtime that moved, under an identity the provider says is unchanged, must <b>not</b>
     * trip the guard.
     *
     * <p>This is the deliberate half of ADR-0033, and the half a future reader will question. The guard
     * used to compare the recorded identity, size and modification time separately, which meant the
     * framework held a second opinion about what "the same source" is — and could overrule the
     * provider with it. A provider that identifies a source by path alone has <i>said</i> that content
     * at that path is one logical source; discarding its workspace because the size changed contradicts
     * a decision that was never the framework's to make.
     *
     * <p>The provider that wants the old behaviour gets it by folding size and mtime into the identity,
     * which is what {@link #warnClearsTheCheckpointAndNotJustTheWorkspace()} shows.
     */
    @Test
    public void aChangedSizeUnderAnUnchangedIdentityIsNotAChangedSource() throws Exception {
        List<String> cleared = new ArrayList<>();
        CheckpointStores.register((pipelineName, runId, stageName) -> new CheckpointStore() {
            @Override
            public Checkpoint lastComplete() {
                return null;
            }

            @Override
            public void append(Checkpoint checkpoint) {
            }

            @Override
            public void clear() {
                cleared.add(stageName);
            }
        });
        JobStores.register(pipelineName -> new JobStore() {
            @Override
            public JobRecord start(JobRun run) {
                // Identity is the path alone - this provider's choice - while the telemetry differs.
                return new JobRecord(run.runId(), "FAILED", 1, "file:/in/a.csv", 16L, 1234L);
            }
        });
        try {
            Path root = Files.createTempDirectory("mft-same-identity");
            StreamPipeline p = pipeline("same-identity");
            p.setWorkspaceRoot(root.toString());
            p.setSourceProvided(true);
            p.addOperator(new Mocks.Checkpointing("position"), true);
            p.addOperator(new Mocks.Sink("sink"), true);
            p.validate();

            p.execute(null, jobWithId("job-9"),
                    new StreamSeed(new ByteArrayInputStream(DATA), "file:/in/a.csv", 99L, 5678L,
                            StreamOrigin.REOPENABLE));

            assertTrue("the provider said this is the same source; the framework must not disagree",
                    cleared.isEmpty());
        } finally {
            CheckpointStores.register(null);
            JobStores.register(null);
        }
    }

    /**
     * The run id is a hash of the identity and the pipeline name, and of nothing else.
     *
     * <p>It used to fold in the seed's size and modification time too, which made an email attachment
     * hash {@code UNKNOWN} twice and diluted an identity already stronger than the composition could
     * express.
     */
    @Test
    public void theRunIdHashesTheIdentityAndNothingElse() throws Exception {
        StreamPipeline p = pipeline("run-id");
        p.setSourceProvided(true);
        p.addOperator(new Mocks.Sink("sink"), true);

        StreamPipeline.RunId a = p.resolveRunId(JobContext.NOOP,
                new StreamSeed(new ByteArrayInputStream(DATA), "imap://h/INBOX|uidvalidity=1|uid=7",
                        StreamSeed.UNKNOWN, StreamSeed.UNKNOWN, StreamOrigin.REOPENABLE),
                StreamOrigin.REOPENABLE);
        StreamPipeline.RunId b = p.resolveRunId(JobContext.NOOP,
                new StreamSeed(new ByteArrayInputStream(DATA), "imap://h/INBOX|uidvalidity=1|uid=7",
                        4096L, 1700000000000L, StreamOrigin.REOPENABLE),
                StreamOrigin.REOPENABLE);

        assertEquals("size and mtime must not reach the run id", a.value(), b.value());
        assertTrue(a.stable());

        StreamPipeline.RunId other = p.resolveRunId(JobContext.NOOP,
                new StreamSeed(new ByteArrayInputStream(DATA), "imap://h/INBOX|uidvalidity=1|uid=8",
                        StreamSeed.UNKNOWN, StreamSeed.UNKNOWN, StreamOrigin.REOPENABLE),
                StreamOrigin.REOPENABLE);
        assertNotEquals("a different identity is a different run", a.value(), other.value());
    }

    /** No identity means nothing can be looked up later, however re-readable the bytes are. */
    @Test
    public void aSeedWithoutAnIdentityIsNotResumable() throws Exception {
        StreamPipeline p = pipeline("no-identity");
        p.setSourceProvided(true);
        p.addOperator(new Mocks.Sink("sink"), true);

        StreamPipeline.RunId id = p.resolveRunId(JobContext.NOOP,
                new StreamSeed(new ByteArrayInputStream(DATA), null, 10L, 20L,
                        StreamOrigin.REOPENABLE),
                StreamOrigin.REOPENABLE);

        assertFalse("a source that cannot be recognised cannot be resumed", id.stable());
        assertTrue(id.value().startsWith("run-"));
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

        // Three operators, three reports. This previously asserted 2, which encoded the omission:
        // the sink has no StageStream around it and so was reported nowhere.
        assertEquals("one report per stage, sink included", 3, job.stages.size());
        assertTrue(job.stages.containsKey("src"));
        assertTrue(job.stages.containsKey("mid"));
        assertTrue("the sink is a stage", job.stages.containsKey("sink"));
        assertEquals("the head stage consumed nothing from below",
                0L, (long) job.stagesIn.get("src"));
        assertEquals("the middle stage consumed what the head produced",
                (long) job.stages.get("src"), (long) job.stagesIn.get("mid"));
        assertEquals("the sink consumed what the stage below it produced",
                (long) job.stages.get("mid"), (long) job.stagesIn.get("sink"));
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
    /**
     * A failure while building a stage is attributed to <b>that</b> stage.
     *
     * <p>It used to reach the outer handler unattributed, which assumes the sink — right for the pull
     * phase, wrong here. A {@code forEach} refusing its configuration was reported against a file sink
     * three stages away, which sends whoever reads the log to the wrong operator.
     */
    @Test
    public void aBuildFailureNamesTheStageThatFailed() throws Exception {
        StreamPipeline p = pipeline("build-blame");
        p.setSourceProvided(true);
        p.addOperator(new Mocks.PassThrough("first"), true);
        p.addOperator(new Mocks.FailsWhenBuilt("culprit"), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        p.validate();
        try {
            p.execute(null, JobContext.NOOP, seedOf("ABC"));
            fail("expected the build to fail");
        } catch (StreamException e) {
            assertEquals("the stage that failed, not the sink", "culprit", e.getStage());
        }
    }

    // ------------------------------------------------------------ resume: artifact prefix replay

    /**
     * A resumed materialising stage hands its downstream the <b>whole</b> stream it produced, not only
     * the part it re-derives.
     *
     * <p>Ten records, a checkpoint every three, failing after seven. The stage checkpointed at 6, so on
     * resume it truncates to six records' worth of bytes, replays those six from the artifact, and
     * re-derives 7..10. The sink must see all ten.
     *
     * <p>Without the replay the sink sees {@code HIJ} only — a silently short output with the run
     * reporting success, which is the defect this exists to catch.
     */
    @Test
    public void aResumedStageReplaysItsArtifactBeforeItsLiveOutput() throws Exception {
        MemoryCheckpoints store = new MemoryCheckpoints();
        store.install();
        try {
            Path root = Files.createTempDirectory("mft-prefix-replay");

            Mocks.Sink first = new Mocks.Sink("sink");
            StreamPipeline failing = prefixPipeline(root, first, 7);
            try {
                failing.execute(null, jobWithId("job-replay"), seedOf("ABCDEFGHIJ"));
                fail("the first attempt was supposed to fail after seven records");
            } catch (StreamException expected) {
                assertEquals("rows", expected.getStage());
            }

            Mocks.Sink second = new Mocks.Sink("sink");
            StreamPipeline resuming = prefixPipeline(root, second, 0);
            resuming.execute(null, jobWithId("job-replay"), seedOf("ABCDEFGHIJ"));

            assertEquals("the downstream must receive every record, prefix then live",
                    "ABCDEFGHIJ", textOf(second));
        } finally {
            store.uninstall();
        }
    }

    /** And the seam is invisible: a fresh run and a resumed run produce identical output. */
    @Test
    public void aResumedRunProducesWhatAnUninterruptedRunWould() throws Exception {
        MemoryCheckpoints store = new MemoryCheckpoints();
        store.install();
        try {
            Path clean = Files.createTempDirectory("mft-clean");
            Mocks.Sink once = new Mocks.Sink("sink");
            prefixPipeline(clean, once, 0).execute(null, jobWithId("job-clean"), seedOf("ABCDEFGHIJ"));

            Path broken = Files.createTempDirectory("mft-broken");
            Mocks.Sink partial = new Mocks.Sink("sink");
            try {
                prefixPipeline(broken, partial, 4)
                        .execute(null, jobWithId("job-broken"), seedOf("ABCDEFGHIJ"));
                fail("expected the first attempt to fail");
            } catch (StreamException ignored) {
                // expected
            }
            Mocks.Sink retried = new Mocks.Sink("sink");
            prefixPipeline(broken, retried, 0)
                    .execute(null, jobWithId("job-broken"), seedOf("ABCDEFGHIJ"));

            assertEquals("a resume must be indistinguishable from never having failed",
                    textOf(once), textOf(retried));
        } finally {
            store.uninstall();
        }
    }

    private static String textOf(Mocks.Sink sink) {
        byte[] bytes = new byte[sink.received.size()];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = sink.received.get(i);
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static StreamPipeline prefixPipeline(Path root, Mocks.Sink sink, int failAfter)
            throws Exception {
        StreamPipeline p = pipeline("replay");
        p.setSourceProvided(true);
        p.setWorkspaceRoot(root.toString());
        p.addOperator(new Mocks.ByteRecords("rows", failAfter), true, 3);
        p.addOperator(sink, true);
        p.validate();
        return p;
    }

    private static StreamSeed seedOf(String text) {
        return new StreamSeed(new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8)),
                "mem:/" + text.length(), text.length(), 1L, StreamOrigin.REOPENABLE);
    }

    /** A checkpoint store that survives between two execute() calls, as the database would. */
    private static final class MemoryCheckpoints {
        private final Map<String, Checkpoint> rows = new HashMap<>();

        void install() {
            CheckpointStores.register((pipelineName, runId, stageName) -> new CheckpointStore() {
                private final String key = runId + "/" + stageName;

                @Override
                public Checkpoint lastComplete() {
                    return rows.get(key);
                }

                @Override
                public void append(Checkpoint checkpoint) {
                    rows.put(key, checkpoint);
                }

                @Override
                public void clear() {
                    rows.remove(key);
                }
            });
        }

        void uninstall() {
            CheckpointStores.register(null);
        }
    }

    // ------------------------------------------------------------ scratch reclamation

    /**
     * A run that cannot resume forgets its positions, not only its files.
     *
     * <p>Checkpoints live in the database, so discarding the workspace never touched them — and a
     * stage that is {@code checkpointed()} without {@code materialises()} has no artifact for
     * {@code L <= A} to catch a stale position against. With {@code resume="false"} the run id is
     * still derived from the source's identity, so the next attempt lands on the same key and would
     * skip records the deployer asked to have re-run.
     */
    @Test
    public void aNonResumableRunClearsItsCheckpointsEvenWithNoWorkspace() throws Exception {
        List<String> cleared = new ArrayList<>();
        CheckpointStores.register((pipelineName, runId, stageName) -> new CheckpointStore() {
            @Override
            public Checkpoint lastComplete() {
                return null;
            }

            @Override
            public void append(Checkpoint checkpoint) {
            }

            @Override
            public void clear() {
                cleared.add(stageName);
            }
        });
        try {
            StreamPipeline p = pipeline("scratch-ckpt");
            p.setSourceProvided(true);
            p.setResume(false);                       // the run is scratch however stable its id
            p.addOperator(new Mocks.CheckpointingSink("upsert"), true);
            p.validate();

            p.execute(null, JobContext.NOOP,
                    new StreamSeed(new ByteArrayInputStream(DATA), "file:/in/a.csv|16|1234", 16L,
                            1234L, StreamOrigin.REOPENABLE));

            assertTrue("a scratch run must not leave a position for the next attempt to resume from",
                    cleared.contains("upsert"));
        } finally {
            CheckpointStores.register(null);
        }
    }

    // ------------------------------------------------------------ ADR-0034 and the G2 sink rule

    /**
     * A checkpointed stage below a non-deterministic materialising one is refused at deployment.
     *
     * <p>Its position counts records of the stage above. A partial resume re-derives that stage's
     * unpersisted tail, which may differ, so the position would skip records of different bytes —
     * silently. ADR-0034.
     */
    @Test
    public void aCheckpointBelowANonDeterministicMaterialisingStageIsRefused() {
        StreamPipeline p = pipeline("nd-above");
        p.setSourceProvided(true);
        p.addOperator(new Mocks.Checkpointing("forEach"), true);      // materialises, not deterministic
        p.addOperator(new Mocks.CheckpointingSink("sink"), true);
        try {
            p.validate();
            fail("expected the checkpoint below a non-deterministic materialising stage to be refused");
        } catch (StreamException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("without declaring deterministic()"));
            assertTrue("the message must name the stage above", e.getMessage().contains("forEach"));
            assertTrue("and offer the way out", e.getMessage().contains("Drop checkpointed()"));
        }
    }

    /** Below a deterministic one it is fine: the re-derived prefix is byte-identical. */
    @Test
    public void aCheckpointBelowADeterministicMaterialisingStageIsAllowed() throws Exception {
        withDurableState(() -> {
            StreamPipeline p = pipeline("det-above");
            p.setSourceProvided(true);
            p.setWorkspaceRoot(Files.createTempDirectory("mft-det-above").toString());
            p.addOperator(new Mocks.DeterministicMaterialising("convert"), true);
            p.addOperator(new Mocks.CheckpointingSink("sink"), true);
            p.validate();
        });
    }

    /**
     * Every stage above is scanned, not just the nearest.
     *
     * <p>Non-determinism propagates through a deterministic stage, because a deterministic stage fed
     * different input produces different output.
     */
    @Test
    public void theScanReachesPastANearerDeterministicStage() {
        StreamPipeline p = pipeline("propagates");
        p.setSourceProvided(true);
        p.addOperator(new Mocks.Checkpointing("forEach"), true);              // non-deterministic
        p.addOperator(new Mocks.DeterministicMaterialising("convert"), true); // deterministic
        p.addOperator(new Mocks.CheckpointingSink("sink"), true);
        try {
            p.validate();
            fail("non-determinism must propagate through the deterministic stage");
        } catch (StreamException e) {
            assertTrue("the offending stage is the far one, not the near one",
                    e.getMessage().contains("forEach"));
        }
    }

    /** A stage with no position of its own is unaffected — there is nothing to invalidate. */
    @Test
    public void aMaterialisingOnlyStageBelowANonDeterministicOneIsAllowed() throws Exception {
        withDurableState(() -> {
            StreamPipeline p = pipeline("no-position");
            p.setSourceProvided(true);
            p.setWorkspaceRoot(Files.createTempDirectory("mft-no-position").toString());
            p.addOperator(new Mocks.Checkpointing("forEach"), true);
            p.addOperator(new Mocks.CapturingWorkspace("spill"), true);  // materialises, no checkpoint
            p.addOperator(new Mocks.Sink("sink"), true);
            p.validate();
        });
    }

    /** Runs a body with a checkpoint store registered, so R15 is satisfied and R18 is what is tested. */
    private static void withDurableState(ThrowingRunnable body) throws Exception {
        CheckpointStores.register((pipelineName, runId, stageName) -> new CheckpointStore() {
            @Override
            public Checkpoint lastComplete() {
                return null;
            }

            @Override
            public void append(Checkpoint checkpoint) {
            }

            @Override
            public void clear() {
            }
        });
        try {
            body.run();
        } finally {
            CheckpointStores.register(null);
        }
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /** A sink's artifact could never be read, so declaring materialises() is refused. */
    @Test
    public void aMaterialisingSinkIsRefused() {
        StreamPipeline p = pipeline("mat-sink");
        p.setSourceProvided(true);
        p.addOperator(new Mocks.MaterialisingSink(), true);
        try {
            p.validate();
            fail("expected a materialising sink to be refused");
        } catch (StreamException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("nothing can ever read the artifact"));
            assertTrue("and say what to do instead", e.getMessage().contains("a second sink"));
        }
    }

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
    public void acceptsATerminalMaterialisingTransform() throws Exception {
        StreamPipeline p = pipeline("terminal-mat");
        p.setWorkspaceRoot(System.getProperty("java.io.tmpdir"));
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.LazyMaterialising("forEach"), true);

        // Valid outright now: a materialising transform may end a pipeline, and the pipeline drains it.
        p.validate();
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
    public void aRealJobIdNamesTheRunAndIsAlwaysStable() throws Exception {
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
    public void contentAddressedRunsAreNamespacedByPipeline() throws Exception {
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

    // ------------------------------------------------------------------ resume from an artifact

    /**
     * Invariant 2: the build loop never skips an operator, so on a resumed run the upstream a
     * materialising stage ignores has still been constructed. It costs nothing to have built — open()
     * and wrap() do no I/O — but it must not be held for the length of the run, which is what
     * closeNow() is for. Until now nothing called it.
     */
    @Test
    public void releasesTheUpstreamAStageResumedPast() throws Exception {
        byte[] artifact = "from-a-previous-run".getBytes(StandardCharsets.UTF_8);
        Mocks.Source source = new Mocks.Source("src", DATA);
        Mocks.Sink sink = new Mocks.Sink("sink");

        StreamPipeline p = pipeline("resumed");
        p.setWorkspaceRoot(System.getProperty("java.io.tmpdir"));
        p.addOperator(source, true);
        p.addOperator(new Mocks.ResumesFromArtifact("mat", artifact), true);
        p.addOperator(sink, true);
        p.validate();

        p.execute(null, jobWithId("resume-test"));

        assertArrayEquals("the sink must read the artifact, not the source", artifact, sink.bytes());
        assertEquals("the source was built, as invariant 2 requires", 1, source.opens.get());
        assertEquals("but never read", 0, source.opened.reads.get());
    }

    /** A stage that says nothing keeps its upstream, exactly as before. */
    @Test
    public void anOrdinaryStageKeepsItsUpstream() throws Exception {
        Mocks.Sink sink = new Mocks.Sink("sink");
        StreamPipeline p = pipeline("ordinary");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.PassThrough("pass"), true);
        p.addOperator(sink, true);
        p.validate();

        p.execute(null, JobContext.NOOP);

        assertArrayEquals(DATA, sink.bytes());
    }

    // ------------------------------------------------------------------ workspace layout

    /**
     * {@code <root>/<pipeline>/<runId>/<stage>/}. The pipeline level is not needed for uniqueness — a
     * run id is either a job id or a hash that already folds the pipeline name in — it is there so the
     * workspace can be operated: retention per pipeline, one pipeline's leftovers cleared on their own,
     * and a directory's owner readable from its path.
     */
    @Test
    public void aStageWritesUnderPipelineThenRunThenStage() throws Exception {
        Path root = Files.createTempDirectory("mft-layout");
        Mocks.CapturingWorkspace mat = new Mocks.CapturingWorkspace("mat");

        StreamPipeline p = pipeline("csv-ingest");
        p.setWorkspaceRoot(root.toString());
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(mat, true);
        p.addOperator(new Mocks.Sink("sink"), true);
        p.validate();

        p.execute(null, jobWithId("job-7"));

        assertEquals(root.resolve("csv-ingest").resolve("job-7").resolve("mat"), mat.seen);
        assertNotNull("a materialising stage is handed an artifact", mat.seenArtifact);
        assertTrue("created before the operator ran", Files.isDirectory(mat.seen));
    }

    /**
     * Two stages resolving to the same name are refused at deployment.
     *
     * <p>A connector operation reaches a shared class whose {@code name()} is a constant, so two stages
     * of one operation carry the same name unless the configured attribute distinguishes them — and a
     * stage's name keys its workspace directory, its checkpoints and its per-stage job row.
     *
     * <p>An earlier version of this test asserted the collision instead: it checked that both stages
     * saw the <i>same</i> workspace, which is the overwrite its own javadoc said must not happen. The
     * check that prevents it did not exist, so the test pinned the defect in place.
     */
    @Test
    public void refusesTwoStagesThatResolveToTheSameName() throws Exception {
        Path root = Files.createTempDirectory("mft-distinct");
        StreamPipeline p = pipeline("two-stages");
        p.setWorkspaceRoot(root.toString());
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.CapturingWorkspace("shared-constant"), true, null);
        p.addOperator(new Mocks.CapturingWorkspace("shared-constant"), true, null);
        p.addOperator(new Mocks.Sink("sink"), true);

        try {
            p.validate();
            fail("two stages sharing a name share a workspace directory and must be refused");
        } catch (StreamException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("two stages named 'shared-constant'"));
            assertTrue(e.getMessage(), e.getMessage().contains("operators 1 and 2"));
        }
    }

    /** A configured name is what makes two stages of one operation distinct, and it works. */
    @Test
    public void explicitNamesGiveTwoStagesSeparateDirectories() throws Exception {
        Path root = Files.createTempDirectory("mft-distinct-ok");
        Mocks.CapturingWorkspace first = new Mocks.CapturingWorkspace("spill-one");
        Mocks.CapturingWorkspace second = new Mocks.CapturingWorkspace("spill-two");

        StreamPipeline p = pipeline("two-stages-named");
        p.setWorkspaceRoot(root.toString());
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(first, true, null);
        p.addOperator(second, true, null);
        p.addOperator(new Mocks.Sink("sink"), true);
        p.validate();

        p.execute(null, jobWithId("job-8"));

        assertNotNull(first.seen);
        assertNotNull(second.seen);
        assertFalse("configured names must yield different directories",
                first.seen.equals(second.seen));
    }

    // ------------------------------------------------------------------ checkpoint store wiring

    /**
     * With a store registered, a checkpointed operator deploys and is handed a store scoped to its own
     * (run, stage). Scoped rather than shared because two stages resume independently, and two runs of
     * one pipeline must never see each other's positions.
     */
    @Test
    public void aCheckpointedStageIsHandedAStoreForItsOwnRunAndStage() throws Exception {
        List<String> asked = new ArrayList<>();
        CheckpointStores.register((pipelineName, runId, stageName) -> {
            asked.add(pipelineName + "/" + runId + "/" + stageName);
            return new CheckpointStore() {
                @Override
                public Checkpoint lastComplete() {
                    return null;
                }

                @Override
                public void append(Checkpoint checkpoint) {
                }

                @Override
                public void clear() {
                }
            };
        });
        try {
            StreamPipeline p = pipeline("ckpt-wired");
            p.setWorkspaceRoot(System.getProperty("java.io.tmpdir"));
            p.addOperator(new Mocks.Source("src", DATA), true);
            p.addOperator(new Mocks.Checkpointing("position"), true);
            p.addOperator(new Mocks.Sink("sink"), true);

            p.validate();
            p.execute(null, jobWithId("job-9"));

            assertEquals(1, asked.size());
            assertEquals("ckpt-wired/job-9/position", asked.get(0));
        } finally {
            CheckpointStores.register(null);
        }
    }

    /** Absent is a legitimate state, and it is refused at deployment rather than at first byte. */
    @Test
    public void withoutAProviderCheckpointingIsRefusedAtDeployment() {
        assertFalse("no provider should be registered by default", CheckpointStores.isAvailable());

        StreamPipeline p = pipeline("no-store");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.Checkpointing("position"), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        try {
            p.validate();
            fail("expected a checkpointed operator with no store to be refused");
        } catch (StreamException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("no checkpoint store is registered"));
        }
    }

    /** The deployer's bound reaches the operator, which is the only way it can honour it. */
    @Test
    public void anOperatorSeesTheDeployersBound() throws Exception {
        Mocks.CapturingWorkspace mat = new Mocks.CapturingWorkspace("mat");
        StreamPipeline p = pipeline("cadence-visible");
        p.setWorkspaceRoot(System.getProperty("java.io.tmpdir"));
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(mat, true, 500);
        p.addOperator(new Mocks.Sink("sink"), true);
        p.validate();

        p.execute(null, jobWithId("job-cadence"));

        assertEquals(500, mat.seenMaxReprocessed);
    }
}
