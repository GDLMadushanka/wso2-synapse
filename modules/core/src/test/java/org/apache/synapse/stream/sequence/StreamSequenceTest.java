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

package org.apache.synapse.stream.sequence;

import org.apache.synapse.stream.Checkpoint;
import org.apache.synapse.stream.spi.CheckpointStore;
import org.apache.synapse.stream.spi.CheckpointStores;
import org.apache.synapse.stream.JobContext;
import org.apache.synapse.stream.spi.JobRecord;
import org.apache.synapse.stream.spi.JobRun;
import org.apache.synapse.stream.spi.JobStore;
import org.apache.synapse.stream.spi.JobStores;
import org.apache.synapse.stream.StreamException;
import org.apache.synapse.stream.StreamOrigin;
import org.apache.synapse.stream.StreamShutdown;
import org.apache.synapse.stream.StreamSeed;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.synapse.stream.StreamSink;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.synapse.stream.StageArtifact;
import org.apache.synapse.stream.StreamContext;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.synapse.stream.StreamSource;
import org.apache.synapse.stream.SourceIdentity;
import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
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
 * Tests for {@link StreamSequence}: the build/pull split, validation, fault attribution, unwind
 * ordering, provided sources and concurrency.
 */
public class StreamSequenceTest {

    private static final byte[] DATA = "hello stream sequence".getBytes(StandardCharsets.UTF_8);

    private static StreamSequence sequence(String name) {
        StreamSequence p = new StreamSequence();
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

        StreamSequence p = sequence("build-only");
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

        StreamSequence p = sequence("full");
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
    public void rejectsASequenceWithNoOperators() {
        assertRejected(sequence("empty"), "has no operators");
    }

    @Test
    public void rejectsAMultiRoleOperator() {
        StreamSequence p = sequence("multi");
        p.addOperator(new Mocks.MultiRole(), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        assertRejected(p, "more than one role interface");
    }

    @Test
    public void rejectsAnOperatorWithNoRole() {
        StreamSequence p = sequence("norole");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.NoRole(), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        assertRejected(p, "implements none of");
    }

    @Test
    public void rejectsASinkThatIsNotLast() {
        StreamSequence p = sequence("earlysink");
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
        StreamSequence p = sequence("twosinks");
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
        StreamSequence p = sequence("nosource");
        p.addOperator(new Mocks.PassThrough("mid"), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        assertRejected(p, "must begin with a StreamSource");
    }

    @Test
    public void rejectsASourceThatIsNotFirst() {
        StreamSequence p = sequence("latesource");
        p.addOperator(new Mocks.Source("a", DATA), true);
        p.addOperator(new Mocks.Source("b", DATA), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        assertRejected(p, "is not first");
    }

    @Test
    public void rejectsADerivedNameOnAnOperatorThatKeepsDurableState() {
        StreamSequence p = sequence("derived");
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
        StreamSequence p = sequence("mat");
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
        StreamSequence p = sequence("ckpt");
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
        StreamSequence p = sequence("attribution");
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

        StreamSequence p = sequence("scratch-clean");
        p.setScratchRoot(root.toString());
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(spiller, true);
        p.addOperator(new Mocks.Sink("sink"), true);
        p.validate();

        p.execute(null, jobWithId("job-scratch"));

        assertEquals("a configured root gets a parallel tree, keyed by sequence, run and stage",
                root.resolve("scratch-clean").resolve("job-scratch").resolve("spill"),
                spiller.seenScratch);
        assertFalse("scratch must not outlive the run", Files.exists(spiller.seenScratch));
    }

    /** And on the failure path, which is the one an operator would forget. */
    @Test
    public void scratchIsRemovedWhenTheRunFails() throws Exception {
        Path root = Files.createTempDirectory("mft-scratch-fail");
        Mocks.ScratchUsing spiller = new Mocks.ScratchUsing("spill");

        StreamSequence p = sequence("scratch-fail");
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

        StreamSequence p = sequence("scratch-default");
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
        StreamSequence p = sequence("position-without-artifact");
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
    /**
     * A position is only ever read by a <b>later attempt</b>. A run invoked inline — no caller job id
     * and no seed, so the run id is a fresh UUID — has no later attempt, so writing one costs a store
     * round trip per unit and leaves rows under an id nothing will ever query.
     *
     * <p>The run must still succeed: the operator asks for its artifact and calls {@code unitDone} in
     * the ordinary way and never learns the difference.
     */
    @Test
    public void aRunThatCannotResumeDiscardsItsPositions() throws Exception {
        AtomicInteger appends = new AtomicInteger();
        CheckpointStores.register((sequenceName, runId, stageName) -> recording(appends));
        try {
            StreamSequence p = sequence("inline-scratch");
            p.setWorkspaceRoot(System.getProperty("java.io.tmpdir"));
            p.addOperator(new Mocks.Source("src", DATA), true);
            p.addOperator(checkpointingSink("upload"), true);
            p.validate();

            // No job id and no seed: resolveRunId falls through to a fresh UUID, so the run is not
            // resumable however reopenable its source happens to be.
            p.execute(null, JobContext.NOOP);

            assertEquals("a run that cannot resume must not write positions", 0, appends.get());
        } finally {
            CheckpointStores.register(null);
        }
    }

    /** The other half: given a job id, the same sequence does record its positions. */
    @Test
    public void aResumableRunRecordsItsPositions() throws Exception {
        AtomicInteger appends = new AtomicInteger();
        CheckpointStores.register((sequenceName, runId, stageName) -> recording(appends));
        try {
            StreamSequence p = sequence("inline-resumable");
            p.setWorkspaceRoot(System.getProperty("java.io.tmpdir"));
            p.addOperator(new Mocks.Source("src", DATA), true);
            p.addOperator(checkpointingSink("upload"), true);
            p.validate();

            p.execute(null, jobWithId("job-resumable"));

            assertTrue("a resumable run records what it did", appends.get() > 0);
        } finally {
            CheckpointStores.register(null);
        }
    }

    private static CheckpointStore recording(AtomicInteger appends) {
        return new CheckpointStore() {
            @Override
            public Checkpoint lastComplete() {
                return null;
            }

            @Override
            public void append(Checkpoint checkpoint) {
                appends.incrementAndGet();
            }

            @Override
            public void clear() {
            }
        };
    }

    /** A sink that really checkpoints, unlike {@code Mocks.CheckpointingSink} which only declares it. */
    private static StreamSink checkpointingSink(String name) {
        return new StreamSink() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public boolean checkpointed() {
                return true;
            }

            @Override
            public void consume(InputStream in, StreamContext ctx) throws StreamException, IOException {
                StageArtifact artifact = ctx.artifact("records", "v1", 1);
                byte[] buffer = new byte[8];
                long units = 0;
                while (in.read(buffer) != -1) {
                    artifact.unitDone(++units);
                }
            }
        };
    }

    @Test
    public void allowsACheckpointedSinkWithNoArtifact() throws Exception {
        CheckpointStores.register((sequenceName, runId, stageName) -> new CheckpointStore() {
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
            StreamSequence p = sequence("remote-authoritative");
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
        CheckpointStores.register((sequenceName, runId, stageName) -> new CheckpointStore() {
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
            StreamSequence p = sequence("segment-boundary");
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
        StreamSequence p = sequence("throwing-name");
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
        StreamSequence p = sequence("throwing-name-late");
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
        StreamSequence p = sequence("nameless");
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
        StreamSequence p = sequence("blank-name");
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
        StreamSequence p = sequence("late-break");
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
    public void acceptsASinkOnlySequenceWhenTheSourceIsProvided() throws Exception {
        Mocks.Sink sink = new Mocks.Sink("sink");
        StreamSequence p = sequence("provided");
        p.setSourceProvided(true);
        p.addOperator(sink, true);
        p.validate();

        p.execute(null, JobContext.NOOP, seed(DATA));

        assertArrayEquals("a sink-only sequence is a legal 'put this stream there' artifact",
                DATA, sink.bytes());
    }

    @Test
    public void rejectsASourceFirstWhenTheSourceIsProvided() {
        StreamSequence p = sequence("provided-with-source");
        p.setSourceProvided(true);
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        assertRejected(p, "must not be a StreamSource");
    }

    @Test
    public void refusesToRunAProvidedSourceSequenceWithNoStream() {
        StreamSequence p = sequence("provided");
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
    public void refusesAStreamWhenTheSequenceOpensItsOwnSource() {
        StreamSequence p = sequence("self");
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

        StreamSequence p = sequence("provided");
        p.setSourceProvided(true);
        p.addOperator(new Mocks.PassThrough("mid"), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        p.validate();

        p.execute(null, JobContext.NOOP,
                new StreamSeed(provided, "test://provided", DATA.length, 0L, StreamOrigin.REOPENABLE));

        assertFalse("the sequence must not close a stream it did not open", provided.closed);
    }

    // ------------------------------------------------------------------ failure handling

    @Test
    public void attributesAFailureToTheStageThatRaisedIt() {
        StreamSequence p = sequence("failing");
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

        StreamSequence p = sequence("failing");
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
        StreamSequence p = sequence("identity");
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
        StreamSequence p = sequence("failing");
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

        StreamSequence p = sequence("error-path");
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

        StreamSequence p = sequence("happy-path");
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
     * sequence's directory as its own and reclaiming it would delete every other run.
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
        StreamSequence p = sequence("path-safety");
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
        StreamSequence p = sequence("stage-path-safety");
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
     * closeNow() matched by identity against what operators registered, but the sequence passed its own
     * StageStream decorators — so it released nothing every time, and then logged that it had. The
     * release is now by watermark: everything registered before the stage that resumed.
     */
    @Test
    public void releasesTheRegisteredUpstreamWhenAStageResumes() throws Exception {
        Mocks.RegisteringSource src = new Mocks.RegisteringSource("src", DATA);

        StreamSequence p = sequence("resumed-release");
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
     * A resumed run tells a seekable source where to start, instead of re-reading the prefix.
     *
     * <p>Both ends read the same number, which is the point: the source begins at
     * {@code upstreamStartsAt()}, and the sink skips its own position minus that — naturally zero when
     * the source honoured it. A second flag kept in step would be a way for them to disagree, and a
     * resume that skips twice or not at all corrupts silently.
     */
    @Test
    public void aResumedRunStartsASeekableSourceAtTheSinksOffset() throws Exception {
        byte[] data = "0123456789abcdefghijklmnopqrstuvwxyz".getBytes(StandardCharsets.UTF_8);
        AtomicLong sourceToldToStartAt = new AtomicLong(-1);
        AtomicLong sinkSawUpstreamAt = new AtomicLong(-1);

        JobStores.register(sequenceName -> new JobStore() {
            @Override
            public JobRecord start(JobRun run) {
                return null;
            }
        });
        try {
            Path root = Files.createTempDirectory("mft-seek");
            StreamSequence p = sequence("seeking");
            p.setWorkspaceRoot(root.toString());
            // A seekable source and a sink that claims to have written the first 10 bytes.
            p.addOperator(new SeekableSource("src", data, sourceToldToStartAt), true);
            p.addOperator(new OffsetReportingSink("sink", 10L, sinkSawUpstreamAt), true);
            p.validate();
            p.execute(null, jobWithId("job-seek"));

            assertEquals("the source must be told to start where the sink resumes",
                    10L, sourceToldToStartAt.get());
            assertEquals("the sink must read the same number, so its own skip comes out zero",
                    10L, sinkSawUpstreamAt.get());
        } finally {
            JobStores.register(null);
        }
    }

    /** A source that records what offset it was asked to begin at. */
    private static final class SeekableSource implements StreamSource {
        private final String name;
        private final byte[] data;
        private final AtomicLong toldToStartAt;

        SeekableSource(String name, byte[] data, AtomicLong toldToStartAt) {
            this.name = name;
            this.data = data;
            this.toldToStartAt = toldToStartAt;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean materialises() {
            return true;                      // so the run has durable state and is resumable
        }

        @Override
        public boolean seekable() {
            return true;
        }

        @Override
        public SourceIdentity identify(StreamContext ctx) {
            return new SourceIdentity("file:/in/a.csv", data.length, 1L);
        }

        @Override
        public InputStream open(StreamContext ctx) {
            long at = ctx.upstreamStartsAt();
            toldToStartAt.set(at);
            return ctx.resources().register(name,
                    new ByteArrayInputStream(data, (int) at, data.length - (int) at));
        }
    }

    /** A sink that says it already wrote {@code resumeAt} bytes, and records what it was told. */
    private static final class OffsetReportingSink implements StreamSink {
        private final String name;
        private final long resumeAt;
        private final AtomicLong sawUpstreamAt;

        OffsetReportingSink(String name, long resumeAt, AtomicLong sawUpstreamAt) {
            this.name = name;
            this.resumeAt = resumeAt;
            this.sawUpstreamAt = sawUpstreamAt;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public long durableByteOffset(StreamContext ctx) {
            return resumeAt;
        }

        @Override
        public void consume(InputStream in, StreamContext ctx) throws IOException {
            sawUpstreamAt.set(ctx.upstreamStartsAt());
            while (in.read() >= 0) {
                // drain
            }
        }
    }

    /**
     * A self-sourcing sequence must tell its stages how large the source is.
     *
     * <p>{@code identify()} is the only thing that knows before a byte moves, and the framework already
     * calls it for the identity guard. It used to keep the identity and throw the size away:
     * {@code sourceOpened} was fed from the seed alone, so a sequence that opened its own source
     * reported UNKNOWN to everything downstream, always.
     *
     * <p>That mattered because an operator cannot ask the source itself. This is a pull sequence, so
     * the sink's {@code consume()} — and any {@code expectedSize()} it reads — runs <b>before</b> the
     * first {@code read()} reaches the source. A size announced by the source on its first read is
     * always too late for the stage that needed it.
     */
    @Test
    public void aSelfSourcingSequenceReportsTheProbedSizeBeforeAnyStageRuns() throws Exception {
        byte[] data = "twenty-six bytes of source.".getBytes(StandardCharsets.UTF_8);
        AtomicReference<Long> seenBySink = new AtomicReference<>();

        JobStores.register(sequenceName -> new JobStore() {
            @Override
            public JobRecord start(JobRun run) {
                return null;
            }
        });
        try {
            Path root = Files.createTempDirectory("mft-probed-size");
            StreamSequence p = sequence("probed-size");
            p.setWorkspaceRoot(root.toString());
            // sourceProvided stays false: the sequence opens its own source, which is the case that
            // reported UNKNOWN.
            p.addOperator(new Mocks.IdentifyingSource("src", data, "file:/in/a.csv"), true);
            p.addOperator(new StreamSink() {
                @Override
                public String name() {
                    return "sink";
                }

                @Override
                public void consume(InputStream in, StreamContext ctx) throws IOException {
                    // Read at the top of consume, before draining, which is when a real sink sizes
                    // its work.
                    seenBySink.set(ctx.job().expectedSize());
                    while (in.read() >= 0) {
                        // drain
                    }
                }
            }, true);
            p.validate();

            // A real job id, so the run id is stable and the run is resumable. That matters: the
            // framework only probes the source when a resume could happen (identity exists to guard
            // one), so a scratch run is still told UNKNOWN and still sizes blind. Every queued MFT
            // transfer is resumable, which is the case this is about.
            p.execute(null, jobWithId("job-size"));

            assertEquals("the sink must be told the size identify() already reported",
                    Long.valueOf(data.length), seenBySink.get());
        } finally {
            JobStores.register(null);
        }
    }

    /**
     * Discarding the workspace is not enough. A stage declaring checkpointed() without materialises()
     * has no artifact for L <= A to reconcile against, so its position survived the discard and the
     * stage resumed at a byte offset into a source it was no longer reading.
     */
    @Test
    public void warnClearsTheCheckpointAndNotJustTheWorkspace() throws Exception {
        List<String> cleared = new ArrayList<>();
        CheckpointStores.register((sequenceName, runId, stageName) -> new CheckpointStore() {
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
        JobStores.register(sequenceName -> new JobStore() {
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
            StreamSequence p = sequence("warn-clears");
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
        CheckpointStores.register((sequenceName, runId, stageName) -> new CheckpointStore() {
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
        JobStores.register(sequenceName -> new JobStore() {
            @Override
            public JobRecord start(JobRun run) {
                // Identity is the path alone - this provider's choice - while the telemetry differs.
                return new JobRecord(run.runId(), "FAILED", 1, "file:/in/a.csv", 16L, 1234L);
            }
        });
        try {
            Path root = Files.createTempDirectory("mft-same-identity");
            StreamSequence p = sequence("same-identity");
            p.setWorkspaceRoot(root.toString());
            p.setSourceProvided(true);
            p.addOperator(new Mocks.Checkpointing("position"), true);
            // Deliberately made to fail. Reclaiming a finished run also clears its positions, so after
            // a SUCCESSFUL run "cleared" cannot distinguish the guard from the reclaim. A failed run
            // keeps its state, which leaves the guard as the only thing that could have cleared.
            p.addOperator(new Mocks.FailingTransform("boom"), true);
            p.addOperator(new Mocks.Sink("sink"), true);
            p.validate();

            try {
                p.execute(null, jobWithId("job-9"),
                        new StreamSeed(new ByteArrayInputStream(DATA), "file:/in/a.csv", 99L, 5678L,
                                StreamOrigin.REOPENABLE));
                fail("the failing stage was supposed to fail");
            } catch (StreamException expected) {
                assertEquals("boom", expected.getStage());
            }

            assertTrue("the provider said this is the same source; the framework must not disagree",
                    cleared.isEmpty());
        } finally {
            CheckpointStores.register(null);
            JobStores.register(null);
        }
    }

    /**
     * The run id is a hash of the identity and the sequence name, and of nothing else.
     *
     * <p>It used to fold in the seed's size and modification time too, which made an email attachment
     * hash {@code UNKNOWN} twice and diluted an identity already stronger than the composition could
     * express.
     */
    @Test
    public void theRunIdHashesTheIdentityAndNothingElse() throws Exception {
        StreamSequence p = sequence("run-id");
        p.setSourceProvided(true);
        p.addOperator(new Mocks.Sink("sink"), true);

        StreamSequence.RunId a = p.resolveRunId(JobContext.NOOP,
                new StreamSeed(new ByteArrayInputStream(DATA), "imap://h/INBOX|uidvalidity=1|uid=7",
                        StreamSeed.UNKNOWN, StreamSeed.UNKNOWN, StreamOrigin.REOPENABLE),
                StreamOrigin.REOPENABLE);
        StreamSequence.RunId b = p.resolveRunId(JobContext.NOOP,
                new StreamSeed(new ByteArrayInputStream(DATA), "imap://h/INBOX|uidvalidity=1|uid=7",
                        4096L, 1700000000000L, StreamOrigin.REOPENABLE),
                StreamOrigin.REOPENABLE);

        assertEquals("size and mtime must not reach the run id", a.value(), b.value());
        assertTrue(a.stable());

        StreamSequence.RunId other = p.resolveRunId(JobContext.NOOP,
                new StreamSeed(new ByteArrayInputStream(DATA), "imap://h/INBOX|uidvalidity=1|uid=8",
                        StreamSeed.UNKNOWN, StreamSeed.UNKNOWN, StreamOrigin.REOPENABLE),
                StreamOrigin.REOPENABLE);
        assertNotEquals("a different identity is a different run", a.value(), other.value());
    }

    /** No identity means nothing can be looked up later, however re-readable the bytes are. */
    @Test
    public void aSeedWithoutAnIdentityIsNotResumable() throws Exception {
        StreamSequence p = sequence("no-identity");
        p.setSourceProvided(true);
        p.addOperator(new Mocks.Sink("sink"), true);

        StreamSequence.RunId id = p.resolveRunId(JobContext.NOOP,
                new StreamSeed(new ByteArrayInputStream(DATA), null, 10L, 20L,
                        StreamOrigin.REOPENABLE),
                StreamOrigin.REOPENABLE);

        assertFalse("a source that cannot be recognised cannot be resumed", id.stable());
        assertTrue("the id says so on sight", id.value().startsWith("mft-run-"));
    }

    // ------------------------------------------------------------------ telemetry

    @Test
    public void reportsEveryStageWithBytesInAndOut() throws Exception {
        RecordingJob job = new RecordingJob();

        StreamSequence p = sequence("telemetry");
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
        assertEquals("the head stage consumed nothing from upstream",
                0L, (long) job.stagesIn.get("src"));
        assertEquals("the middle stage consumed what the head produced",
                (long) job.stages.get("src"), (long) job.stagesIn.get("mid"));
        assertEquals("the sink consumed what the stage upstream of it produced",
                (long) job.stages.get("mid"), (long) job.stagesIn.get("sink"));
    }

    @Test
    public void reportsStagesEvenOnTheFailurePath() {
        RecordingJob job = new RecordingJob();

        StreamSequence p = sequence("failing");
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
        StreamSequence p = sequence("concurrent");
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
     * A job context with a real id. Required by any sequence whose operators keep durable state —
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
        StreamSequence p = sequence("build-blame");
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

    // ------------------------------------------------------------ graceful shutdown

    /**
     * A shutdown stops a <b>resumable</b> run, and the position it had earned is recorded.
     *
     * <p>Not "run until the next checkpoint": that waits a whole interval to save at most
     * {@code checkpointFrequency} units of rework. Stopping at the next unit and writing what is already
     * finished costs one record instead, and loses nothing. ADR-0037.
     */
    @Test
    public void aShutdownStopsAResumableRunAndKeepsWhatItEarned() throws Exception {
        MemoryCheckpoints store = new MemoryCheckpoints();
        store.install();
        try {
            Path root = Files.createTempDirectory("mft-shutdown");
            Mocks.Sink sink = new Mocks.Sink("sink");
            StreamSequence p = sequence("stopping");
            p.setSourceProvided(true);
            p.setWorkspaceRoot(root.toString());
            // An interval of 1000 over ten records: without a flush on stopping, nothing would ever be
            // recorded and the whole run would be repeated.
            // The signal arrives after four records, as a shutdown would mid-transfer.
            p.addOperator(new Mocks.ByteRecords("rows", 0, 4, 1000), true);
            p.addOperator(sink, true);
            p.validate();

            try {
                p.execute(null, jobWithId("job-stop"), seedOf("ABCDEFGHIJ"));
                fail("a run that stopped short of its source's end did not finish, and saying so is"
                        + " the only thing that stops the caller consuming the source");
            } catch (StreamException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("stopped before the transfer"));
                assertTrue("whoever drives retries must be told to try again", e.isRetryable());
            }

            assertEquals("a resumable run stops at the next unit, keeping what finished",
                    "ABCD", textOf(sink));
            Checkpoint recorded = store.rows.get("job-stop/rows");
            assertNotNull("the position it reached must be recorded, not thrown away", recorded);
            assertEquals("and it must be what actually completed",
                    textOf(sink).length(), recorded.position());
            // The half that used to undo the other: a run that looks finished reclaims its workspace
            // and clears its positions, which deleted the flush above one line after writing it.
            assertTrue("a stopped run keeps the workspace its position describes",
                    Files.exists(root.resolve("stopping").resolve("job-stop")));
        } finally {
            StreamShutdown.reset();
            store.uninstall();
        }
    }

    /**
     * A run nothing could redo is left to finish.
     *
     * <p>An API request body is {@code ONE_SHOT}: stopping it destroys bytes no retry can obtain again,
     * so it gets the same contract as any other in-flight mediation. `resumable` is the discriminator
     * rather than the origin, because an API over a re-openable file source has no seed either.
     */
    @Test
    public void aShutdownLetsANonResumableRunFinish() throws Exception {
        MemoryCheckpoints store = new MemoryCheckpoints();
        store.install();
        try {
            Path root = Files.createTempDirectory("mft-shutdown-oneshot");
            Mocks.Sink sink = new Mocks.Sink("sink");
            StreamSequence p = sequence("finishing");
            p.setSourceProvided(true);
            p.setWorkspaceRoot(root.toString());
            p.addOperator(new Mocks.ByteRecords("rows", 0, 0, 1000), true);
            p.addOperator(sink, true);
            p.validate();

            StreamShutdown.request();
            p.execute(null, JobContext.NOOP,
                    new StreamSeed(new ByteArrayInputStream("ABCDEFGHIJ".getBytes(StandardCharsets.UTF_8)),
                            "mem:/one-shot", 10L, 1L, StreamOrigin.ONE_SHOT));

            assertEquals("nothing would redo this, so it must not be cut short",
                    "ABCDEFGHIJ", textOf(sink));
        } finally {
            StreamShutdown.reset();
            store.uninstall();
        }
    }

    /** The signal counts transfers, so a shutdown can wait for them. */
    @Test
    public void inFlightTransfersAreCountedAndWaitedFor() throws Exception {
        StreamShutdown.reset();
        assertEquals(0, StreamShutdown.inFlight());
        assertTrue("nothing running means immediately quiet", StreamShutdown.awaitQuiet(50));

        StreamShutdown.transferStarted();
        try {
            assertEquals(1, StreamShutdown.inFlight());
            assertFalse("a running transfer must hold the wait", StreamShutdown.awaitQuiet(50));
        } finally {
            StreamShutdown.transferEnded();
        }
        assertTrue(StreamShutdown.awaitQuiet(50));
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
            StreamSequence failing = prefixSequence(root, first, 7);
            try {
                failing.execute(null, jobWithId("job-replay"), seedOf("ABCDEFGHIJ"));
                fail("the first attempt was supposed to fail after seven records");
            } catch (StreamException expected) {
                assertEquals("rows", expected.getStage());
            }

            Mocks.Sink second = new Mocks.Sink("sink");
            StreamSequence resuming = prefixSequence(root, second, 0);
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
            prefixSequence(clean, once, 0).execute(null, jobWithId("job-clean"), seedOf("ABCDEFGHIJ"));

            Path broken = Files.createTempDirectory("mft-broken");
            Mocks.Sink partial = new Mocks.Sink("sink");
            try {
                prefixSequence(broken, partial, 4)
                        .execute(null, jobWithId("job-broken"), seedOf("ABCDEFGHIJ"));
                fail("expected the first attempt to fail");
            } catch (StreamException ignored) {
                // expected
            }
            Mocks.Sink retried = new Mocks.Sink("sink");
            prefixSequence(broken, retried, 0)
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

    private static StreamSequence prefixSequence(Path root, Mocks.Sink sink, int failAfter)
            throws Exception {
        StreamSequence p = sequence("replay");
        p.setSourceProvided(true);
        p.setWorkspaceRoot(root.toString());
        p.addOperator(new Mocks.ByteRecords("rows", failAfter, 0, 3), true);
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
        final Map<String, Checkpoint> rows = new HashMap<>();

        void install() {
            CheckpointStores.register((sequenceName, runId, stageName) -> new CheckpointStore() {
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
        CheckpointStores.register((sequenceName, runId, stageName) -> new CheckpointStore() {
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
            StreamSequence p = sequence("scratch-ckpt");
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
     * A checkpointed stage downstream of a non-deterministic materialising one is refused at deployment.
     *
     * <p>Its position counts records of the stage above. A partial resume re-derives that stage's
     * unpersisted tail, which may differ, so the position would skip records of different bytes —
     * silently. ADR-0034.
     */
    @Test
    public void aCheckpointBelowANonDeterministicMaterialisingStageIsRefused() {
        StreamSequence p = sequence("nd-above");
        p.setSourceProvided(true);
        p.addOperator(new Mocks.Checkpointing("forEach"), true);      // materialises, not deterministic
        p.addOperator(new Mocks.CheckpointingSink("sink"), true);
        try {
            p.validate();
            fail("expected the checkpoint downstream of a non-deterministic materialising stage"
                    + " to be refused");
        } catch (StreamException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("without declaring deterministic()"));
            assertTrue("the message must name the stage above", e.getMessage().contains("forEach"));
            assertTrue("and offer the way out", e.getMessage().contains("Drop checkpointed()"));
        }
    }

    /**
     * The deployer's answer is honoured where the operator has said it cannot answer itself.
     *
     * <p>A per-record loop emits whatever the configured sequence emits. If those mediators are a pure
     * transform of each record, the stage really is deterministic — and the only party who can see that
     * is whoever wrote them.
     */
    @Test
    public void aDeployerMayDeclareADeferringStageDeterministic() throws Exception {
        withDurableState(() -> {
            StreamSequence p = sequence("deployer-says-yes");
            p.setSourceProvided(true);
            p.setWorkspaceRoot(Files.createTempDirectory("mft-deployer-det").toString());
            p.addOperator(new Mocks.DeferringMaterialising("forEach"), true, Boolean.TRUE);
            p.addOperator(new Mocks.CheckpointingSink("sink"), true);

            p.validate();       // would be refused without the attribute
        });
    }

    /** And the other way: an explicit false is the same as saying nothing. */
    @Test
    public void aDeployerMayAlsoDeclareItNonDeterministic() {
        StreamSequence p = sequence("deployer-says-no");
        p.setSourceProvided(true);
        p.addOperator(new Mocks.DeferringMaterialising("forEach"), true, Boolean.FALSE);
        p.addOperator(new Mocks.CheckpointingSink("sink"), true);
        try {
            p.validate();
            fail("expected deterministic=false to leave the rule in force");
        } catch (StreamException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("without declaring deterministic()"));
        }
    }

    /** With no attribute the operator still decides, so nothing about existing sequences changes. */
    @Test
    public void aDeferringStageIsStillNonDeterministicWithNoAttribute() {
        StreamSequence p = sequence("no-attribute");
        p.setSourceProvided(true);
        p.addOperator(new Mocks.DeferringMaterialising("forEach"), true);
        p.addOperator(new Mocks.CheckpointingSink("sink"), true);
        try {
            p.validate();
            fail("expected the rule to hold when the deployer said nothing");
        } catch (StreamException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("without declaring deterministic()"));
        }
    }

    /**
     * Refused, not ignored, on an operator that answers for itself. Silently dropping it would leave a
     * deployer believing they had asserted something — and then building on the belief.
     */
    @Test
    public void theAttributeIsRefusedOnAnOperatorThatAnswersForItself() {
        StreamSequence p = sequence("not-yours-to-say");
        p.setSourceProvided(true);
        p.addOperator(new Mocks.Checkpointing("forEach"), true, Boolean.TRUE);
        p.addOperator(new Mocks.CheckpointingSink("sink"), true);
        try {
            p.validate();
            fail("expected the attribute to be refused where the operator answers for itself");
        } catch (StreamException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("answers that itself"));
            assertTrue("the message must name the stage", e.getMessage().contains("forEach"));
        }
    }

    /** Below a deterministic one it is fine: the re-derived prefix is byte-identical. */
    @Test
    public void aCheckpointBelowADeterministicMaterialisingStageIsAllowed() throws Exception {
        withDurableState(() -> {
            StreamSequence p = sequence("det-above");
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
        StreamSequence p = sequence("propagates");
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
            StreamSequence p = sequence("no-position");
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
        CheckpointStores.register((sequenceName, runId, stageName) -> new CheckpointStore() {
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
        StreamSequence p = sequence("mat-sink");
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

    private static void assertRejected(StreamSequence p, String expectedInMessage) {
        try {
            p.validate();
            fail("expected validate() to reject this sequence for: " + expectedInMessage);
        } catch (StreamException e) {
            assertTrue("message should mention '" + expectedInMessage + "' but was: "
                    + e.getMessage(), e.getMessage().contains(expectedInMessage));
        }
    }

    /** Tracks whether the sequence closed a stream it was handed. */
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

    /** Captures what the sequence reports. */
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
        StreamSequence p = sequence("terminal-mat");
        p.setWorkspaceRoot(System.getProperty("java.io.tmpdir"));
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.LazyMaterialising("forEach"), true);

        // Valid outright now: a materialising transform may end a sequence, and the sequence drains it.
        p.validate();
    }

    @Test
    public void rejectsATerminalTransformWithNoDurableEffect() {
        StreamSequence p = sequence("terminal-plain");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.PassThrough("mid"), true);
        assertRejected(p, "must end with a StreamSink");
    }

    /**
     * With no sink, the sequence pulls the terminal stage itself. Asserted on the operator's own
     * record of what it wrote, so this proves the work happened rather than just that a loop ran.
     */
    @Test
    public void drainsTheTerminalStageWhenThereIsNoSink() throws Exception {
        Mocks.LazyMaterialising terminal = new Mocks.LazyMaterialising("forEach");

        StreamSequence p = sequence("drain");
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

        StreamSequence p = sequence("drain-telemetry");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.LazyMaterialising("forEach"), true);

        p.execute(null, job);

        assertEquals("the terminal stage produced every byte",
                (long) DATA.length, (long) job.stages.get("forEach"));
    }

    /**
     * Without this, a sequence ending in a materialising transform would be the one shape that
     * cannot be cancelled, because there is no sink polling on its behalf.
     */
    @Test
    public void drainingRespectsCancellation() {
        StreamSequence p = sequence("drain-cancel");
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
    public void aSinkOnlyProvidedSourceSequenceStillNeedsNoDrain() throws Exception {
        // Regression guard: the sink path and the drain path must not both run.
        Mocks.Sink sink = new Mocks.Sink("sink");
        StreamSequence p = sequence("provided-sink");
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
     * the sequence into a term of the caller's contract.
     *
     * <p>The framework names its own workspace instead. With nothing stable to derive one from, the run
     * is scratch: it completes, and nothing will resume it.
     */
    @Test
    public void runsWithoutAJobIdEvenWhenOperatorsKeepDurableState() throws Exception {
        Mocks.Sink sink = new Mocks.Sink("sink");

        StreamSequence p = sequence("durable");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.LazyMaterialising("forEach"), true);
        p.addOperator(sink, true);
        // validate() still rejects materialising operators until workspace support lands.

        p.execute(null, JobContext.NOOP);

        assertTrue("materialisation is a sequence detail, not a caller obligation", p.hasDurableState());
    }

    /**
     * An ephemeral sequence — nothing declaring durable state — needs no job id at all. This is the
     * shape a synchronous API-attachment transfer takes: request-scoped, no workspace, no job record,
     * nothing to resume because there is no second attempt.
     */
    @Test
    public void runsAnEphemeralSequenceWithNoJobIdAtAll() throws Exception {
        Mocks.Sink sink = new Mocks.Sink("sink");

        StreamSequence p = sequence("ephemeral");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(sink, true);
        p.validate();

        p.execute(null, JobContext.NOOP);

        assertArrayEquals(DATA, sink.bytes());
        assertFalse("nothing here keeps durable state", p.hasDurableState());
    }

    /**
     * The case that broke the earlier design. One sequence, identical operators, two origins: fed a
     * re-openable source it is durable and demands a job id; fed a one-shot source it is ephemeral
     * and runs without one.
     *
     * <p>When durability was read off the operators alone, the second call threw — and the only way
     * out was to write a second sequence whose operators differed, which is exactly the duplication
     * the design exists to avoid.
     */
    @Test
    public void theSameOperatorsAreDurableOverAFileAndScratchOverARequestBody() throws Exception {
        StreamSequence p = sequence("shared");
        p.setSourceProvided(true);
        p.addOperator(new Mocks.LazyMaterialising("forEach"), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        // validate() still rejects materialising operators until workspace support lands.

        StreamSequence.RunId file = p.resolveRunId(JobContext.NOOP, seed(DATA), StreamOrigin.REOPENABLE);
        assertTrue("a re-openable file yields a resumable run",
                p.resumable(StreamOrigin.REOPENABLE, file.value(), file.stable()));

        StreamSeed body = new StreamSeed(new ByteArrayInputStream(DATA), "http://api/upload",
                DATA.length, StreamSeed.UNKNOWN, StreamOrigin.ONE_SHOT);
        StreamSequence.RunId req = p.resolveRunId(JobContext.NOOP, body, StreamOrigin.ONE_SHOT);
        assertFalse("the same operators over a request body are not resumable",
                p.resumable(StreamOrigin.ONE_SHOT, req.value(), req.stable()));

        // Both run. Neither asks the caller for anything.
        p.execute(null, JobContext.NOOP, seed(DATA));
        p.execute(null, JobContext.NOOP, body);
    }

    /**
     * A seed decides the origin, and its identity fields are enough to name a workspace with no help
     * from the caller: same sequence over the same bytes resolves to the same run id, so re-detecting a
     * file resumes instead of starting over.
     */
    @Test
    public void aSeedsIdentityNamesTheRunWithoutAJobId() throws Exception {
        StreamSequence p = sequence("provided");
        p.setSourceProvided(true);
        p.addOperator(new Mocks.LazyMaterialising("forEach"), true);
        p.addOperator(new Mocks.Sink("sink"), true);
        // validate() still rejects materialising operators until workspace support lands.

        StreamSequence.RunId first = p.resolveRunId(JobContext.NOOP, seed(DATA), StreamOrigin.REOPENABLE);
        StreamSequence.RunId again = p.resolveRunId(JobContext.NOOP, seed(DATA), StreamOrigin.REOPENABLE);
        assertEquals("the same bytes must land on the same workspace", first.value(), again.value());
        assertTrue("a content-addressed run is resumable", first.stable());

        // A one-shot seed has no identity worth addressing, so the run is scratch.
        StreamSeed body = new StreamSeed(new ByteArrayInputStream(DATA), "http://api/upload",
                DATA.length, StreamSeed.UNKNOWN, StreamOrigin.ONE_SHOT);
        StreamSequence.RunId scratch = p.resolveRunId(JobContext.NOOP, body, StreamOrigin.ONE_SHOT);
        assertFalse("nothing can resume a one-shot run", scratch.stable());

        p.execute(null, JobContext.NOOP, body);
    }

    /** A caller-supplied job id always wins, and is what an async caller polls with. */
    @Test
    public void aRealJobIdNamesTheRunAndIsAlwaysStable() throws Exception {
        StreamSequence p = sequence("queued");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.LazyMaterialising("mat"), true);

        StreamSequence.RunId id = p.resolveRunId(jobWithId("job-42"), null, StreamOrigin.REOPENABLE);
        assertEquals("job-42", id.value());
        assertTrue(id.stable());
        assertTrue(p.resumable(StreamOrigin.REOPENABLE, id.value(), id.stable()));
    }

    /** Two sequences over the same file must not share one workspace. */
    @Test
    public void contentAddressedRunsAreNamespacedBySequence() throws Exception {
        StreamSequence a = sequence("alpha");
        a.setSourceProvided(true);
        a.addOperator(new Mocks.LazyMaterialising("mat"), true);

        StreamSequence b = sequence("beta");
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
        StreamSequence stateless = sequence("e");
        stateless.addOperator(new Mocks.Source("src", DATA), true);
        stateless.addOperator(new Mocks.Sink("sink"), true);
        assertFalse("no durable state, so nothing to resume",
                stateless.resumable(StreamOrigin.REOPENABLE, "r", true));

        StreamSequence stateful = sequence("d");
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
        StreamSequence ephemeral = sequence("e");
        ephemeral.addOperator(new Mocks.Source("src", DATA), true);
        ephemeral.addOperator(new Mocks.Sink("sink"), true);
        assertFalse(ephemeral.hasDurableState());

        StreamSequence durable = sequence("d");
        durable.addOperator(new Mocks.Source("src", DATA), true);
        durable.addOperator(new Mocks.LazyMaterialising("mat"), true);
        assertTrue(durable.hasDurableState());
    }

    // ------------------------------------------------------------------ cadence resolution

    /**
     * The framework holds no cadence of its own any more.
     *
     * <p>It used to: a framework-reserved {@code checkpointFrequency} attribute on the stage, read
     * by the sequence and handed to the artifact. ADR-0067 moved it to the operator, because an
     * attribute cannot appear in a connector's parameter form and so could not be configured from
     * the tooling. What the operator passes to {@link StreamContext#artifact(String, String, int)}
     * is now the only cadence there is, which this proves end to end: the same sequence, two
     * operators differing only in the number they pass, two different checkpoint counts.
     *
     * <p>ADR-0025 is untouched by that move. The number still originates with the deployer and is
     * still validated against nothing — it simply travels as a parameter rather than an attribute.
     */
    @Test
    public void theOperatorsOwnNumberIsTheCadence() throws Exception {
        assertEquals("one checkpoint per record", 10, checkpointsOverTenRecords(1));
        assertEquals("one checkpoint per five records", 2, checkpointsOverTenRecords(5));
    }

    /**
     * Runs ten one-byte records through a checkpointing stage and counts what reached the store.
     * Registers its own store rather than reusing {@link #withDurableState}, which discards appends.
     */
    private int checkpointsOverTenRecords(int frequency) throws Exception {
        java.util.concurrent.atomic.AtomicInteger appends = new java.util.concurrent.atomic.AtomicInteger();
        CheckpointStores.register((sequenceName, runId, stageName) -> new CheckpointStore() {
            @Override
            public Checkpoint lastComplete() {
                return null;
            }

            @Override
            public void append(Checkpoint checkpoint) {
                appends.incrementAndGet();
            }

            @Override
            public void clear() {
            }
        });
        try {
            Path root = Files.createTempDirectory("mft-cadence");
            StreamSequence p = sequence("cadence-" + frequency);
            p.setSourceProvided(true);
            p.setWorkspaceRoot(root.toString());
            p.addOperator(new Mocks.ByteRecords("rows", 0, 0, frequency), true);
            p.addOperator(new Mocks.Sink("sink"), true);
            p.validate();
            p.execute(null, jobWithId("job-cadence-" + frequency), seedOf("ABCDEFGHIJ"));
            return appends.get();
        } finally {
            CheckpointStores.register(null);
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

        StreamSequence p = sequence("resumed");
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
        StreamSequence p = sequence("ordinary");
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(new Mocks.PassThrough("pass"), true);
        p.addOperator(sink, true);
        p.validate();

        p.execute(null, JobContext.NOOP);

        assertArrayEquals(DATA, sink.bytes());
    }

    // ------------------------------------------------------------------ workspace layout

    /**
     * {@code <root>/<sequence>/<runId>/<stage>/}. The sequence level is not needed for uniqueness — a
     * run id is either a job id or a hash that already folds the sequence name in — it is there so the
     * workspace can be operated: retention per sequence, one sequence's leftovers cleared on their own,
     * and a directory's owner readable from its path.
     */
    @Test
    public void aStageWritesUnderSequenceThenRunThenStage() throws Exception {
        Path root = Files.createTempDirectory("mft-layout");
        Mocks.CapturingWorkspace mat = new Mocks.CapturingWorkspace("mat");

        StreamSequence p = sequence("csv-ingest");
        p.setWorkspaceRoot(root.toString());
        p.addOperator(new Mocks.Source("src", DATA), true);
        p.addOperator(mat, true);
        p.addOperator(new Mocks.Sink("sink"), true);
        p.validate();

        p.execute(null, jobWithId("job-7"));

        assertEquals(root.resolve("csv-ingest").resolve("job-7").resolve("mat"), mat.seen);
        assertNotNull("a materialising stage is handed an artifact", mat.seenArtifact);
        // Asked of the operator, not of the filesystem: a successful run reclaims its workspace, so
        // what is on disk afterwards says nothing about what the stage was handed.
        assertTrue("created before the operator ran", mat.seenDirectoryExisted);
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
        StreamSequence p = sequence("two-stages");
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

        StreamSequence p = sequence("two-stages-named");
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
     * one sequence must never see each other's positions.
     */
    @Test
    public void aCheckpointedStageIsHandedAStoreForItsOwnRunAndStage() throws Exception {
        List<String> asked = new ArrayList<>();
        CheckpointStores.register((sequenceName, runId, stageName) -> {
            asked.add(sequenceName + "/" + runId + "/" + stageName);
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
            StreamSequence p = sequence("ckpt-wired");
            p.setWorkspaceRoot(System.getProperty("java.io.tmpdir"));
            p.addOperator(new Mocks.Source("src", DATA), true);
            p.addOperator(new Mocks.Checkpointing("position"), true);
            p.addOperator(new Mocks.Sink("sink"), true);

            p.validate();
            p.execute(null, jobWithId("job-9"));

            // Asked more than once now -- the build asks, and reclaiming a finished run asks again to
            // clear the position it will never need. The key is what this pins.
            assertEquals(Set.of("ckpt-wired/job-9/position"), new java.util.HashSet<>(asked));
        } finally {
            CheckpointStores.register(null);
        }
    }

    /** Absent is a legitimate state, and it is refused at deployment rather than at first byte. */
    @Test
    public void withoutAProviderCheckpointingIsRefusedAtDeployment() {
        assertFalse("no provider should be registered by default", CheckpointStores.isAvailable());

        StreamSequence p = sequence("no-store");
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

}
