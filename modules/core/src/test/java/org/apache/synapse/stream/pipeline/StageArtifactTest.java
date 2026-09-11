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
import org.apache.synapse.stream.CheckpointUnit;
import org.apache.synapse.stream.ResourceScope;
import org.apache.synapse.stream.StageArtifact;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
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
 * The facade that owns temp-write, fsync, rename, discard and the checkpoint ordering contract.
 *
 * <p>These tests carry the weight that used to sit in one connector operation's implementation. The
 * ordering test in particular is the one worth keeping honest: it asserts the artifact is already
 * durable <i>at the moment</i> the position is recorded, which is the property that both comparable
 * frameworks had to rewrite their resume machinery to obtain.
 */
public class StageArtifactTest {

    private static final String VERSION = "test-1";

    // ------------------------------------------------------------------ helpers

    /** Records every checkpoint, and what the artifact measured when each was written. */
    private static final class RecordingStore implements CheckpointStore {

        private final Path partial;
        final List<Checkpoint> written = new ArrayList<>();
        final List<Long> fileSizeAtWrite = new ArrayList<>();
        final AtomicInteger cleared = new AtomicInteger();
        Checkpoint existing;

        RecordingStore(Path partial) {
            this.partial = partial;
        }

        @Override
        public Checkpoint lastComplete() {
            return existing;
        }

        @Override
        public void append(Checkpoint checkpoint) throws IOException {
            written.add(checkpoint);
            fileSizeAtWrite.add(Files.exists(partial) ? Files.size(partial) : 0L);
            existing = checkpoint;
        }

        @Override
        public void clear() {
            cleared.incrementAndGet();
            existing = null;
        }
    }

    private static Path stageDir() throws IOException {
        return Files.createTempDirectory("mft-artifact");
    }

    private static Path partialOf(Path dir) {
        return dir.resolve("artifact.out" + DefaultStageArtifact.PARTIAL_SUFFIX);
    }

    private static Path finalOf(Path dir) {
        return dir.resolve("artifact.out");
    }

    private DefaultStageArtifact artifact(Path dir, CheckpointStore store, int maxReprocessed,
                                          ResourceScope scope, AtomicInteger resumeSignals) {
        return new DefaultStageArtifact("stage", dir, "artifact.out", store,
                store == null ? null : CheckpointUnit.RECORDS, store == null ? null : VERSION,
                maxReprocessed, scope, resumeSignals::incrementAndGet, true);
    }

    /** A named working file, which unlike the canonical artifact may be sealed mid-run. */
    private DefaultStageArtifact named(Path dir, String fileName, ResourceScope scope,
                                       AtomicInteger resumeSignals) {
        return new DefaultStageArtifact("stage", dir, fileName, null, null, null, 1, scope,
                resumeSignals::incrementAndGet, false);
    }

    private DefaultStageArtifact named(Path dir, String fileName, ResourceScope scope) {
        return new DefaultStageArtifact("stage", dir, fileName, null, null, null, 1, scope,
                () -> { }, false);
    }

    private static void append(StageArtifact artifact, String text) throws IOException {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        artifact.append(bytes, 0, bytes.length);
    }

    // ------------------------------------------------------------------ build phase

    /**
     * Invariant 1: obtaining an artifact touches nothing, so it is safe to do in {@code wrap()}.
     */
    @Test
    public void constructingTouchesTheFilesystem() throws Exception {
        Path dir = stageDir();
        ResourceScope scope = new ResourceScope();
        artifact(dir, null, 1, scope, new AtomicInteger());

        assertEquals("no file may be created while the chain is being built",
                0, Files.list(dir).count());
        assertEquals("and nothing registered, so there is nothing to unwind", 0, scope.size());
    }

    /** A stage that keeps a position but writes no bytes must not leave a file behind. */
    @Test
    public void aCheckpointOnlyStageCreatesNoFile() throws Exception {
        Path dir = stageDir();
        RecordingStore store = new RecordingStore(partialOf(dir));
        ResourceScope scope = new ResourceScope();
        StageArtifact artifact = artifact(dir, store, 1, scope, new AtomicInteger());

        artifact.unitDone(1);
        artifact.unitDone(2);
        scope.close();

        assertEquals(2, store.written.size());
        assertEquals("a position with no artifact records a zero length",
                0L, store.written.get(0).outputLength());
        assertEquals("nothing is written, so nothing is created", 0, Files.list(dir).count());
    }

    // ------------------------------------------------------------------ publishing

    /** A successful run publishes by rename, so presence under the final name means complete. */
    @Test
    public void aSuccessfulRunRenamesTheArtifactIntoPlace() throws Exception {
        Path dir = stageDir();
        ResourceScope scope = new ResourceScope();
        StageArtifact artifact = artifact(dir, null, 1, scope, new AtomicInteger());

        append(artifact, "hello world");
        assertFalse("nothing visible under the final name while writing",
                Files.exists(finalOf(dir)));

        scope.close();

        assertEquals("hello world", Files.readString(finalOf(dir)));
        assertFalse("the partial is gone", Files.exists(partialOf(dir)));
    }

    /**
     * Invariant 5, and the reason the facade owns publishing rather than the operator.
     *
     * <p>A partial artifact renamed into place is read by the next attempt as a finished segment, so
     * the failure surfaces nowhere near its cause. There is no method an operator could call to cause
     * this — the run's outcome decides, through the scope.
     */
    @Test
    public void aFailedRunPublishesNothing() throws Exception {
        Path dir = stageDir();
        ResourceScope scope = new ResourceScope();
        StageArtifact artifact = artifact(dir, null, 1, scope, new AtomicInteger());

        append(artifact, "half a file");
        scope.abort();

        assertFalse("no artifact under the final name", Files.exists(finalOf(dir)));
        assertFalse("and the partial is discarded too", Files.exists(partialOf(dir)));
    }

    // ------------------------------------------------------------------ the ordering contract

    /**
     * The whole correctness argument: when the position is recorded, the bytes it describes are
     * already durable.
     *
     * <p>Asserted by measuring the artifact from inside the store's {@code append}, which is the only
     * vantage point where "was the fsync before the position moved" is observable at all. A checkpoint
     * ahead of its data is silent data loss; this is what rules it out.
     */
    @Test
    public void theArtifactIsDurableBeforeThePositionMoves() throws Exception {
        Path dir = stageDir();
        RecordingStore store = new RecordingStore(partialOf(dir));
        ResourceScope scope = new ResourceScope();
        StageArtifact artifact = artifact(dir, store, 1, scope, new AtomicInteger());

        for (int record = 1; record <= 5; record++) {
            append(artifact, "record-" + record + "\n");
            artifact.unitDone(record);
        }
        scope.close();

        assertEquals(5, store.written.size());
        for (int i = 0; i < store.written.size(); i++) {
            long recorded = store.written.get(i).outputLength();
            assertTrue("checkpoint " + i + " recorded " + recorded + " bytes but only "
                            + store.fileSizeAtWrite.get(i) + " were on disk when it was written",
                    store.fileSizeAtWrite.get(i) >= recorded);
        }
        assertEquals("the position is the operator's unit, not a byte count",
                5L, store.written.get(4).position());
    }

    /** Cadence is the deployer's bound, and it is the only lever on per-unit durability cost. */
    @Test
    public void maxReprocessedDecidesHowOftenThePositionMoves() throws Exception {
        Path dir = stageDir();
        RecordingStore store = new RecordingStore(partialOf(dir));
        ResourceScope scope = new ResourceScope();
        StageArtifact artifact = artifact(dir, store, 1000, scope, new AtomicInteger());

        for (int record = 1; record <= 2500; record++) {
            append(artifact, "x");
            artifact.unitDone(record);
        }
        scope.close();

        assertEquals("2500 records at a bound of 1000 is two checkpoints", 2, store.written.size());
        assertEquals(1000L, store.written.get(0).position());
        assertEquals(2000L, store.written.get(1).position());
    }

    /**
     * A coarse reporter is not punished with an fsync per byte.
     *
     * <p>Counting a bound of 1 in bytes and calling once per byte was the pathology in the first
     * implementation of this. Because cadence is measured on the reported position rather than on the
     * number of calls, an operator that reports once per block checkpoints once per block — as close
     * to the bound as its own granularity allows, and no closer.
     */
    @Test
    public void reportingCoarselyCheckpointsPerReportNotPerUnit() throws Exception {
        Path dir = stageDir();
        RecordingStore store = new RecordingStore(partialOf(dir));
        ResourceScope scope = new ResourceScope();
        StageArtifact artifact = artifact(dir, store, 1, scope, new AtomicInteger());

        append(artifact, "a 64k block, notionally");
        artifact.unitDone(65536);
        append(artifact, "and another");
        artifact.unitDone(131072);
        scope.close();

        assertEquals("one checkpoint per report, not 131072 of them", 2, store.written.size());
    }

    // ------------------------------------------------------------------ resume

    /** Resume truncates the artifact to the recorded length and hands back the input position. */
    @Test
    public void resumeTruncatesToTheRecordedLengthAndReportsThePosition() throws Exception {
        Path dir = stageDir();
        RecordingStore store = new RecordingStore(partialOf(dir));

        // First attempt: three records recorded, then a fourth appended and never checkpointed.
        ResourceScope first = new ResourceScope();
        StageArtifact attempt1 = artifact(dir, store, 1, first, new AtomicInteger());
        append(attempt1, "aaa");
        attempt1.unitDone(1);
        append(attempt1, "bbb");
        attempt1.unitDone(2);
        append(attempt1, "ccc");
        attempt1.unitDone(3);
        append(attempt1, "DANGLING");
        first.abort();

        // abort() discarded the partial, so put it back as an unclean death would have left it:
        // everything written, including the bytes past the last checkpoint.
        Files.writeString(partialOf(dir), "aaabbbcccDANGLING");

        ResourceScope second = new ResourceScope();
        StageArtifact attempt2 = artifact(dir, store, 1, second, new AtomicInteger());

        assertEquals("resume at the recorded input position", 3L, attempt2.resumePosition());
        assertEquals("and the uncheckpointed tail is cut", 9L, Files.size(partialOf(dir)));

        append(attempt2, "ddd");
        attempt2.unitDone(4);
        second.close();

        assertEquals("aaabbbcccddd", Files.readString(finalOf(dir)));
    }

    /**
     * {@code L > A}: a position describing bytes that are not there is discarded, not resumed on.
     *
     * <p>Should be unreachable given the mandatory fsync, so this is a net under storage that
     * acknowledges writes it has not committed.
     */
    @Test
    public void aCheckpointAheadOfTheArtifactIsDiscarded() throws Exception {
        Path dir = stageDir();
        RecordingStore store = new RecordingStore(partialOf(dir));
        store.existing = new Checkpoint(9L, 900L, CheckpointUnit.RECORDS, VERSION);
        Files.writeString(partialOf(dir), "only twelve");

        ResourceScope scope = new ResourceScope();
        StageArtifact artifact = artifact(dir, store, 1, scope, new AtomicInteger());

        assertEquals("a checkpoint ahead of the data restarts the segment",
                0L, artifact.resumePosition());
        assertFalse("and the untrustworthy partial is removed", Files.exists(partialOf(dir)));
    }

    /** A complete artifact short-circuits the stage, and the pipeline is told without being asked. */
    @Test
    public void openCompleteSignalsResumedFromArtifact() throws Exception {
        Path dir = stageDir();
        Files.writeString(finalOf(dir), "already done");
        AtomicInteger signals = new AtomicInteger();
        ResourceScope scope = new ResourceScope();
        StageArtifact artifact = artifact(dir, null, 1, scope, signals);

        assertTrue(artifact.isComplete());
        try (InputStream in = artifact.openComplete()) {
            assertEquals("already done", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
        assertEquals("the operator must not have to remember to signal this", 1, signals.get());
        assertEquals("and the stream is the scope's to close", 1, scope.size());
        scope.close();
    }

    @Test
    public void openCompleteRefusesWhenThereIsNoArtifact() throws Exception {
        Path dir = stageDir();
        StageArtifact artifact = artifact(dir, null, 1, new ResourceScope(), new AtomicInteger());
        assertFalse(artifact.isComplete());
        try {
            artifact.openComplete();
            fail("expected a refusal");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("no completed artifact"));
        }
    }

    // ------------------------------------------------------------------ misuse

    /** Checkpointing without saying what the position counts is refused, not defaulted. */
    @Test
    public void unitDoneWithoutAUnitIsRefused() throws Exception {
        Path dir = stageDir();
        StageArtifact artifact = artifact(dir, null, 1, new ResourceScope(), new AtomicInteger());
        try {
            artifact.unitDone(1);
            fail("expected a refusal");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage(),
                    expected.getMessage().contains("without a checkpoint unit"));
        }
    }

    /** A position that moves backwards is a bug in the operator, and it is not silently accepted. */
    @Test
    public void aPositionMayNotMoveBackwards() throws Exception {
        Path dir = stageDir();
        RecordingStore store = new RecordingStore(partialOf(dir));
        StageArtifact artifact = artifact(dir, store, 1, new ResourceScope(), new AtomicInteger());

        artifact.unitDone(10);
        try {
            artifact.unitDone(9);
            fail("expected a refusal");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("must not move backwards"));
        }
    }

    /** Nothing was written, so a checkpoint that was never taken must read as absent. */
    @Test
    public void anAbsentCheckpointIsZeroAndNotAGuess() throws Exception {
        Path dir = stageDir();
        RecordingStore store = new RecordingStore(partialOf(dir));
        StageArtifact artifact = artifact(dir, store, 1, new ResourceScope(), new AtomicInteger());

        assertNull(store.lastComplete());
        assertEquals(0L, artifact.resumePosition());
    }

    /**
     * A stage that keeps a position and writes nothing needs no workspace at all.
     *
     * <p>{@code validate()} demands a workspace only for {@code materialises()}, so a checkpointed
     * sink — the remote-authoritative shape — must work on a Synapse with none configured. Appending
     * without one is what fails, and it says which of the two declarations is missing.
     */
    @Test
    public void aCheckpointOnlyStageNeedsNoWorkspace() throws Exception {
        RecordingStore store = new RecordingStore(Path.of("/nonexistent"));
        StageArtifact artifact = new DefaultStageArtifact("upload", null, "artifact.out", store,
                CheckpointUnit.BYTES, VERSION, 1, new ResourceScope(), () -> { }, true);

        assertFalse("nothing can be complete without a workspace", artifact.isComplete());
        assertEquals(0L, artifact.resumePosition());
        artifact.unitDone(4096);
        assertEquals(1, store.written.size());
        assertEquals(0L, store.written.get(0).outputLength());

        try {
            artifact.append(new byte[] {1}, 0, 1);
            fail("expected appending without a workspace to be refused");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("materialises()"));
        }
    }


    // ------------------------------------------------------------------ seal

    /**
     * The case that makes an external merge sort possible: phase 2 must read back what phase 1 wrote,
     * within the same run. Without seal(), every run file stays a {@code .part} until the run ends.
     */
    @Test
    public void aSealedWorkingFileIsReadableWithinTheSameRun() throws Exception {
        Path dir = stageDir();
        ResourceScope scope = new ResourceScope();
        StageArtifact run0 = named(dir, "run-0", scope);

        append(run0, "sorted chunk");
        assertFalse("not complete while still being written", run0.isComplete());

        run0.seal();

        assertTrue("sealed means complete, now", run0.isComplete());
        try (InputStream in = run0.openComplete()) {
            assertEquals("sorted chunk", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
        scope.close();
    }

    /**
     * A sealed working file survives a failed run — deliberately, and unlike the canonical artifact.
     *
     * <p>That is how phase 1 resumes without re-splitting the input, and it does not weaken invariant
     * 5: nothing downstream ever reads a named working file as the stage's output.
     */
    @Test
    public void aSealedWorkingFileSurvivesAFailedRun() throws Exception {
        Path dir = stageDir();
        ResourceScope scope = new ResourceScope();

        StageArtifact run0 = named(dir, "run-0", scope);
        append(run0, "complete run");
        run0.seal();

        StageArtifact run1 = named(dir, "run-1", scope);
        append(run1, "half a run");            // never sealed

        scope.abort();

        assertEquals("the sealed run is what the next attempt resumes from",
                "complete run", Files.readString(dir.resolve("run-0")));
        assertFalse("the unsealed one is discarded", Files.exists(dir.resolve("run-1")));
        assertFalse(Files.exists(dir.resolve("run-1" + DefaultStageArtifact.PARTIAL_SUFFIX)));
    }

    /** Publishing the stage's own output is the run's decision, never the operator's. */
    @Test
    public void sealingTheCanonicalArtifactIsRefused() throws Exception {
        Path dir = stageDir();
        StageArtifact artifact = artifact(dir, null, 1, new ResourceScope(), new AtomicInteger());
        append(artifact, "partial output");
        try {
            artifact.seal();
            fail("expected sealing the canonical artifact to be refused");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("canonical"));
        }
        assertFalse("and nothing was published", Files.exists(finalOf(dir)));
    }

    /** A sealed file may already have been read back, so changing it afterwards is a bug. */
    @Test
    public void appendingAfterSealIsRefused() throws Exception {
        Path dir = stageDir();
        StageArtifact run0 = named(dir, "run-0", new ResourceScope());
        append(run0, "done");
        run0.seal();
        try {
            append(run0, "more");
            fail("expected an append after seal to be refused");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("after sealing"));
        }
    }

    /** Sealing twice is a no-op, so a retry of the surrounding logic is not a failure. */
    @Test
    public void sealIsIdempotent() throws Exception {
        Path dir = stageDir();
        StageArtifact run0 = named(dir, "run-0", new ResourceScope());
        append(run0, "once");
        run0.seal();
        run0.seal();
        assertEquals("once", Files.readString(dir.resolve("run-0")));
    }

    /** Zero runs matched is a legitimate answer, and a reader must be able to tell it from absent. */
    @Test
    public void sealingWithNothingAppendedCreatesAnEmptyFile() throws Exception {
        Path dir = stageDir();
        ResourceScope scope = new ResourceScope();
        StageArtifact run0 = named(dir, "run-0", scope);
        run0.seal();
        assertTrue(run0.isComplete());
        assertEquals(0L, Files.size(dir.resolve("run-0")));
        scope.close();
    }


    // ------------------------------------------------------------------ payload

    /**
     * One integer cannot express a state machine, which is what the payload is for.
     *
     * <p>An external merge sort killed during the split must resume reading input; killed during the
     * merge it must not read the input at all. Both can present as the same position with a zero
     * output length, and guessing "merge" when it was splitting drops every row not yet written.
     */
    @Test
    public void thePayloadRoundTripsAndDiscriminatesThePhase() throws Exception {
        Path dir = stageDir();
        RecordingStore store = new RecordingStore(partialOf(dir));

        ResourceScope first = new ResourceScope();
        StageArtifact split = artifact(dir, store, 1, first, new AtomicInteger());
        split.unitDone(354_000_000L, "{\"phase\":\"split\",\"runsSealed\":7}");
        first.abort();

        ResourceScope second = new ResourceScope();
        StageArtifact resumed = artifact(dir, store, 1, second, new AtomicInteger());

        assertEquals(354_000_000L, resumed.resumePosition());
        assertEquals("{\"phase\":\"split\",\"runsSealed\":7}", resumed.resumePayload());
        second.close();
    }

    /** No checkpoint means no payload, and that is how a fresh run is told from a resumed one. */
    @Test
    public void anAbsentPayloadIsNullNotEmpty() throws Exception {
        Path dir = stageDir();
        RecordingStore store = new RecordingStore(partialOf(dir));
        StageArtifact artifact = artifact(dir, store, 1, new ResourceScope(), new AtomicInteger());
        assertNull("null means no checkpoint, which is the phase discriminator", artifact.resumePayload());
    }

    /**
     * Over the cap is refused, never trimmed.
     *
     * <p>Spring Batch truncates its short context when it overflows, which can write invalid JSON
     * that fails on a later read. A refusal is a bug report; a trim is a corrupt checkpoint.
     */
    @Test
    public void anOverLongPayloadIsRefusedNotTruncated() throws Exception {
        Path dir = stageDir();
        RecordingStore store = new RecordingStore(partialOf(dir));
        StageArtifact artifact = artifact(dir, store, 1, new ResourceScope(), new AtomicInteger());

        String tooBig = "x".repeat(Checkpoint.MAX_PAYLOAD_BYTES + 1);
        try {
            artifact.unitDone(1, tooBig);
            fail("expected an over-long payload to be refused");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("refused rather than"));
        }
        assertTrue("and nothing was recorded", store.written.isEmpty());
    }

    /** The cap is bytes of UTF-8, not characters — the other half of the same trap. */
    @Test
    public void thePayloadCapCountsUtf8BytesNotCharacters() {
        String threeByteChars = "\u4e2d".repeat(Checkpoint.MAX_PAYLOAD_BYTES / 3 + 1);
        assertTrue("well under the cap in characters",
                threeByteChars.length() < Checkpoint.MAX_PAYLOAD_BYTES);
        try {
            new Checkpoint(1, 0, CheckpointUnit.RECORDS, VERSION, threeByteChars);
            fail("expected a multi-byte payload over the byte cap to be refused");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("bytes of UTF-8"));
        }
    }

    // ------------------------------------------------------------------ length

    /** Exposed so an operator reporting progress does not have to count separately. */
    @Test
    public void lengthReportsWhatHasBeenAppendedIncludingAResumedPrefix() throws Exception {
        Path dir = stageDir();
        RecordingStore store = new RecordingStore(partialOf(dir));

        ResourceScope first = new ResourceScope();
        StageArtifact a1 = artifact(dir, store, 1, first, new AtomicInteger());
        assertEquals(0L, a1.length());
        append(a1, "12345");
        a1.unitDone(1);
        assertEquals(5L, a1.length());
        first.abort();

        Files.writeString(partialOf(dir), "12345");
        ResourceScope second = new ResourceScope();
        StageArtifact a2 = artifact(dir, store, 1, second, new AtomicInteger());
        assertEquals("a resumed prefix counts", 5L, a2.length());
        append(a2, "678");
        assertEquals(8L, a2.length());
        second.close();
    }


    // ------------------------------------------------------- canonical vs named

    /**
     * Reading back a sealed working file must <b>not</b> report that the stage short-circuited.
     *
     * <p>An external merge sort opens its sealed runs while phase 1 is still consuming the input. If
     * that counted as a short-circuit the pipeline would release the upstream phase 1 is reading
     * from — the stage's own source, pulled out from under it mid-run.
     */
    @Test
    public void openingANamedFileDoesNotReportAShortCircuit() throws Exception {
        Path dir = stageDir();
        AtomicInteger signals = new AtomicInteger();
        ResourceScope scope = new ResourceScope();

        StageArtifact run0 = named(dir, "run-0", scope, signals);
        append(run0, "sorted chunk");
        run0.seal();
        try (InputStream in = run0.openComplete()) {
            assertEquals("sorted chunk", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }

        assertEquals("a named file says nothing about the stage", 0, signals.get());
        scope.close();
    }

    /** The canonical artifact does report it — that is what releases the orphaned upstream. */
    @Test
    public void openingTheCanonicalArtifactReportsAShortCircuit() throws Exception {
        Path dir = stageDir();
        Files.writeString(finalOf(dir), "from a previous run");
        AtomicInteger signals = new AtomicInteger();
        ResourceScope scope = new ResourceScope();

        StageArtifact artifact = artifact(dir, null, 1, scope, signals);
        try (InputStream in = artifact.openComplete()) {
            in.readAllBytes();
        }

        assertEquals(1, signals.get());
        scope.close();
    }

    /**
     * A stage holds several artifacts at once and each commits to its own file.
     *
     * <p>Note what this does <b>not</b> pin. These used to register under one scope name,
     * {@code stage:artifact}, which made a merge sort's run files indistinguishable in the scope and
     * in every log line about them. That is now {@code stage:run-0} and so on — but the scope neither
     * deduplicates by name nor exposes the names, so the fix shows up only in a log line and no
     * assertion here can reach it. What this test does cover is that the artifacts stay separate.
     */
    @Test
    public void aStageCanHoldSeveralArtifactsAtOnce() throws Exception {
        Path dir = stageDir();
        ResourceScope scope = new ResourceScope();

        append(named(dir, "run-0", scope), "a");
        append(named(dir, "run-1", scope), "b");
        append(artifact(dir, null, 1, scope, new AtomicInteger()), "c");

        assertEquals(3, scope.size());
        scope.close();

        assertEquals("a", Files.readString(dir.resolve("run-0")));
        assertEquals("b", Files.readString(dir.resolve("run-1")));
        assertEquals("c", Files.readString(finalOf(dir)));
    }

    /** Two callers must not each hold their own append offset over one file. */
    @Test
    public void theContextHandsOutOneArtifactPerName() {
        DefaultStreamContext ctx = new DefaultStreamContext(null, null, new ResourceScope(), "stage",
                true, false, Path.of(System.getProperty("java.io.tmpdir")), null, 1, null);

        StageArtifact first = ctx.artifact();
        assertNotNull(first);
        assertTrue("the same artifact, not a second writer over one file", first == ctx.artifact());
    }
}
