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

package org.apache.synapse.stream;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the small value types and the defaulted contracts: {@link Checkpoint},
 * {@link StreamSeed}, {@link JobContext} and {@link StreamOperator}'s flags.
 * <p>
 * These mostly pin defaults. The defaults matter because in every case the conservative answer is
 * also the zero value, which is what makes an operator that declares nothing safe rather than
 * silently wrong.
 */
public class StreamContractsTest {

    // ---------- StreamOperator flags ----------

    /** Declares nothing beyond a name — the shape of a simple pure decorator. */
    private static final StreamOperator MINIMAL = () -> "minimal";

    @Test
    public void allBehaviourFlagsDefaultToTheConservativeAnswer() {
        assertFalse("a wrong 'deterministic' corrupts silently, so it must be opt-in",
                MINIMAL.deterministic());
        assertFalse(MINIMAL.checkpointed());
        assertFalse(MINIMAL.materialises());
        assertEquals("minimal", MINIMAL.name());
    }

    // ---------- Checkpoint ----------

    @Test
    public void checkpointCarriesPositionOutputLengthUnitAndVersion() {
        Checkpoint cp = new Checkpoint(50L, 4096L, CheckpointUnit.RECORDS, "v1");

        assertEquals(50L, cp.position());
        assertEquals(4096L, cp.outputLength());
        assertEquals(CheckpointUnit.RECORDS, cp.unit());
        assertEquals("v1", cp.formatVersion());
    }

    @Test
    public void checkpointRejectsAMissingUnitBecauseAPositionAloneIsAmbiguous() {
        assertRejected(() -> new Checkpoint(1L, 0L, null, "v1"), "unit");
    }

    @Test
    public void checkpointRejectsAMissingFormatVersion() {
        assertRejected(() -> new Checkpoint(1L, 0L, CheckpointUnit.BYTES, null), "formatVersion");
        assertRejected(() -> new Checkpoint(1L, 0L, CheckpointUnit.BYTES, "  "), "formatVersion");
    }

    @Test
    public void checkpointRejectsNegativeCounters() {
        assertRejected(() -> new Checkpoint(-1L, 0L, CheckpointUnit.BYTES, "v1"), "position");
        assertRejected(() -> new Checkpoint(0L, -1L, CheckpointUnit.BYTES, "v1"), "outputLength");
    }

    // ---------- StreamSeed ----------

    @Test
    public void seedCarriesTheStreamAndTheIdentityNeededToVerifyItLater() {
        InputStream in = new ByteArrayInputStream(new byte[0]);
        StreamSeed seed = new StreamSeed(in, "sftp://host/in/data.csv", 1024L, 1700000000000L,
                StreamOrigin.REOPENABLE);

        assertEquals(in, seed.stream());
        assertEquals("sftp://host/in/data.csv", seed.identity());
        assertTrue(seed.hasIdentity());
        assertTrue(seed.hasSize());
        assertTrue(seed.hasLastModified());
    }

    /**
     * A provider with nothing to recognise its bytes by says so, and the run becomes scratch.
     *
     * <p>The identity used to be refused as blank-or-null, which forced every provider to invent one.
     * An HTTP multipart body has none to invent, and a made-up identity is the dangerous answer: it
     * collides with the next made-up one and two unrelated runs share a workspace.
     */
    @Test
    public void aSeedMayHaveNoIdentityAtAll() {
        StreamSeed anonymous = new StreamSeed(new ByteArrayInputStream(new byte[0]), null,
                StreamSeed.UNKNOWN, StreamSeed.UNKNOWN, StreamOrigin.ONE_SHOT);
        assertFalse(anonymous.hasIdentity());
        assertNull(anonymous.identity());
    }

    /** Blank is the same as absent: an empty identity carries no more information than none. */
    @Test
    public void aBlankIdentityNormalisesToAbsent() {
        for (String blank : new String[] {"", "   ", "\t\n"}) {
            StreamSeed seed = new StreamSeed(new ByteArrayInputStream(new byte[0]), blank,
                    StreamSeed.UNKNOWN, StreamSeed.UNKNOWN, StreamOrigin.REOPENABLE);
            assertFalse("'" + blank + "' must not name a run", seed.hasIdentity());
            assertNull(seed.identity());
        }
    }

    /**
     * An identity too long to record is refused, not truncated.
     *
     * <p>Truncating would leave something that still looks like an identity, so two sources agreeing
     * for the first kilobyte would silently become one run.
     */
    @Test
    public void anIdentityTooLongToRecordIsRefused() {
        String tooLong = "x".repeat(StreamSeed.MAX_IDENTITY_CHARS + 1);
        assertRejected(() -> new StreamSeed(new ByteArrayInputStream(new byte[0]), tooLong,
                StreamSeed.UNKNOWN, StreamSeed.UNKNOWN, StreamOrigin.REOPENABLE), "hash it down");

        String atLimit = "x".repeat(StreamSeed.MAX_IDENTITY_CHARS);
        assertEquals(atLimit, new StreamSeed(new ByteArrayInputStream(new byte[0]), atLimit,
                StreamSeed.UNKNOWN, StreamSeed.UNKNOWN, StreamOrigin.REOPENABLE).identity());
    }

    @Test
    public void seedTreatsSizeAndModificationTimeAsBestEffort() {
        StreamSeed seed = new StreamSeed(new ByteArrayInputStream(new byte[0]), "id",
                StreamSeed.UNKNOWN, StreamSeed.UNKNOWN, StreamOrigin.REOPENABLE);

        assertFalse(seed.hasSize());
        assertFalse(seed.hasLastModified());
    }

    @Test
    public void seedRequiresAStreamAndNothingElse() {
        assertRejected(() -> new StreamSeed(null, "id", 1L, 1L, StreamOrigin.REOPENABLE), "stream");
        assertRejected(() -> new StreamSeed(new ByteArrayInputStream(new byte[0]), "id", 1L, 1L, null),
                "origin");
        // The identity is deliberately NOT required -- see aSeedMayHaveNoIdentityAtAll.
    }

    // ---------- JobContext ----------

    @Test
    public void noopIsACompleteImplementationSoOperatorsNeverNullCheckIt() {
        JobContext job = JobContext.NOOP;

        // Every reporting method must be safe to call and must not throw.
        job.sourceOpened(1L, 1L);
        job.progress("stage", 1L, 0L);
        job.stageFinished("stage", 1L, 1L, 1L);
        job.checksum("stage", "SHA-256", "abc");
        job.failed("stage", "CODE", "message");

        assertEquals("noop", job.jobId());
        assertFalse(job.isCancelled());
        assertNull("a null expectedSize is the normal case, not an error", job.expectedSize());
    }


    // ---------- what a source says it is about to read ----------

    /**
     * A blank identity is no identity. Two sources that both said "" would compare equal, which is the
     * one thing an identity may never do.
     */
    @Test
    public void aBlankSourceIdentityIsNoIdentity() {
        assertFalse(new SourceIdentity("   ", 1L, 1L).hasIdentity());
        assertNull(new SourceIdentity("", 1L, 1L).identity());
        assertTrue(new SourceIdentity("conn1|in/a.csv|12|9", 12L, 9L).hasIdentity());
    }

    /**
     * Refused rather than truncated, for the same reason a seed's is: a truncated identity compares
     * equal to a different source, which is a silent wrong resume rather than a loud failure.
     */
    @Test
    public void anOverlongIdentityIsRefused() {
        String tooLong = "x".repeat(StreamSeed.MAX_IDENTITY_CHARS + 1);

        assertRejected(() -> new SourceIdentity(tooLong, 1L, 1L), "hash it down");
    }

    /** Measurements are optional; a source that can name a resource but not size it is normal. */
    @Test
    public void unknownMeasurementsAreNormalised() {
        SourceIdentity named = SourceIdentity.of("conn1|in/a.csv");

        assertEquals(StreamSeed.UNKNOWN, named.size());
        assertEquals(StreamSeed.UNKNOWN, named.lastModified());
        assertTrue(named.hasIdentity());
        assertEquals(StreamSeed.UNKNOWN, new SourceIdentity("x", -5L, -1L).size());
    }

    /** Five states, and the two questions the framework actually asks of them. */
    @Test
    public void transferStatusesKnowWhichAreTerminalAndWhichOccupyTheQueue() {
        assertTrue(TransferStatus.QUEUED.isInFlight());
        assertTrue(TransferStatus.RUNNING.isInFlight());
        assertFalse(TransferStatus.DONE.isInFlight());

        assertTrue(TransferStatus.DONE.isTerminal());
        assertTrue(TransferStatus.FAILED.isTerminal());
        assertTrue(TransferStatus.CANCELLED.isTerminal());
        assertFalse(TransferStatus.QUEUED.isTerminal());
    }

    /**
     * The run's name is derived from the job id and nothing else, which is what lets a second attempt
     * land in the workspace the first one left.
     */
    @Test
    public void aTransfersRunNameIsDerivedFromItsJobId() {
        Transfer transfer = new Transfer(1423L, "mft-queue", Map.of("path", "in/a.csv"), "QUEUED", 0,
                0L, null, null, null, null, 0L, 0L, 0L);

        assertEquals("mft-job-1423", transfer.runName());
        assertEquals("in/a.csv", transfer.parameter("path"));
    }

    /**
     * The parameters outlive the caller that supplied them — they are written to a row and read back by
     * another node — so a transfer must not share a map anyone else can still change.
     */
    @Test
    public void aTransfersParametersAreItsOwnAndUnmodifiable() {
        Map<String, String> supplied = new HashMap<>();
        supplied.put("path", "in/a.csv");
        Transfer transfer = new Transfer(1L, "mft-queue", supplied, "QUEUED", 0, 0L, null, null, null,
                null, 0L, 0L, 0L);

        supplied.put("path", "in/b.csv");
        assertEquals("a later edit by the caller must not change the transfer",
                "in/a.csv", transfer.parameter("path"));

        try {
            transfer.parameters().put("connection", "conn2");
            fail("a transfer's parameters must not be modifiable");
        } catch (UnsupportedOperationException expected) {
            // the point
        }
    }

    // ---------- helper ----------

    private static void assertRejected(Runnable construction, String expectedInMessage) {
        try {
            construction.run();
            fail("expected construction to be rejected for: " + expectedInMessage);
        } catch (IllegalArgumentException expected) {
            assertTrue("message should mention '" + expectedInMessage + "' but was: "
                            + expected.getMessage(),
                    expected.getMessage().contains(expectedInMessage));
        }
    }
}
