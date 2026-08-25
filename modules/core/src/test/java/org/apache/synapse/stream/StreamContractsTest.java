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
        assertEquals("sftp://host/in/data.csv", seed.sourceId());
        assertTrue(seed.hasSize());
        assertTrue(seed.hasLastModified());
    }

    @Test
    public void seedTreatsSizeAndModificationTimeAsBestEffort() {
        StreamSeed seed = new StreamSeed(new ByteArrayInputStream(new byte[0]), "id",
                StreamSeed.UNKNOWN, StreamSeed.UNKNOWN, StreamOrigin.REOPENABLE);

        assertFalse(seed.hasSize());
        assertFalse(seed.hasLastModified());
    }

    @Test
    public void seedRequiresAStreamAndASourceId() {
        assertRejected(() -> new StreamSeed(null, "id", 1L, 1L, StreamOrigin.REOPENABLE), "stream");
        assertRejected(() -> new StreamSeed(new ByteArrayInputStream(new byte[0]), null, 1L, 1L, StreamOrigin.REOPENABLE),
                "sourceId");
        assertRejected(() -> new StreamSeed(new ByteArrayInputStream(new byte[0]), " ", 1L, 1L, StreamOrigin.REOPENABLE),
                "sourceId");
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
