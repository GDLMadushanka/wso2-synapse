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

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link StageStream}: byte accounting, self-time subtraction and fault attribution.
 */
public class StageStreamTest {

    private static final byte[] DATA = "0123456789".getBytes(StandardCharsets.UTF_8);

    @Test
    public void countsBytesItProducedToItsConsumer() throws Exception {
        StageStream s = new StageStream(new ByteArrayInputStream(DATA), "stage", null);

        byte[] buf = new byte[4];
        assertEquals(4, s.read(buf, 0, 4));
        assertEquals(4, s.bytes());

        assertEquals('4', s.read());
        assertEquals(5, s.bytes());
    }

    @Test
    public void doesNotCountEndOfStreamAsAByte() throws Exception {
        StageStream s = new StageStream(new ByteArrayInputStream(new byte[0]), "stage", null);
        assertEquals(-1, s.read());
        assertEquals(0, s.bytes());
    }

    @Test
    public void bytesInIsWhatTheStageBelowProduced() throws Exception {
        StageStream lower = new StageStream(new ByteArrayInputStream(DATA), "lower", null);
        StageStream upper = new StageStream(lower, "upper", lower);

        upper.read(new byte[10], 0, 10);

        assertEquals("the head consumed nothing from below", 0L, lower.bytesIn());
        assertEquals(10L, lower.bytes());
        assertEquals("the upper stage consumed what the lower produced", 10L, upper.bytesIn());
    }

    @Test
    public void selfTimeSubtractsTheChainBelowSoItIsNotJustTheWholeTransfer() throws Exception {
        // A slow head with a fast wrapper over it: inclusive time is similar at both levels, but
        // self time should attribute almost all of it to the head.
        InputStream slow = new FilterInputStream(new ByteArrayInputStream(DATA)) {
            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                try {
                    Thread.sleep(40);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return in.read(b, off, len);
            }
        };

        StageStream lower = new StageStream(slow, "slow", null);
        StageStream upper = new StageStream(lower, "fast", lower);

        upper.read(new byte[10], 0, 10);

        assertTrue("inclusive time at the top includes the slow stage below",
                upper.totalNanos() >= lower.totalNanos());
        assertTrue("the slow stage should own nearly all of the self time",
                lower.selfNanos() > upper.selfNanos());
    }

    @Test
    public void selfTimeIsNeverNegative() throws Exception {
        StageStream lower = new StageStream(new ByteArrayInputStream(DATA), "lower", null);
        StageStream upper = new StageStream(lower, "upper", lower);

        upper.read(new byte[10], 0, 10);

        assertTrue(upper.selfNanos() >= 0L);
        assertTrue(lower.selfNanos() >= 0L);
    }

    @Test
    public void attributesAFailureToItsOwnStage() {
        StageStream s = new StageStream(failing(), "culprit", null);

        try {
            s.read();
            fail("expected the failure to surface");
        } catch (IOException e) {
            assertTrue(e instanceof StageStream.StageIOException);
            assertEquals("culprit", ((StageStream.StageIOException) e).getStage());
            assertNotNull("the original failure must be the cause", e.getCause());
        }
    }

    @Test
    public void doesNotReStampAnAlreadyAttributedFailure() {
        // Nested wrappers: the innermost stage is the correct attribution, and an outer wrapper
        // overwriting it would name the wrong stage.
        StageStream inner = new StageStream(failing(), "inner", null);
        StageStream outer = new StageStream(inner, "outer", inner);

        try {
            outer.read();
            fail("expected the failure to surface");
        } catch (IOException e) {
            assertEquals("inner", ((StageStream.StageIOException) e).getStage());
        }
    }

    @Test
    public void attributesBulkReadFailuresToo() {
        StageStream s = new StageStream(failing(), "culprit", null);

        try {
            s.read(new byte[4], 0, 4);
            fail("expected the failure to surface");
        } catch (IOException e) {
            assertEquals("culprit", ((StageStream.StageIOException) e).getStage());
        }
    }

    @Test
    public void timesSkipsWithoutCountingThemAsProducedBytes() throws Exception {
        StageStream s = new StageStream(new ByteArrayInputStream(DATA), "stage", null);

        assertEquals(4L, s.skip(4));
        assertEquals("skipped bytes were never produced to a consumer", 0L, s.bytes());
        assertTrue("but the time still has to land somewhere", s.totalNanos() >= 0L);
    }

    @Test
    public void rejectsANullStreamAndABlankName() {
        try {
            new StageStream(null, "stage", null);
            fail("expected a null stream to be rejected");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("null stream"));
        }
        try {
            new StageStream(new ByteArrayInputStream(DATA), "", null);
            fail("expected a blank stage name to be rejected");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("must have a name"));
        }
    }

    @Test
    public void exposesItsOwnStageName() {
        StageStream s = new StageStream(new ByteArrayInputStream(DATA), "gunzip", null);
        assertSame("gunzip", s.stage());
    }

    private static InputStream failing() {
        return new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("device on fire");
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                throw new IOException("device on fire");
            }
        };
    }
}
