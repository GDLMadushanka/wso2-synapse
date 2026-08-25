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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link StreamException}.
 * <p>
 * The copy semantics of {@link StreamException#withStage(String)} exist so that a stage identity
 * assigned by an inner frame cannot be overwritten by an outer frame's guess. That is easy to
 * regress into a mutating setter, so it is pinned here.
 */
public class StreamExceptionTest {

    @Test
    public void isNotRetryableByDefaultBecauseTheAsymmetryFavoursCaution() {
        assertFalse(new StreamException("bad key").isRetryable());
        assertFalse(new StreamException("bad key", new RuntimeException()).isRetryable());
    }

    @Test
    public void carriesAnExplicitRetryabilityVerdictWhenGivenOne() {
        assertTrue(new StreamException("connection reset", true).isRetryable());
        assertTrue(new StreamException("timeout", new RuntimeException(), true).isRetryable());
    }

    @Test
    public void isUnattributedUntilTheFrameworkStampsAStage() {
        assertNull("an operator does not name its own stage", new StreamException("boom").getStage());
    }

    @Test
    public void withStageReturnsACopyRatherThanMutating() {
        StreamException original = new StreamException("boom");
        StreamException stamped = original.withStage("gunzip");

        assertNotSame(original, stamped);
        assertNull("the original must be untouched", original.getStage());
        assertEquals("gunzip", stamped.getStage());
    }

    @Test
    public void withStagePreservesMessageCauseAndRetryability() {
        RuntimeException cause = new RuntimeException("underlying");
        StreamException stamped = new StreamException("timed out", cause, true).withStage("sftpSource");

        assertEquals("timed out", stamped.getMessage());
        assertSame(cause, stamped.getCause());
        assertTrue("retryability must survive attribution", stamped.isRetryable());
        assertEquals("sftpSource", stamped.getStage());
    }

    @Test
    public void anAlreadyAttributedExceptionKeepsTheInnermostStage() {
        // The StageStream nearest the failure attributes it; outer wrappers must not re-stamp,
        // or the reported stage is the wrong one.
        StreamException inner = new StreamException("read failed").withStage("pgpDecrypt");
        StreamException outer = inner.withStage("gunzip");

        assertSame("re-stamping must be a no-op returning this", inner, outer);
        assertEquals("pgpDecrypt", outer.getStage());
    }
}
