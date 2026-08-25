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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link ResourceScope}.
 * <p>
 * The ordering and error-handling rules here are not compiler-enforced and are the sort of thing
 * that quietly regresses, so they are asserted rather than assumed.
 */
public class ResourceScopeTest {

    /** Records the order in which resources were closed or aborted, and can fail on close. */
    private static final class Recorder implements org.apache.synapse.stream.StreamCommit {

        private final String name;
        private final List<String> log;
        private final boolean failOnClose;
        private int closeCount;
        private int abortCount;

        Recorder(String name, List<String> log) {
            this(name, log, false);
        }

        Recorder(String name, List<String> log, boolean failOnClose) {
            this.name = name;
            this.log = log;
            this.failOnClose = failOnClose;
        }

        @Override
        public void close() throws IOException {
            closeCount++;
            log.add(name);
            if (failOnClose) {
                throw new IOException("close failed: " + name);
            }
        }

        @Override
        public void abort() throws IOException {
            abortCount++;
            log.add("abort:" + name);
            if (failOnClose) {
                throw new IOException("abort failed: " + name);
            }
        }
    }

    @Test
    public void registerReturnsItsArgumentSoRegistrationIsNotASeparateStatement() {
        ResourceScope scope = new ResourceScope();
        Recorder plain = new Recorder("a", new ArrayList<>());
        Recorder committing = new Recorder("b", new ArrayList<>());

        assertSame(plain, scope.register("a", plain));
        assertSame(committing, scope.registerCommitting("b", committing));
        assertEquals(2, scope.size());
    }

    @Test
    public void closesInExactReverseRegistrationOrder() throws IOException {
        List<String> closed = new ArrayList<>();
        ResourceScope scope = new ResourceScope();

        // Construction order: a socket, then a decorator reading from it. The decorator must
        // close first, or its close may attempt a read on a dead transport.
        scope.register("socket", new Recorder("socket", closed));
        scope.register("decrypt", new Recorder("decrypt", closed));
        scope.register("gunzip", new Recorder("gunzip", closed));

        scope.close();

        assertEquals(List.of("gunzip", "decrypt", "socket"), closed);
    }

    @Test
    public void closeAttemptsEveryResourceEvenAfterOneThrows() throws IOException {
        List<String> closed = new ArrayList<>();
        ResourceScope scope = new ResourceScope();

        scope.register("first", new Recorder("first", closed));
        scope.register("boom", new Recorder("boom", closed, true));   // cleanup failure: swallowed
        scope.register("last", new Recorder("last", closed));

        scope.close();

        // All three attempted, in reverse order, despite the middle one throwing.
        assertEquals(List.of("last", "boom", "first"), closed);
    }

    @Test
    public void aCleanupFailureIsSwallowedBecauseItCannotUnTransferBytes() {
        ResourceScope scope = new ResourceScope();
        scope.register("socket", new Recorder("socket", new ArrayList<>(), true));

        try {
            scope.close();
        } catch (IOException e) {
            fail("a register()ed resource failing to close must not fail the transfer: " + e);
        }
    }

    @Test
    public void aCommitFailureFailsTheTransferEvenThoughEveryByteWasRead() {
        ResourceScope scope = new ResourceScope();
        scope.register("socket", new Recorder("socket", new ArrayList<>()));
        scope.registerCommitting("rename", new Recorder("rename", new ArrayList<>(), true));

        try {
            scope.close();
            fail("expected a commit failure to propagate");
        } catch (IOException expected) {
            assertNotNull("the underlying failure must be the cause", expected.getCause());
            assertTrue(expected.getMessage().contains("promised state"));
        }
    }

    @Test
    public void multipleCommitFailuresThrowTheFirstAndSuppressTheRest() {
        ResourceScope scope = new ResourceScope();
        scope.registerCommitting("commitA", new Recorder("commitA", new ArrayList<>(), true));
        scope.registerCommitting("commitB", new Recorder("commitB", new ArrayList<>(), true));

        try {
            scope.close();
            fail("expected a commit failure to propagate");
        } catch (IOException expected) {
            assertEquals("one suppressed failure expected", 1, expected.getSuppressed().length);
        }
    }

    @Test
    public void closeNowReleasesOneResourceEarlyAndForgetsIt() throws IOException {
        List<String> closed = new ArrayList<>();
        ResourceScope scope = new ResourceScope();

        Recorder orphan = new Recorder("orphaned-upstream", closed);
        scope.register("orphaned-upstream", orphan);
        scope.register("kept", new Recorder("kept", closed));

        scope.closeNow(orphan);

        assertEquals(List.of("orphaned-upstream"), closed);
        assertEquals("closeNow must deregister as well as close", 1, scope.size());

        scope.close();

        // Closed exactly once, not again by close().
        assertEquals(List.of("orphaned-upstream", "kept"), closed);
        assertEquals(1, orphan.closeCount);
    }

    @Test
    public void closeNowIgnoresNullAndUnknownResources() {
        ResourceScope scope = new ResourceScope();
        scope.register("kept", new Recorder("kept", new ArrayList<>()));

        scope.closeNow(null);
        scope.closeNow(new Recorder("never-registered", new ArrayList<>()));

        assertEquals(1, scope.size());
    }

    @Test
    public void closeIsIdempotent() throws IOException {
        List<String> closed = new ArrayList<>();
        ResourceScope scope = new ResourceScope();
        scope.register("a", new Recorder("a", closed));

        scope.close();
        scope.close();

        assertEquals(List.of("a"), closed);
        assertTrue(scope.isClosed());
    }

    @Test
    public void registeringAfterCloseIsAProgrammingError() throws IOException {
        ResourceScope scope = new ResourceScope();
        scope.close();

        try {
            scope.register("late", new Recorder("late", new ArrayList<>()));
            fail("expected registration after close to be rejected");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("already closed"));
        }
    }

    @Test
    public void nullResourcesAreIgnoredRatherThanRegistered() {
        ResourceScope scope = new ResourceScope();
        scope.register("nothing", null);
        assertEquals(0, scope.size());
    }

    // ------------------------------------------------------------------ commit vs abort

    /**
     * The defect this split exists for: a failed run must not publish its partial artifact. Before
     * abort() existed, the pipeline's finally block closed committing resources on every path, so a
     * transfer that died at 60% renamed its truncated output into place and the next attempt read
     * that as a completed segment.
     */
    @Test
    public void abortDiscardsCommittingResourcesInsteadOfPublishingThem() {
        List<String> log = new ArrayList<>();
        ResourceScope scope = new ResourceScope();
        Recorder socket = new Recorder("socket", log);
        Recorder artifact = new Recorder("artifact", log);

        scope.register("socket", socket);
        scope.registerCommitting("artifact", artifact);

        scope.abort();

        assertEquals("the artifact must be discarded, the socket merely closed",
                List.of("abort:artifact", "socket"), log);
        assertEquals(0, artifact.closeCount);
        assertEquals(1, artifact.abortCount);
    }

    /** The success path is unchanged: closing a committing resource is what publishes it. */
    @Test
    public void closeStillPublishesCommittingResources() throws IOException {
        List<String> log = new ArrayList<>();
        ResourceScope scope = new ResourceScope();
        scope.registerCommitting("artifact", new Recorder("artifact", log));

        scope.close();

        assertEquals(List.of("artifact"), log);
    }

    /** abort() runs while a failure is already being reported, so it must never throw. */
    @Test
    public void abortNeverThrowsEvenWhenDiscardingFails() {
        ResourceScope scope = new ResourceScope();
        scope.registerCommitting("bad", new Recorder("bad", new ArrayList<>(), true));
        scope.register("alsoBad", new Recorder("alsoBad", new ArrayList<>(), true));

        scope.abort();   // must not throw

        assertTrue(scope.isClosed());
    }

    /** Every resource is attempted, even after one fails to discard. */
    @Test
    public void abortAttemptsEveryResourceEvenAfterOneThrows() {
        List<String> log = new ArrayList<>();
        ResourceScope scope = new ResourceScope();
        scope.register("first", new Recorder("first", log));
        scope.registerCommitting("boom", new Recorder("boom", log, true));
        scope.register("last", new Recorder("last", log));

        scope.abort();

        assertEquals(List.of("last", "abort:boom", "first"), log);
    }

    /** close() and abort() are mutually exclusive; whichever runs first wins. */
    @Test
    public void abortAfterCloseIsANoOp() throws IOException {
        List<String> log = new ArrayList<>();
        ResourceScope scope = new ResourceScope();
        Recorder artifact = new Recorder("artifact", log);
        scope.registerCommitting("artifact", artifact);

        scope.close();
        scope.abort();

        assertEquals(1, artifact.closeCount);
        assertEquals("already published; abort must not run", 0, artifact.abortCount);
    }

    /** Abandoning an orphaned upstream must not publish it either. */
    @Test
    public void closeNowDiscardsACommittingResourceRatherThanPublishingIt() {
        List<String> log = new ArrayList<>();
        ResourceScope scope = new ResourceScope();
        Recorder orphan = new Recorder("orphan", log);
        scope.registerCommitting("orphan", orphan);

        scope.closeNow(orphan);

        assertEquals(List.of("abort:orphan"), log);
        assertEquals(0, orphan.closeCount);
        assertEquals(0, scope.size());
    }
}
