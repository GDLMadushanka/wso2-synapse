/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.synapse.stream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The server-wide "stop soon" signal, and the count of transfers still running.
 *
 * <h2>Graceful shutdown is an optimisation, not a correctness requirement</h2>
 * {@code unitDone} fsyncs the artifact before advancing the position, so at every instant the recorded
 * position describes work that definitely landed. Killing the process mid-transfer costs <b>redone
 * work</b>, never correctness. Everything here exists to reduce that rework and to let a transfer that
 * cannot be redone finish — and its failure mode is simply what would have happened anyway.
 *
 * <p>That is why {@link #awaitQuiet} may time out without a second thought: expiry falls back to abrupt
 * termination, which is already safe. The hard question — how long to wait — has no dangerous answer.
 *
 * <h2>Two arms, split on whether anything will redo the work</h2>
 * A <b>resumable</b> run is asked to stop at its next unit boundary; a retry picks it up from the
 * checkpoint. A <b>non-resumable</b> one is left alone to finish, because stopping it destroys work
 * nothing will redo — an API request body cannot be read twice. {@code StreamPipeline} applies that
 * split; this class only carries the signal. See ADR-0037.
 *
 * <p>Static because a shutdown is a property of the server rather than of any one pipeline, and an
 * operator must be able to observe it through the {@code JobContext} it already has, without every
 * caller being taught to pass something new.
 */
public final class StreamShutdown {

    private static final Log log = LogFactory.getLog(StreamShutdown.class);

    private static final AtomicBoolean requested = new AtomicBoolean();
    private static final AtomicInteger running = new AtomicInteger();
    private static final Object quiet = new Object();

    private StreamShutdown() {
    }

    /**
     * Asks every resumable transfer to stop at its next unit boundary.
     *
     * <p>Idempotent, and deliberately does not wait — call {@link #awaitQuiet} for that, so a caller
     * can signal first and then do other shutdown work while transfers wind down.
     */
    public static void request() {
        if (requested.compareAndSet(false, true)) {
            log.info("Stream pipelines asked to stop: " + running.get() + " transfer(s) in flight."
                    + " Resumable ones stop at their next unit and resume later; the rest run to"
                    + " completion, because nothing would redo them");
        }
    }

    /** Whether a shutdown has been asked for. Read by every resumable run's {@code isCancelled()}. */
    public static boolean requested() {
        return requested.get();
    }

    /**
     * Clears the signal. For tests and for a server that starts again in the same JVM — without it a
     * second run in the same process would see a shutdown that is long over.
     */
    public static void reset() {
        requested.set(false);
    }

    /** Counts a transfer in. Paired with {@link #ended()} in a finally. */
    public static void started() {
        running.incrementAndGet();
    }

    /** Counts a transfer out, waking anything waiting for quiet. */
    public static void ended() {
        if (running.decrementAndGet() <= 0) {
            synchronized (quiet) {
                quiet.notifyAll();
            }
        }
    }

    /** How many transfers are still running. */
    public static int inFlight() {
        return Math.max(0, running.get());
    }

    /**
     * Waits for in-flight transfers to finish, for at most {@code timeoutMillis}.
     *
     * <p>Returning {@code false} is not a failure. What follows is ordinary termination, which loses at
     * most the rework already bounded by {@code maxReprocessed} for a resumable run, and for a
     * non-resumable one loses what any interrupted mediation loses. The caller should log and carry on
     * rather than escalate.
     *
     * <p><b>Expiry is not logged here</b>, because this is built to be called repeatedly with a short
     * timeout by a caller polling on its own deadline — MI's shutdown loop is exactly that — and a
     * warning per poll would say "giving up" once a second while the wait is still going fine. The
     * caller owns the deadline, so the caller reports reaching it.
     *
     * @param timeoutMillis how long to wait; not positive means do not wait at all
     * @return {@code true} if nothing is left running
     */
    public static boolean awaitQuiet(long timeoutMillis) {
        long deadline = System.currentTimeMillis() + Math.max(0L, timeoutMillis);
        synchronized (quiet) {
            while (running.get() > 0) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0L) {
                    return false;
                }
                try {
                    quiet.wait(Math.min(remaining, 200L));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return running.get() == 0;
                }
            }
        }
        return true;
    }
}
