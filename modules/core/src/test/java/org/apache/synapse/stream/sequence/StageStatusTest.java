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

import org.apache.synapse.stream.JobContext;
import org.apache.synapse.stream.ResourceScope;
import org.junit.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@code ctx.status(...)} — the activity label an operator sets so a stage that is busy without
 * emitting can say so (ADR-0069).
 *
 * <p>What these pin down is the <b>asymmetry</b> the decision rests on: an operator may call as often
 * as it likes, and the store must not see one write per call. If that stops being true, a sort
 * reporting per sealed run turns into four thousand round trips on the transfer's own thread, which
 * is exactly the cost the pull model exists to avoid.
 */
public class StageStatusTest {

    /** Records what reached the store, in order. */
    private static final class Recorder implements JobContext {

        private final List<String> seen = new ArrayList<>();
        private RuntimeException toThrow;

        @Override
        public String jobId() {
            return "job-status";
        }

        @Override
        public void status(String stage, int index, String activity) {
            if (toThrow != null) {
                throw toThrow;
            }
            seen.add(stage + "#" + index + ":" + activity);
        }
    }

    private static DefaultStreamContext contextWith(Recorder recorder) {
        return new DefaultStreamContext(null, recorder, new ResourceScope(), "sort",
                false, false, null, null, null, 2);
    }

    /**
     * Many calls, one write. The first reaches the store immediately — a stage that has just started
     * is the one a console is most likely to be watching — and the rest are absorbed.
     */
    @Test
    public void manyCallsProduceOneStoreWrite() {
        Recorder recorder = new Recorder();
        DefaultStreamContext ctx = contextWith(recorder);

        for (int i = 1; i <= 500; i++) {
            ctx.status("splitting: sealed run " + i);
        }

        assertEquals("the throttle must absorb everything after the first", 1, recorder.seen.size());
        assertEquals("sort#2:splitting: sealed run 1", recorder.seen.get(0));
    }

    /**
     * The last thing a stage said survives the run even though its interval had not elapsed.
     *
     * <p>This is the case that matters most: on a failed transfer the final label names what it was
     * doing when it died, and losing it to a three-second window would lose it exactly when it is
     * worth having.
     */
    @Test
    public void theFinalValueIsFlushedOnUnwind() {
        Recorder recorder = new Recorder();
        DefaultStreamContext ctx = contextWith(recorder);

        ctx.status("splitting: sealed run 1");
        ctx.status("splitting: sealed run 47");
        ctx.flushStatus();

        assertEquals(2, recorder.seen.size());
        assertEquals("sort#2:splitting: sealed run 47", recorder.seen.get(1));
    }

    /** And it does not write the same value twice when nothing moved since the last write. */
    @Test
    public void theFinalFlushSkipsAValueAlreadyWritten() {
        Recorder recorder = new Recorder();
        DefaultStreamContext ctx = contextWith(recorder);

        ctx.status("splitting: sealed run 1");
        ctx.flushStatus();

        assertEquals(1, recorder.seen.size());
    }

    /** Nothing to say is not something to say. */
    @Test
    public void blankAndNullAreIgnored() {
        Recorder recorder = new Recorder();
        DefaultStreamContext ctx = contextWith(recorder);

        ctx.status(null);
        ctx.status("");
        ctx.status("   ");
        ctx.flushStatus();

        assertTrue(recorder.seen.isEmpty());
    }

    /**
     * A broken store must not fail a transfer, and must not be retried on every call.
     *
     * <p>The second half is the one worth a test: if the throttle timestamp only advanced on success,
     * a database that was down would be retried on every single call — turning an outage into a slow
     * transfer, which is a worse failure than the missing telemetry.
     */
    @Test
    public void aFailingStoreNeitherPropagatesNorRetriesEveryCall() {
        Recorder recorder = new Recorder();
        recorder.toThrow = new IllegalStateException("store is down");
        DefaultStreamContext ctx = contextWith(recorder);

        for (int i = 0; i < 100; i++) {
            ctx.status("splitting: sealed run " + i);      // must not throw
        }
        ctx.flushStatus();

        assertTrue("a failed write still advances the throttle", recorder.seen.isEmpty());
    }

    /** A sequence assembled in code has no job to report to, and must not blow up over it. */
    @Test
    public void aContextWithNoJobStillAcceptsStatus() {
        DefaultStreamContext ctx = new DefaultStreamContext(null, null, new ResourceScope(), "sort",
                false, false, (Path) null, null, null, 0);

        ctx.status("splitting: sealed run 1");
        ctx.flushStatus();
    }
}
