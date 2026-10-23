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

package org.apache.synapse.stream.transfer;

/**
 * Where a submitted transfer is in its life.
 *
 * <p>Still deliberately no state for "retrying": a transfer waiting for another attempt is
 * {@code QUEUED} with a future {@code nextAttemptAt} and an {@code attempt} above zero, which is two
 * facts that cannot contradict each other. A state meaning the same thing could.
 *
 * <h2>Why pausing and stopping are states rather than a flag</h2>
 * Cancellation is <b>cooperative</b>: an operator polls {@code JobContext#isCancelled()} between
 * units, so a request to pause or stop lands up to one unit later — for a file sink, up to 8 MB.
 * Something has to be true during that window, and each candidate answer is worse:
 *
 * <ul>
 *   <li>Leaving it {@code RUNNING} loses the request if the node dies before the run notices.</li>
 *   <li>Writing the terminal state immediately makes the row claim the transfer stopped while it is
 *       still moving bytes.</li>
 *   <li>A separate {@code STOP_REQUESTED} column would have to be checked everywhere {@code STATUS}
 *       already is — including the claim query, which must not hand out a transfer someone just asked
 *       to stop.</li>
 * </ul>
 *
 * <p>So the intent <i>is</i> the status. {@code PAUSING} and {@code STOPPING} are not claimable, which
 * is the property that stops a second node picking the transfer up, and they are visibly transient, so
 * an operator watching sees the request land before it takes effect. The run that owns the transfer
 * writes the terminal state itself, because it is the only party that knows it has actually stopped.
 */
public enum TransferStatus {

    /** Submitted, not claimed. Also where a transfer waiting to be retried sits. */
    QUEUED,

    /** Claimed by a node and executing. */
    RUNNING,

    /**
     * Asked to pause, still winding down. The run stops at its next unit, flushes the position it had
     * earned, and leaves the workspace and checkpoints in place.
     */
    PAUSING,

    /**
     * Stopped at an operator's request, with its state kept. Not claimable — nothing will pick it up
     * until someone asks — and not terminal, because {@code start} puts it back on the queue and the
     * next attempt resumes from where this one left off.
     */
    PAUSED,

    /**
     * Asked to stop, still winding down. Differs from {@link #PAUSING} in what happens to the state:
     * the workspace and the checkpoints are discarded, so nothing can resume afterwards.
     */
    STOPPING,

    /** The transfer completed and everything it produced was committed. */
    DONE,

    /**
     * Terminal for this submission: either the failure will not succeed on a retry, or the retries ran
     * out. The workspace and checkpoints are kept, so {@code start} can put it back on the queue and
     * resume from the position the last attempt reached.
     */
    FAILED,

    /**
     * Given up before it ran, or stopped by an operator. Unlike {@link #FAILED}, nothing was kept:
     * a transfer that reaches here through {@link #STOPPING} has had its workspace and checkpoints
     * discarded, so it cannot be resumed — only submitted again as a new job.
     */
    CANCELLED;

    /** Whether nothing further will happen to a transfer in this state without someone asking. */
    public boolean isTerminal() {
        return this == DONE || this == FAILED || this == CANCELLED;
    }

    /**
     * Whether a transfer in this state occupies the queue — the window an identical submission joins.
     *
     * <p>Includes the paused and winding-down states: the transfer still exists, still owns a
     * workspace, and a second submission of the same values should join it rather than start a rival
     * transfer into the same run id.
     */
    public boolean isInFlight() {
        return this == QUEUED || this == RUNNING || this == PAUSING || this == PAUSED
                || this == STOPPING;
    }

    /**
     * Whether a node may claim a transfer in this state.
     *
     * <p>Only {@code QUEUED}. Written out rather than inferred from {@link #isTerminal()}, because
     * {@code PAUSING}, {@code PAUSED} and {@code STOPPING} are all non-terminal and all unclaimable,
     * and "not finished" is exactly the wrong test for whether to pick something up.
     */
    public boolean isClaimable() {
        return this == QUEUED;
    }

    /** Whether a run is winding down because someone asked it to. */
    public boolean isStopping() {
        return this == PAUSING || this == STOPPING;
    }
}
