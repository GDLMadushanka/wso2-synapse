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

import java.util.List;
import java.util.Map;

/**
 * Where submitted transfers wait, and the record of what became of them.
 *
 * <p>The storage half of MFT's asynchronous flow: a {@code <mftSubmit>} writes here and gets an id back,
 * an MFT message processor claims from here, and the management API reads from here. Rows live beside
 * the job records in the same datasource, so a claim and the job row it causes are one commit.
 *
 * <h2>A submission is not a run</h2>
 * They are separate rows with separate lifetimes. A submission may never become a run — cancelled, or
 * still queued — and most runs have no submission, because a synchronous transfer never passed through
 * here. {@link #linkRun} is the join, written when the run starts.
 *
 * <h2>What an implementation must guarantee</h2>
 * <ul>
 *   <li><b>{@link #claim} is atomic.</b> Two callers must never receive the same transfer. A conditional
 *       update whose affected-row count decides the winner is enough, and is the point:
 *       {@code JDBCMessageStore.peek()} selects {@code min(indexId)} with no locking, so its consumers
 *       avoid concurrency rather than surviving it.</li>
 *   <li><b>An idempotency key is unique within a store.</b> Enforced, not checked — a caller that
 *       supplies one is asserting "these are the same request", and a unique constraint is what makes
 *       that true under a race.</li>
 *   <li><b>Failure throws.</b> A store that cannot be reached must not read as an empty queue, or a
 *       processor polls a broken datasource for ever and reports nothing wrong.</li>
 * </ul>
 */
public interface TransferStore {

    /**
     * Queues a transfer, or hands back one already in flight that is indistinguishable from it.
     *
     * <p>Two different guards, and the difference is deliberate:
     * <ul>
     *   <li>An <b>idempotency key</b> is <i>enforced</i>. The same key twice returns the first id
     *       however long ago it was submitted and whatever became of it — and the same key with
     *       <b>different values</b> is <b>refused</b>, because the caller has contradicted themselves
     *       and neither answer is one they could see: returning the first would discard what they just
     *       submitted, queuing a second would make the key mean nothing.</li>
     *   <li>Identical <b>parameters</b> collapse only while the first transfer is {@code QUEUED} or
     *       {@code RUNNING}, and only as a <i>best effort</i>: two exactly simultaneous submissions may
     *       both be queued, which degrades to one transfer per submission — the ordinary rule. Once the
     *       first is finished the same parameters queue a new transfer, because "send it again" must
     *       stay possible without a feature.</li>
     * </ul>
     *
     * <p>Nothing here is validated against the pipeline that will run it. The store does not know which
     * processor drains it, so it cannot know which parameters a source operator needs; a missing one
     * fails when the run builds, and is recorded on the job.
     *
     * @param storeName      the store to submit to
     * @param parameters     the values the pipeline's operators will read; may be empty
     * @param idempotencyKey the caller's assertion of sameness, or {@code null}
     * @param submitter      the artifact this submission came through, or {@code null} when none could
     *                       be identified. Recorded rather than derived later because it cannot be:
     *                       whatever submitted has returned by the time anything runs the transfer
     * @return the id, and whether this call is what created it
     * @throws StreamException if the store could not be reached, or if this key already belongs to a
     *                         transfer with different values — the latter not retryable, since
     *                         repeating the request cannot resolve a contradiction inside it
     */
    Submission submit(String storeName, Map<String, String> parameters, String idempotencyKey,
                      String submitter) throws StreamException;

    /**
     * Takes the oldest transfer this node may run, atomically.
     *
     * <p><b>The store records who claimed it, not the caller.</b> Ownership has to name the <i>node</i>,
     * because the only thing that recovers a transfer whose process died is that node recognising its
     * own rows when it starts again. A processor knows its own name and nothing about node identity —
     * and its name is the same string on every node in a cluster, so a sweep keyed on it would requeue
     * transfers that a different, perfectly live node is still writing.
     *
     * @param storeName the store to claim from
     * @return the claimed transfer, or {@code null} when nothing is waiting
     * @throws StreamException if the store could not be reached
     */
    Transfer claim(String storeName) throws StreamException;

    /**
     * Records which run is executing a claimed transfer.
     *
     * <p>Called once the run has been named, which is the first moment the two can be joined: the id
     * comes from this store, the run id from the pipeline.
     *
     * @param jobId the transfer
     * @param runId the run executing it
     * @throws StreamException if the store could not be reached
     */
    void linkRun(long jobId, String runId) throws StreamException;

    /**
     * Returns a failed transfer to the queue for another attempt.
     *
     * <p>Same job id, so the next attempt lands in the workspace the last one left and resumes from its
     * checkpoint. {@code notBefore} is a column rather than a sleep because the wait belongs to the
     * transfer: another node must not claim it early either.
     *
     * @param jobId     the transfer to retry
     * @param notBefore epoch millis before which no node may claim it
     * @return {@code true} if it was requeued
     * @throws StreamException if the store could not be reached
     */
    boolean requeue(long jobId, long notBefore) throws StreamException;

    /**
     * Records a transfer as finished, either way.
     *
     * @param jobId        the transfer
     * @param status       {@link TransferStatus#DONE} or {@link TransferStatus#FAILED}
     * @param errorCode    the failure's code, or {@code null}
     * @param errorMessage the failure's message, or {@code null}
     * @throws StreamException if the store could not be reached
     */
    void finish(long jobId, TransferStatus status, String errorCode, String errorMessage)
            throws StreamException;

    /**
     * Asks a transfer to pause: stop at its next unit and keep everything it has earned.
     *
     * <p>Sets {@link TransferStatus#PAUSING} on a running transfer, or {@link TransferStatus#PAUSED}
     * directly on one that has not started — there is nothing to wind down, so the request is already
     * satisfied. Either way the transfer stops being claimable, which is what "remove it from the
     * store" means here: the row stays, because it is also the only record of what happened to it.
     *
     * @param jobId the transfer to pause
     * @return the status it now has, or {@code null} if no such transfer exists or it had already
     *         finished — a paused transfer that is already {@code DONE} is not an error to report, it
     *         is a race the caller lost
     * @throws StreamException if the store could not be reached
     */
    TransferStatus requestPause(long jobId) throws StreamException;

    /**
     * Asks a transfer to stop, discarding what it has earned.
     *
     * <p>Sets {@link TransferStatus#STOPPING} on a running transfer, or {@link TransferStatus#CANCELLED}
     * directly on one that has not started. The difference from {@link #requestPause} is entirely in
     * what the run does as it unwinds: a stop discards the workspace and the checkpoints, so nothing
     * can resume afterwards.
     *
     * @param jobId the transfer to stop
     * @return the status it now has, or {@code null} if no such transfer exists or it had already
     *         finished
     * @throws StreamException if the store could not be reached
     */
    TransferStatus requestStop(long jobId) throws StreamException;

    /**
     * Puts a paused or failed transfer back on the queue.
     *
     * <p>Claimable at once, and <b>not</b> a promise of immediate execution: a processor claims it on
     * its next poll, behind whatever else is waiting and bounded by its concurrency. Same job id, so
     * the run is named the same way and lands in the workspace the previous attempt left — which is
     * what makes this a resume rather than a restart.
     *
     * <p>The attempt counter is reset. An operator asking for this has made a decision the retry bound
     * was standing in for, and a transfer that came back only to be given up immediately because its
     * count was already exhausted would be a confusing answer to a deliberate request.
     *
     * @param jobId the transfer to requeue
     * @return {@code true} if it was paused or failed and is now queued; {@code false} if it was in no
     *         state to start — running, already queued, or terminal with nothing kept
     * @throws StreamException if the store could not be reached
     */
    boolean start(long jobId) throws StreamException;

    /**
     * Records that a run has finished winding down after a pause or stop request.
     *
     * <p>Called by the run itself, which is the only party that knows it has actually stopped. Moves
     * {@link TransferStatus#PAUSING} to {@link TransferStatus#PAUSED} and {@link TransferStatus#STOPPING}
     * to {@link TransferStatus#CANCELLED}.
     *
     * @param jobId the transfer
     * @return the terminal-for-now status written, or {@code null} if the transfer was not winding down
     * @throws StreamException if the store could not be reached
     */
    TransferStatus stopped(long jobId) throws StreamException;

    /**
     * Gives up a transfer that has not started. One already claimed belongs to whoever is running it,
     * and marking it cancelled underneath them would describe a transfer that is still moving bytes.
     *
     * @param jobId the transfer to cancel
     * @return {@code true} if it was queued and no longer is
     * @throws StreamException if the store could not be reached
     */
    boolean cancel(long jobId) throws StreamException;

    /**
     * @param jobId the transfer to look up
     * @return it, or {@code null} if no such job id exists
     * @throws StreamException if the store could not be reached
     */
    Transfer find(long jobId) throws StreamException;

    /**
     * Lists transfers, newest first.
     *
     * @param storeName restrict to one store, or {@code null} for all
     * @param status    restrict to one {@link TransferStatus}, or {@code null} for all
     * @param limit     the most to return
     * @return the matching transfers; empty when there are none
     * @throws StreamException if the store could not be reached
     */
    List<Transfer> list(String storeName, String status, int limit) throws StreamException;

    /**
     * @param storeName the store to count
     * @return how many transfers are waiting in it
     * @throws StreamException if the store could not be reached
     */
    int queued(String storeName) throws StreamException;
}
