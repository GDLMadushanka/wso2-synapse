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

import java.io.IOException;

/**
 * Durable storage for one operator's resume position.
 * <p>
 * <b>One live checkpoint per stage, overwritten.</b> Not a log: {@link #append} supersedes whatever was
 * there and {@link #lastComplete} is the only reader. A history would be needed only if the artifact
 * could be shorter than the recorded length, and the durable flush required below rules that out —
 * ADR-0017. The method is named {@code append} for the ordering it implies, not for a log it keeps.
 * <p>
 * The framework owns durability, atomicity and where the bytes live. The operator owns what a
 * checkpoint <i>means</i> and when to write one. No path or file is exposed here, deliberately:
 * that is what allows the backing store to change later without touching a connector.
 *
 * <h2>The ordering contract — the whole correctness argument</h2>
 * A checkpoint must never be ahead of the effect it describes:
 * <pre>
 *   1. do the work            (side effects happen)
 *   2. confirm it landed      (output is durable — a buffered flush is not; see below)
 *   3. append the checkpoint  (position moves)
 * </pre>
 * <b>Never fused. Never reordered for throughput.</b> A checkpoint ahead of its effect is silent
 * data loss; behind it is one duplicate. Only one of those is acceptable, which is why this is
 * not a performance decision.
 * <p>
 * <b>Step 2 means an <i>fsync</i> of the artifact, not a {@code flush()}.</b> Stating it negatively
 * because "durable" alone reads as satisfied by a {@code BufferedOutputStream.flush()}, which only
 * reaches the OS page cache. Lose power there and the file is the right length with unwritten gaps
 * below it — a state no length comparison can detect, so the {@code L <= A} reconciliation that catches
 * every other torn write silently passes it.
 * <p>
 * This is where comparable frameworks have gone wrong. Both that had to rewrite their resume
 * machinery rewrote this coupling rather than their data model — one of them specifically because
 * checkpoint state was being persisted for work that had since been rolled back.
 *
 * <h2>Storage is not part of this contract</h2>
 * Deliberately no file, path or offset appears in these signatures. That is what allowed checkpoints
 * to move from an append-only log in the workspace to rows in the database without touching a single
 * operator — see ADR-0017, which supersedes ADR-0012.
 * <p>
 * Implementations owe two properties regardless of medium: a write must be <b>atomic</b>, so a crash
 * never leaves a half-written checkpoint that reads as valid; and a read must <b>never invent one</b>.
 * A null or defaulted checkpoint is indistinguishable from "no checkpoint", which means "start from
 * the beginning" and quietly duplicates everything.
 */
public interface CheckpointStore {

    /**
     * The most recent complete checkpoint, or {@code null} if this operator has never written
     * one in this workspace.
     * <p>
     * A {@code null} return means <b>"no checkpoint"</b> and nothing else. It must never be used
     * to report a checkpoint that exists but could not be read: a caller cannot distinguish
     * those, and treating an unreadable checkpoint as absent means starting from the beginning
     * and silently duplicating everything already done. An unreadable or truncated-beyond-repair
     * store throws instead.
     * <p>
     * A half-written checkpoint is never observable: the transition must be atomic, so a reader sees
     * either the previous position or the new one and never a mixture.
     *
     * @return the last complete checkpoint, or {@code null} if there is none
     * @throws IOException if the store exists but cannot be read
     */
    Checkpoint lastComplete() throws IOException;

    /**
     * Records a checkpoint, superseding any earlier one for this stage, and does not return until it is durable.
     * <p>
     * Call this <b>after</b> the work it describes is durable, never before or alongside. See the
     * ordering contract in the class javadoc.
     *
     * @param checkpoint the point reached; never {@code null}
     * @throws IOException if the checkpoint could not be made durable — in which case the caller
     *                     must treat the position as not advanced
     */
    void append(Checkpoint checkpoint) throws IOException;

    /**
     * Forgets this stage's checkpoint, so the next attempt starts the segment from clean.
     *
     * <p>Needed because discarding an artifact is not enough on its own. When the source-identity guard
     * finds that a retry is reading different bytes, it deletes the previous attempt's workspace — but a
     * stage that declares {@code checkpointed()} without {@code materialises()} has no artifact to
     * reconcile against, so its recorded position survived the discard and the stage resumed at a byte
     * offset into a file that no longer existed. A sink would skip 250 GB of the <i>new</i> source and
     * append the remainder onto a half-written destination, complete-looking and wrong, after a log line
     * claiming the run had started over.
     *
     * <p>Must be idempotent: clearing a stage that has no checkpoint is not an error.
     *
     * @throws IOException if the checkpoint could not be removed — which must propagate, because
     *                     continuing would resume from a position known to be wrong
     */
    void clear() throws IOException;
}
