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

package org.apache.synapse.stream.spi;

import java.util.List;
import org.apache.synapse.stream.StreamSeed;

/**
 * One execution of a sequence, as it can be read back.
 *
 * <p>The read model for {@code MFT_JOB}, which until now was written and never queried: a run's row
 * existed so that a crash could be recognised and a resume could find its position, not so that anyone
 * could ask about it. {@link JobStore} is still the write side; this is what a listing sees.
 *
 * <h2>Every run is here, which is the point</h2>
 * A queued transfer has a row in both tables, joined by {@code runId}. A synchronous one — an inbound
 * endpoint, an API invoking a sequence inline — has a row only here, because it never passed through a
 * queue (ADR-0022). So this is the only place that can answer "what has this sequence been doing", and
 * the only place {@link #invokerName()} exists.
 *
 * @param runId          names this execution, and its workspace
 * @param sequenceName   the sequence that ran
 * @param invokerType    {@code API}, {@code INBOUND}, {@code PROXY}, {@code QUEUED} or {@code DIRECT}
 * @param invokerName    the <b>invocation path</b>: every artifact this run came through, in order.
 *                       See {@link Invoker}
 * @param status         {@code RUNNING}, {@code DONE}, {@code FAILED} or {@code INTERRUPTED}
 * @param attempt        which attempt this is, counting from 1
 * @param resumable      whether a further attempt could resume from what this one left
 * @param ownerNode      the node that ran it
 * @param workspacePath  where its restart state lives, when it has any
 * @param sourceId       what the source identified itself as, when it could
 * @param sourceSize     the source's size at identification, or {@link StreamSeed#UNKNOWN}
 * @param currentStage   the stage it reached
 * @param bytesDone      bytes the sink had taken
 * @param recordsDone    records the sink had taken
 * @param failedStage    the stage that failed, when one did
 * @param errorCode      the failure's code
 * @param errorMessage   the failure's message
 * @param retryable      whether the failure was retryable
 * @param createdAt      when the row was first written
 * @param startedAt      when this attempt began
 * @param firstStartedAt when the <b>first</b> attempt began
 * @param workingMillis  milliseconds spent working, summed over the attempts that have ended
 * @param endedAt        when it finished, either way
 * @param updatedAt      when progress was last recorded. <b>Not a liveness signal</b> (ADR-0028)
 */
public record Run(String runId, String sequenceName, String invokerType, String invokerName,
                  String status, int attempt, boolean resumable, String ownerNode,
                  String workspacePath, String sourceId, long sourceSize, String currentStage,
                  long bytesDone, long recordsDone, String failedStage, String errorCode,
                  String errorMessage, Boolean retryable, long createdAt, long startedAt,
                  long firstStartedAt, long workingMillis, long endedAt, long updatedAt) {

    /**
     * Wall clock for the whole run: first attempt's start to the final end.
     *
     * <p>Distinct from {@link #startedAt()}, which is reset every time a transfer is re-claimed and so
     * marks the <i>last</i> attempt. Both are kept because they answer different questions, and
     * reporting one under a name that suggests the other is how a resumed transfer comes to look fast:
     * its last attempt is legitimately shorter than a clean run, having skipped what it already did.
     *
     * @param now the current epoch millisecond, used only while the run is unfinished
     * @return elapsed milliseconds, counting up to {@code now} for a run still in flight
     */
    public long elapsedMillis(long now) {
        if (firstStartedAt <= 0) {
            return 0L;
        }
        return (endedAt > 0 ? endedAt : now) - firstStartedAt;
    }

    /**
     * How long the run was actually working, summed across attempts.
     *
     * <p>Excludes everything between attempts — the retry back-off, a server that was down, time spent
     * queued. For a transfer killed once and resumed this is roughly what a clean run would take; for
     * a clean run it equals {@link #elapsedMillis(long)}. The gap between the two is dead time, and
     * until these two existed no field answered "how much work did this transfer do".
     *
     * <p>{@code workingMillis} is only credited when an attempt <i>ends</i>, so the attempt in flight
     * is added here rather than read from the column. A caller that reported the column alone would
     * show a running transfer as doing no work at all until the moment it finished.
     *
     * @param now the current epoch millisecond, used only while an attempt is in flight
     */
    public long processingMillis(long now) {
        long inFlight = endedAt <= 0 && startedAt > 0 ? Math.max(0L, now - startedAt) : 0L;
        return workingMillis + inFlight;
    }

    /**
     * @return the artifacts this run came through, in order; empty when none was identified
     */
    public List<String> invocationPath() {
        return Invoker.segments(invokerName);
    }

    /**
     * @param artifact an API, proxy, inbound endpoint, message store or message processor
     * @return whether this run came through it — matching a whole segment, so {@code api1} does not
     *         answer for {@code api10}
     */
    public boolean passedThrough(String artifact) {
        return Invoker.passedThrough(invokerName, artifact);
    }
}
