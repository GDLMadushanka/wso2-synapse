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

/**
 * One execution of a pipeline, as it can be read back.
 *
 * <p>The read model for {@code MFT_JOB}, which until now was written and never queried: a run's row
 * existed so that a crash could be recognised and a resume could find its position, not so that anyone
 * could ask about it. {@link JobStore} is still the write side; this is what a listing sees.
 *
 * <h2>Every run is here, which is the point</h2>
 * A queued transfer has a row in both tables, joined by {@code runId}. A synchronous one — an inbound
 * endpoint, an API invoking a pipeline inline — has a row only here, because it never passed through a
 * queue (ADR-0022). So this is the only place that can answer "what has this pipeline been doing", and
 * the only place {@link #invokerName()} exists.
 *
 * @param runId          names this execution, and its workspace
 * @param pipelineName   the pipeline that ran
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
 * @param endedAt        when it finished, either way
 * @param updatedAt      when progress was last recorded. <b>Not a liveness signal</b> (ADR-0028)
 */
public record Run(String runId, String pipelineName, String invokerType, String invokerName,
                  String status, int attempt, boolean resumable, String ownerNode,
                  String workspacePath, String sourceId, long sourceSize, String currentStage,
                  long bytesDone, long recordsDone, String failedStage, String errorCode,
                  String errorMessage, Boolean retryable, long createdAt, long startedAt,
                  long endedAt, long updatedAt) {

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
