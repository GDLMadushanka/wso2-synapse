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

/**
 * What the framework knows about a run at the moment it starts, handed to {@link JobStore#start}.
 *
 * <p>Deliberately does <b>not</b> carry the owning node. Synapse has no notion of cluster membership
 * and must not acquire one; the store implementation belongs to the product, which does. Keeping node
 * identity on that side is also what lets crash recovery match a row to a node without any of that
 * concept leaking into the pipeline.
 *
 * <p>The three source-identity fields are what a later attempt is checked against. They may be absent
 * — a caller-supplied stream need not know its own length — in which case {@code sourceId} is
 * {@code null} and the guard has nothing to compare and does not fire.
 *
 * <h2>The invoker is who caused this run to execute</h2>
 * Not who originally asked for the transfer. For a queued transfer the submitting API enqueued a
 * descriptor and returned without running anything, so the invoker is the <b>message processor</b> that
 * later picked it up. A queued run's row is therefore created with no invoker at all, and gains one at
 * the moment something actually invokes the pipeline.
 *
 * @param runId              the framework's name for this run; the primary key
 * @param pipelineName       the pipeline being run
 * @param invokerType        {@code API}, {@code INBOUND}, {@code PROXY}, {@code PROCESSOR} or
 *                           {@code DIRECT}; never {@code null}
 * @param invokerName        the invoking artifact's name, or {@code null} when nothing identified it
 * @param resumable          whether this run could be resumed, as computed for this origin
 * @param workspacePath      the run's directory, or {@code null} when it has none
 * @param sourceId           the seed's opaque {@link StreamSeed#identity()}, or {@code null} when the
 *                           provider had none — in which case nothing can resume this run
 * @param sourceSize         source byte length, or {@link StreamSeed#UNKNOWN}. Telemetry only
 * @param sourceLastModified source modification time in epoch millis, or {@link StreamSeed#UNKNOWN}.
 *                           Telemetry only
 */
public record JobRun(String runId, String pipelineName, String invokerType, String invokerName,
                     boolean resumable, String workspacePath, String sourceId, long sourceSize,
                     long sourceLastModified) {
}
