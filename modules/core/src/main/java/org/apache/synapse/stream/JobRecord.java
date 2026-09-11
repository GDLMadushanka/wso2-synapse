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
 * What a previous attempt at this run left behind, returned by {@link JobStore#start}.
 *
 * <p>Its only job is to answer two questions the framework asks at the start of a run: how many
 * attempts have already happened, and <b>were they reading the same bytes</b>. The second is the
 * source-identity guard, and it is the reason this type exists at all — without a record of what the
 * first attempt saw, a resume cannot tell a continuation from a different file that happens to have
 * the same name.
 *
 * @param runId              the run this describes
 * @param status             the status the previous attempt ended in
 * @param attempt            how many attempts had been made before this one
 * @param sourceId           the seed's opaque {@link StreamSeed#identity()} as recorded then, or
 *                           {@code null} if none was. Compared for <b>equality only</b> — the framework
 *                           has no opinion about what is inside it (ADR-0033)
 * @param sourceSize         the size recorded then, or {@link StreamSeed#UNKNOWN}. Telemetry: it appears
 *                           in diagnostics and feeds no decision about whether the source changed
 * @param sourceLastModified the modification time recorded then, or {@link StreamSeed#UNKNOWN}.
 *                           Telemetry, as {@code sourceSize} is
 */
public record JobRecord(String runId, String status, int attempt, String sourceId, long sourceSize,
                        long sourceLastModified) {

    /**
     * Whether this record carries enough to check a source against.
     *
     * @return {@code true} if a {@code sourceId} was recorded
     */
    public boolean hasSourceIdentity() {
        return sourceId != null && !sourceId.isBlank();
    }
}
