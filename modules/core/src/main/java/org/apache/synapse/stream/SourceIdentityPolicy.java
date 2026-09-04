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
 * What to do when a retry finds that its source is not the one the first attempt read.
 *
 * <h2>The failure this exists to prevent</h2>
 * A retry deliberately reuses its run id — that is what makes resume work. So a queued transfer of
 * {@code /in/daily.csv} that failed at 60% comes back to the same workspace and picks up where it
 * stopped. If the file has been replaced in the meantime, resuming splices 60% of yesterday's file
 * onto 40% of today's and commits it as a complete artifact. Nothing reports anything.
 *
 * <p>The guard compares the {@code sourceId}, size and modification time recorded on the first attempt
 * against what this attempt found. It is not a checksum: a source that changed while keeping all three
 * identical still slips through, which is a known gap rather than a claim.
 *
 * <h2>Why the default is not {@link #STRICT}</h2>
 * A changed source is often exactly what the deployer intended — a file that is rewritten in place
 * between runs is an ordinary integration pattern. Refusing to run is the wrong default for that, and
 * being unable to run at all is a louder failure than the one being prevented. {@link #WARN} takes the
 * safe half of {@code STRICT} — never resume into artifacts belonging to different bytes — without the
 * half that stops the transfer.
 *
 * <p>Note that only a caller-supplied job id reaches this guard at all. When the framework
 * content-addresses a run from the seed's identity, a changed source already produces a different run
 * id and lands in its own workspace, so there is nothing to collide with.
 */
public enum SourceIdentityPolicy {

    /**
     * Refuse the run. The transfer fails with an error naming both identities.
     *
     * <p>For a pipeline where a mid-flight source change means something has gone wrong upstream and
     * continuing is worse than stopping.
     */
    STRICT,

    /**
     * Log loudly, discard the previous attempt's workspace, and run from the beginning.
     *
     * <p>The default. The discard is the point rather than a side effect: warning and then resuming
     * anyway would be the silent-corruption case with a log line attached, which is not an improvement.
     */
    WARN,

    /**
     * Resume regardless.
     *
     * <p>Only correct where the source is known to be appended to rather than replaced, so that older
     * bytes are genuinely still the same bytes. Wrong anywhere else, and wrong silently.
     */
    OFF;

    /**
     * Parses a configured value.
     *
     * @param value the attribute's value, trimmed by the caller; may be {@code null}
     * @return the policy, or {@code null} if the value names none
     */
    public static SourceIdentityPolicy parse(String value) {
        if (value == null) {
            return null;
        }
        for (SourceIdentityPolicy p : values()) {
            if (p.name().equalsIgnoreCase(value.trim())) {
                return p;
            }
        }
        return null;
    }
}
