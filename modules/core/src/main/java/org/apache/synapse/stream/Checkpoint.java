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
 * A durable resume point for one operator.
 * <p>
 * Deliberately <b>not a bare number</b>. It carries four things, and each exists because leaving
 * it out is a known defect in some shipped framework:
 * <ul>
 *   <li>{@code position} — how far through its input the operator got.</li>
 *   <li>{@code outputLength} — how many bytes of its artifact were durable at that point. On
 *       resume the operator truncates its output to this before continuing, which is what makes
 *       the artifact <b>exactly-once</b> even though side effects are not. Without it, a crash
 *       between the append and the checkpoint leaves the record in the output twice.</li>
 *   <li>{@code unit} — what {@code position} counts. See {@link CheckpointUnit}.</li>
 *   <li>{@code formatVersion} — the operator's own version tag. The operator owns the meaning of
 *       a checkpoint, so the meaning changes when the operator is upgraded; version 2 must not
 *       silently misread what version 1 wrote.</li>
 * </ul>
 *
 * <h2>The guarantee this supports</h2>
 * Output artifacts: exactly-once. Operator side effects: at-least-once, with at most one record
 * reprocessed per failure. The second half is a floor, not a shortfall — closing the window
 * further would require transactional coordination with whatever the operator calls, which is
 * not available for arbitrary user-authored mediation.
 *
 * @param position      how far through the input, in {@code unit}; never negative
 * @param outputLength  durable bytes of the operator's artifact; {@code 0} if it writes none
 * @param unit          what {@code position} counts; never {@code null}
 * @param formatVersion the operator's own format tag; never {@code null} or blank
 */
public record Checkpoint(long position, long outputLength, CheckpointUnit unit, String formatVersion) {

    public Checkpoint {
        if (position < 0) {
            throw new IllegalArgumentException("checkpoint position must not be negative: " + position);
        }
        if (outputLength < 0) {
            throw new IllegalArgumentException("checkpoint outputLength must not be negative: " + outputLength);
        }
        if (unit == null) {
            throw new IllegalArgumentException("checkpoint unit is required: a position without a unit is ambiguous");
        }
        if (formatVersion == null || formatVersion.isBlank()) {
            throw new IllegalArgumentException("checkpoint formatVersion is required");
        }
    }
}
