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

import java.nio.charset.StandardCharsets;

/**
 * A durable resume point for one operator.
 * <p>
 * Deliberately <b>not a bare number</b>. It carries five things, and each exists because leaving
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
 *   <li>{@code payload} — optional operator-owned state, for a resume position that is not a
 *       single number. See below.</li>
 * </ul>
 *
 * <h2>Why the payload exists</h2>
 * A single integer cannot express a <b>state machine</b>, and the failure mode of guessing which
 * state you were in is silent data loss.
 * <p>
 * An external merge sort has two phases: split the input into sorted runs, then merge them. Killed
 * during the split it must resume reading input; killed during the merge it must not read the input
 * at all. Both states can present as {@code position = 400000000, outputLength = 0}, and inferring
 * which one from the number of sealed run files is a chain of assumptions that breaks the moment
 * chunks are sized by memory rather than row count. Guess "merging" when it was still splitting and
 * you merge the runs you have, dropping every row that had not been written yet — a correctly
 * sorted, complete-looking, wrong result.
 * <p>
 * So the operator writes what it knows instead of encoding it:
 * <pre>
 *   {"phase":"split","runsSealed":7,"rowsConsumed":354000000}
 *   {"phase":"merge","runs":8}
 * </pre>
 * Note {@code formatVersion} does not help here: it versions the <i>format</i>, telling you whether
 * the payload can be parsed, not which state the operator was in.
 *
 * <h2>The payload augments; it never replaces</h2>
 * {@code position} and {@code outputLength} stay first-class, because the framework reconciles
 * {@code L <= A} and truncates the artifact on resume using them. Move that state into an opaque blob
 * and every guarantee reverts to the operator, which is the opposite of what ADR-0031 is for.
 *
 * <h2>Bounded, and loud at the bound</h2>
 * Capped at {@value #MAX_PAYLOAD_BYTES} bytes of <b>UTF-8</b>, and an over-long payload is
 * <b>refused</b>. Both halves are deliberate. Spring Batch persists its execution context to a
 * {@code VARCHAR(2500)} plus a CLOB and truncates the short form when it overflows — which can write
 * invalid JSON that fails on a later read — and its own limit has to be halved for multi-byte
 * charsets. Measuring characters instead of bytes is the same trap from the other end.
 * <p>
 * Keep it primitive. Developer-owned format should mean the developer owns a small map or a short
 * string, not an object graph whose class shape has to survive a connector upgrade.
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
 * @param payload       operator-owned state, or {@code null}; at most
 *                      {@value #MAX_PAYLOAD_BYTES} bytes of UTF-8
 */
public record Checkpoint(long position, long outputLength, CheckpointUnit unit, String formatVersion,
                         String payload) {

    /**
     * The largest payload accepted, in bytes of UTF-8.
     *
     * <p>4 KB is the Oracle boundary for storing a CLOB inline, comfortably holds a hundred merge
     * positions as JSON, and sits above the point at which PostgreSQL moves a value out of line
     * anyway — so there is no storage tier to be gained by picking a smaller number.
     */
    public static final int MAX_PAYLOAD_BYTES = 4096;

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
        if (payload != null) {
            int bytes = payload.getBytes(StandardCharsets.UTF_8).length;
            if (bytes > MAX_PAYLOAD_BYTES) {
                // Refused, never trimmed. A truncated payload is not a smaller payload — it is
                // usually invalid JSON that reads back as a corrupt checkpoint far from here.
                throw new IllegalArgumentException("checkpoint payload is " + bytes + " bytes of UTF-8,"
                        + " over the " + MAX_PAYLOAD_BYTES + "-byte limit. It is refused rather than"
                        + " truncated, because a trimmed payload usually parses as something else or"
                        + " not at all. Keep resume state small and primitive: a position, a phase, a"
                        + " handful of offsets — not a serialized object graph");
            }
        }
    }

    /**
     * A checkpoint that is just a position, for the common case of an operator whose resume state is
     * one number.
     *
     * @param position      how far through the input, in {@code unit}
     * @param outputLength  durable bytes of the operator's artifact
     * @param unit          what {@code position} counts
     * @param formatVersion the operator's own format tag
     */
    public Checkpoint(long position, long outputLength, CheckpointUnit unit, String formatVersion) {
        this(position, outputLength, unit, formatVersion, null);
    }
}
