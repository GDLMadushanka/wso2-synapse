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
 * The deployer's checkpoint cadence, as every checkpointed operator reads it.
 *
 * <pre>
 *   &lt;checkpointFrequency&gt;500&lt;/checkpointFrequency&gt;
 * </pre>
 *
 * <h2>A parameter, not an attribute</h2>
 * It was a framework-reserved stage attribute, read by the sequence and stripped before the
 * operator's factory saw the element. That put it out of reach of the tooling, which builds a
 * connector's form from the template's <b>parameters</b> and cannot see an attribute — so the one
 * number a deployer most needs to tune was the one they had to hand-edit the XML for. ADR-0067 moved
 * it to the operator that counts the units, which is also the only party that knows what a unit is.
 *
 * <h2>What the number means</h2>
 * <b>At most this many of the stage's own units reprocessed after a failure, repeating whatever side
 * effects they had.</b> That exposure is the deployer's alone to size and is validated against
 * nothing (ADR-0025) — the framework compares it to no capability and asks no operator's permission.
 *
 * <p>The unit is the operator's: records for a loop, blocks for a blob or file sink. How large one
 * is sits next to this in the same configuration — {@code chunkSize}, {@code blockSize},
 * {@code writeBlock} — so a deployer sizing the duplicate window has both halves in one place.
 *
 * <p>It is also the only lever on that stage's {@code fsync} rate, which is the real throughput
 * ceiling. Raising it buys speed and pays in duplicates; that trade is the whole of the parameter.
 *
 * <h2>Defaults belong to the operator</h2>
 * This class defaults nothing, because the right default differs by what is being counted. A record
 * loop mediating a deployer's sequence wants the cautious 1: a repeated record is a repeated
 * request. A sink staging bytes nothing can see before commit wants far coarser, because a
 * reprocessed block costs only bandwidth. Each operator passes its own {@code fallback}.
 */
public final class CheckpointFrequency {

    private CheckpointFrequency() {
    }

    /** The parameter name, so the five operators that read it cannot spell it four ways. */
    public static final String PARAMETER = "checkpointFrequency";

    /**
     * Resolves the configured cadence.
     *
     * @param value    the parameter as written; {@code null} or blank takes {@code fallback}
     * @param fallback the operator's own default, in its own unit
     * @param operator the operator's name, for the message
     * @return the cadence, at least 1
     * @throws StreamException if the value is not a whole number, or is below 1
     */
    public static int of(String value, int fallback, String operator) throws StreamException {
        String text = value == null ? null : value.trim();
        if (text == null || text.isEmpty()) {
            return fallback;
        }
        int frequency;
        try {
            frequency = Integer.parseInt(text);
        } catch (NumberFormatException notANumber) {
            throw new StreamException(operator + " has " + PARAMETER + "=\"" + value + "\"; it must"
                    + " be a whole number of this stage's own units", notANumber, false);
        }
        if (frequency < 1) {
            // Zero would claim a failure costs nothing, which no protocol delivers: a failure always
            // costs at least the unit it was in the middle of. Refused rather than clamped, because
            // a deployer who wrote 0 believed something this cannot give them.
            throw new StreamException(operator + " has " + PARAMETER + "=\"" + value + "\"; it must"
                    + " be at least 1, because a failure always costs at least the unit it was in the"
                    + " middle of", false);
        }
        return frequency;
    }
}
