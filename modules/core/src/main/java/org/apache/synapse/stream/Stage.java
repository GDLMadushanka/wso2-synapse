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
 * What one stage of a run did, as it can be read back.
 *
 * <h2>Read once the run is over, not while it runs</h2>
 * A pull pipeline has no moment at which one stage finishes and the next begins: the sink calls
 * {@code read()} and everything upstream executes inside its {@code consume()}. So every stage ends
 * when the sink does, and all of these rows are written together in the run's {@code finally} block.
 * The table is empty for the whole transfer and complete afterwards — which makes it an accounting
 * record, not a progress feed. Live progress is on the run itself.
 *
 * @param stageName  the stage's configured name, which is also its checkpoint key
 * @param index      its position in the chain, from 0. What orders a rendering of the pipeline
 * @param status     {@code DONE}, or {@code SKIPPED} for a segment a resumed run did not re-execute
 * @param bytesIn    bytes this stage took from the one below it
 * @param bytesOut   bytes it passed to the one above. Equal to {@code bytesIn} for a sink, by
 *                   construction rather than by measurement: a sink writes somewhere the framework
 *                   cannot see, so the only honest figure is what it was handed
 * @param selfNanos  time spent in <b>this</b> stage, with the stages below it subtracted out.
 *                   <b>Wall clock, not CPU</b>: a GC pause, a lock wait, or a service throttling a
 *                   request all land here and read as the stage being slow
 * @param mediationNanos the part of {@code selfNanos} spent inside a deployer-written sequence, or
 *                   {@code -1} where the stage has none. {@code streaming.forEach} is the only
 *                   operator that hands control to one, so it is the only stage that reports this
 * @param updatedAt  when the row was written
 */
public record Stage(String stageName, int index, String status, long bytesIn, long bytesOut,
                    long selfNanos, long mediationNanos, long updatedAt) {

    /** A segment a resumed run inherited from an earlier attempt rather than executing. */
    public static final String SKIPPED = "SKIPPED";

    /**
     * @return whether this stage ran at all in the attempt being described
     */
    public boolean executed() {
        return !SKIPPED.equalsIgnoreCase(status);
    }

    /**
     * @return {@link #selfNanos} as milliseconds, which is the unit a person reads
     */
    public long selfMillis() {
        return selfNanos / 1_000_000L;
    }

    /** @return whether this stage ran deployer-written mediation and timed it */
    public boolean hasMediation() {
        return mediationNanos >= 0L;
    }

    /** @return {@link #mediationNanos} as milliseconds, or {@code -1} when there is none */
    public long mediationMillis() {
        return hasMediation() ? mediationNanos / 1_000_000L : -1L;
    }

    /**
     * The stage's own work: framing, buffering, checkpointing — everything that is not the deployer's
     * sequence.
     *
     * <p>Subtracted rather than measured separately because the two must add up to {@code selfNanos}
     * by construction. Timing both independently would let rounding and an untimed gap between them
     * produce a pair that does not, and a chart drawn from figures that disagree with their own total
     * is worse than one figure.
     *
     * @return the looping time as milliseconds, or {@link #selfMillis()} when there was no mediation
     */
    public long loopingMillis() {
        return hasMediation() ? Math.max(0L, selfNanos - mediationNanos) / 1_000_000L : selfMillis();
    }
}
