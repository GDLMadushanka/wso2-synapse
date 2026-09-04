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
 * The pipeline's channel back to whoever asked for the transfer.
 * <p>
 * Every method has a default, so {@link #NOOP} is a complete implementation and an operator never
 * has to null-check {@code ctx.job()}.
 *
 * <h2>Rules for implementations</h2>
 * <ul>
 *   <li><b>No reporting method may throw.</b> A telemetry backend being down must not fail a
 *       transfer. Catch and log internally.</li>
 *   <li><b>Must be thread-safe.</b> {@link #progress} is called from the run thread while
 *       {@link #isCancelled()} may be polled from a control thread.</li>
 *   <li><b>{@link #expectedSize()} may return {@code null}</b>, and usually does for a compressed
 *       or generated source. Any consumer computing a percentage must handle that; it is the most
 *       likely defect against this interface.</li>
 * </ul>
 */
public interface JobContext {

    /**
     * A complete no-op implementation, used when a pipeline is invoked without a caller that
     * wants reporting — a sequence-invoked transfer with no job, for instance.
     */
    JobContext NOOP = new JobContext() {
    };

    /**
     * The job id {@link #NOOP} reports.
     * <p>
     * Named so that it can be rejected. Every NOOP run shares it, so a pipeline that keeps durable
     * state cannot use it to name a workspace — two concurrent runs would collide, and a resume would
     * adopt another run's artifacts. The framework therefore never uses this id to name a
     * workspace unless it is real: it resolves a run id itself, falling back to the seed's identity
     * and then to a fresh id. A caller is never asked for one — see ADR-0020.
     */
    String NOOP_JOB_ID = "noop";

    /**
     * Correlation id for this transfer, when the caller has one.
     * <p>
     * <b>Optional.</b> Returning {@link #NOOP_JOB_ID} is a legitimate answer and the framework names
     * the run itself; it is never an error to have no job id. A caller that <i>does</i> supply one —
     * a queued transfer, whose id is also what the submitter polls with — owes two properties that
     * pull against each other: <b>unique across concurrent transfers</b>, since a collision means two
     * runs sharing artifacts; and <b>stable across retries of the same logical transfer</b>, since
     * otherwise a retry resumes from nothing. A fresh random id per attempt satisfies the first and
     * destroys the second.
     *
     * @return a stable, path-safe identifier, or {@link #NOOP_JOB_ID}; never {@code null}
     */
    default String jobId() {
        return NOOP_JOB_ID;
    }

    /**
     * Overrides the invoker's <b>kind</b>, when the framework cannot work it out for itself.
     *
     * <p><b>Return {@code null} unless you are certain.</b> The framework derives this from the message
     * context — Synapse stamps the invoking proxy, API or inbound endpoint onto it — and a derived value
     * cannot be forgotten, which is the whole point. An earlier version of this method asked every
     * caller to declare the answer, no caller did, and the column held one value forever.
     *
     * <p>The case that genuinely needs it is a <b>message processor</b>, whose message context is
     * synthesised and carries none of those properties. That is a framework component, so the
     * declaration cannot be omitted by a user.
     *
     * @return {@code API}, {@code INBOUND}, {@code PROXY}, {@code PROCESSOR}, or {@code null} to derive
     * @see #invokerName()
     */
    default String invokerType() {
        return null;
    }

    /**
     * Overrides the invoker's <b>name</b>. Same rule as {@link #invokerType()}: {@code null} to derive.
     *
     * @return the invoking artifact's name, or {@code null} to derive
     */
    default String invokerName() {
        return null;
    }

    /**
     * Total expected byte count, or {@code null} when unknown.
     *
     * @return the expected size, or {@code null} — see the class javadoc
     */
    default Long expectedSize() {
        return null;
    }

    /**
     * The source's stream has been obtained and the transfer has begun. No bytes have moved yet.
     *
     * @param size         source byte length, or {@link StreamSeed#UNKNOWN}
     * @param lastModified source modification time in epoch millis, or {@link StreamSeed#UNKNOWN}
     */
    default void sourceOpened(long size, long lastModified) {
    }

    /**
     * Cumulative progress for the run — <b>not</b> deltas, and monotonically non-decreasing.
     * <p>
     * Callers must rate-limit; per-byte or per-small-read reporting is not conformant. {@code bytes}
     * counts bytes delivered by the sink rather than read by the source, which differ wherever a
     * transform changes volume. {@code records} is {@code 0} where the pipeline has no record
     * semantics — a byte chain does not know what a record is.
     *
     * @param stage   the reporting stage's name
     * @param bytes   cumulative bytes delivered
     * @param records cumulative records processed, or {@code 0}
     */
    default void progress(String stage, long bytes, long records) {
    }

    /**
     * One stage has finished, with its final measurements.
     * <p>
     * {@code selfNanos} is <b>wall clock, not CPU</b>. A GC pause landing in this stage's read, a
     * lock wait, or a decorator that buffers and returns instantly on most calls will all skew
     * it. It reliably identifies <i>which</i> stage to look at and only weakly estimates how much
     * a fix would save; any surface displaying it should say so.
     *
     * @param stage     the stage's name
     * @param bytesIn   bytes this stage consumed
     * @param bytesOut  bytes this stage produced
     * @param selfNanos time spent in this stage's own work, excluding upstream wait
     */
    default void stageFinished(String stage, long bytesIn, long bytesOut, long selfNanos) {
    }

    /**
     * A digest an operator computed in passing. The framework computes none itself.
     *
     * @param stage     the stage that computed it
     * @param algorithm the digest algorithm; a bare value without one is not usable
     * @param value     the digest
     */
    default void checksum(String stage, String algorithm, String value) {
    }

    /**
     * Terminal failure. Called once per failed run, before the exception propagates.
     * <p>
     * {@code stage} is the attributed stage and never a guess. {@code code} must be a stable
     * machine-readable token suitable for alert routing, not free or localised text. {@code message}
     * must not contain a credential, a URI with userinfo, or payload bytes.
     *
     * @param stage   the stage that failed
     * @param code    stable machine-readable failure code
     * @param message human-readable detail
     */
    default void failed(String stage, String code, String message) {
    }

    /**
     * Whether the caller has asked for this transfer to stop.
     * <p>
     * Cooperative only — the pipeline does not interrupt the run thread. Must be cheap, since it
     * is polled in a read loop: no I/O, no contended lock. Once {@code true}, must stay
     * {@code true}.
     *
     * @return {@code true} if cancellation has been requested
     */
    default boolean isCancelled() {
        return false;
    }
}
