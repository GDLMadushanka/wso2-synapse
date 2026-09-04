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
 * Where a run's job record lives — the durable answer to "what is running, and what ran".
 *
 * <h2>Not the same thing as {@link JobContext}</h2>
 * They carry almost the same data and exist for opposite reasons, which is worth stating because
 * conflating them is the obvious mistake. {@code JobContext} is the <b>caller's</b> channel: it exists
 * only if someone asked for reporting, and a run invoked from a sequence with no job legitimately has
 * {@link JobContext#NOOP}. This is the <b>framework's</b> record: every run gets one, whether or not
 * anyone is listening, because a partial answer to "what is running" is worse than none.
 *
 * <p>So the pipeline writes to both, and neither substitutes for the other. In practice the framework
 * mirrors the caller's reporting calls into this store, which is why the method shapes line up.
 *
 * <h2>Rules for implementations</h2>
 * <ul>
 *   <li><b>Only {@link #start} may throw.</b> A run that cannot be recorded cannot be recovered or
 *       reported on, so failing there is honest. Every other method runs while a transfer is in
 *       flight, and a telemetry write must never be the thing that fails it — catch and log.</li>
 *   <li><b>{@link #start} must be idempotent on {@code runId}.</b> A retry of the same logical
 *       transfer deliberately reuses its id; the second call updates rather than inserts, and the
 *       attempt count is the store's to increment.</li>
 *   <li><b>Must be thread-safe.</b> One store serves concurrent runs of the same pipeline.</li>
 * </ul>
 */
public interface JobStore {

    /**
     * A complete no-op implementation, used when no provider is registered.
     *
     * <p>Absent is a legitimate state here, unlike a checkpoint store: plain Synapse has no datasource,
     * and a pipeline that keeps no durable state runs correctly with nothing recorded. What is lost is
     * only the ability to look the run up afterwards.
     */
    JobStore NOOP = new JobStore() {
    };

    /**
     * Records that a run has begun, and reports what a previous attempt left.
     *
     * <p>Called once, before any bytes move. Inserts a row for a first attempt; for a retry — same
     * {@code runId}, which is exactly what makes resume possible — updates the existing row and
     * increments its attempt count.
     *
     * @param run what is known about this run at its start
     * @return the previous attempt's record, or {@code null} if this is the first
     * @throws StreamException if the run could not be recorded
     */
    default JobRecord start(JobRun run) throws StreamException {
        return null;
    }

    /**
     * Cumulative progress. Mirrors {@link JobContext#progress}, including its rate-limiting
     * obligation — this writes to a database and must not be called per read.
     *
     * @param runId   the run
     * @param stage   the reporting stage
     * @param bytes   cumulative bytes delivered
     * @param records cumulative records processed, or {@code 0}
     */
    default void progress(String runId, String stage, long bytes, long records) {
    }

    /**
     * One stage finished, with its final measurements.
     *
     * @param runId     the run
     * @param stage     the stage's name — the key, together with {@code runId}
     * @param index     position in the chain, for display ordering only
     * @param bytesIn   bytes this stage consumed
     * @param bytesOut  bytes this stage produced
     * @param selfNanos wall-clock time in this stage's own work
     */
    default void stageFinished(String runId, String stage, int index, long bytesIn, long bytesOut,
                               long selfNanos) {
    }

    /**
     * A stage did no work this attempt, because a previous one had already finished it.
     *
     * <p>Written when a run resumes from a complete artifact: everything upstream of that stage was
     * constructed and never read, so reporting it as finished would be wrong and reporting nothing
     * leaves a resumed run's per-stage table sparse with no explanation.
     *
     * <p>Separate from {@link #stageFinished} rather than a status parameter on it, because a status
     * would have to be threaded through {@link JobContext} — the <b>caller's</b> channel, which has no
     * interest in this. The distinction is framework-to-store only.
     *
     * @param runId the run
     * @param stage the stage's name — the key, together with {@code runId}
     * @param index position in the chain, for display ordering only
     */
    default void stageSkipped(String runId, String stage, int index) {
    }

    /**
     * The run completed and everything it produced was committed.
     *
     * @param runId   the run
     * @param bytes   final cumulative bytes
     * @param records final cumulative records, or {@code 0}
     */
    default void succeeded(String runId, long bytes, long records) {
    }

    /**
     * The run failed terminally for this attempt.
     *
     * @param runId     the run
     * @param stage     the attributed stage
     * @param code      stable machine-readable failure code
     * @param message   human-readable detail, free of credentials and payload bytes
     * @param retryable whether another attempt could plausibly succeed
     */
    default void failed(String runId, String stage, String code, String message, boolean retryable) {
    }
}
