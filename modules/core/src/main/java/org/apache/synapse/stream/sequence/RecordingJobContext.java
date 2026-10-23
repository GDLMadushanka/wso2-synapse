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

package org.apache.synapse.stream.sequence;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.stream.JobContext;
import org.apache.synapse.stream.StreamSeed;
import org.apache.synapse.stream.spi.JobStore;
import org.apache.synapse.stream.StreamShutdown;

/**
 * A {@link JobContext} that mirrors everything reported to the caller into the run's {@link JobStore}.
 *
 * <h2>Why a decorator rather than calls at each site</h2>
 * The framework has to record a run whether or not the caller wanted reporting, and the caller has to
 * receive its reports whether or not a store is registered. Those are two obligations over the same
 * events. Writing both at every call site means every future report has to remember both, and the one
 * that gets forgotten is the store — it has no test asking for it, because nobody is watching. Wrapping
 * makes the mirroring structural: an operator calling {@code ctx.job().progress(...)} records a row
 * without knowing this class exists.
 *
 * <h2>Nothing here may throw</h2>
 * {@link JobContext}'s contract already says so, and it matters more here than for a telemetry
 * backend: this one is a database on the transfer's own thread. A store that has gone away must cost
 * the run its <i>record</i>, never its bytes. So every store call is caught and logged, and the
 * delegate is invoked regardless of whether the store succeeded.
 */
final class RecordingJobContext implements JobContext {

    private static final Log log = LogFactory.getLog(RecordingJobContext.class);

    private final JobContext delegate;
    private final JobStore store;
    private final String runId;

    /**
     * Last cumulative figures seen, so the terminal update can carry them. Not synchronised: they are
     * written by the run thread and read by it, and a torn read would cost a wrong byte count on a row
     * rather than anything a transfer depends on.
     */
    private volatile long bytes;
    private volatile long records;

    /** Chain position for the next stage to finish. {@link JobContext} carries no index of its own. */
    private int nextStageIndex;

    /**
     * What each stage reported spending in deployer-supplied mediation, until the stage row is written.
     *
     * <p>Concurrent because the reporting operator's thread and the unwind are not guaranteed to be
     * the same one, and the map is tiny -- one entry per stage that has any mediation at all, which
     * today means one entry at most.
     */
    private final java.util.Map<String, Long> mediationNanos =
            new java.util.concurrent.ConcurrentHashMap<>();

    /** Guards against recording two terminal states, which a failure during commit could otherwise do. */
    private boolean terminal;

    /**
     * Whether a server shutdown should stop this run, which is true only if something would redo it.
     *
     * <p>A resumable run stops at its next unit and a retry continues from the checkpoint. A
     * non-resumable one is left to finish: stopping an API request body destroys bytes nothing can
     * obtain again, so it gets the same contract as any other in-flight mediation. See ADR-0037.
     */
    private final boolean stopsOnShutdown;

    /**
     * What the source said its size was, remembered so {@link #expectedSize()} can answer with it.
     * Written once during the build, before any stage runs, and read by stages thereafter.
     */
    private volatile long sourceSize = StreamSeed.UNKNOWN;

    RecordingJobContext(JobContext delegate, JobStore store, String runId) {
        this(delegate, store, runId, false);
    }

    RecordingJobContext(JobContext delegate, JobStore store, String runId, boolean stopsOnShutdown) {
        this.stopsOnShutdown = stopsOnShutdown;
        this.delegate = delegate;
        this.store = store;
        this.runId = runId;
    }

    // ---------------------------------------------------------------- mirrored

    @Override
    public void progress(String stage, long bytes, long records) {
        this.bytes = bytes;
        this.records = records;
        try {
            store.progress(runId, stage, bytes, records);
        } catch (RuntimeException e) {
            quietly("progress", e);
        }
        delegate.progress(stage, bytes, records);
    }

    @Override
    public void status(String stage, int index, String activity) {
        if (store != null && runId != null) {
            try {
                store.status(runId, stage, index, activity, System.currentTimeMillis());
            } catch (RuntimeException e) {
                quietly("status", e);
            }
        }
        delegate.status(stage, index, activity);
    }

    @Override
    public void stageFinished(String stage, long bytesIn, long bytesOut, long selfNanos) {
        try {
            // The mediation figure arrives separately, from the operator, while the run is still
            // going -- and the stage row is written here, at the end. Folded in rather than written
            // on its own so the row is created once: a separate write would either be clobbered by
            // this one or have to create the row itself, and then two writers would own it.
            store.stageFinished(runId, stage, nextStageIndex++, bytesIn, bytesOut, selfNanos,
                    mediationNanos.getOrDefault(stage, -1L));
        } catch (RuntimeException e) {
            quietly("stageFinished", e);
        }
        delegate.stageFinished(stage, bytesIn, bytesOut, selfNanos);
    }

    @Override
    public void stageMediation(String stage, long nanos) {
        // Highest wins, never last. The figure is cumulative and arrives repeatedly, so a put would
        // let any out-of-order or late zero erase a real total -- the same way a plain assignment let
        // the sink's "no records" erase a transform's record count.
        mediationNanos.merge(stage, nanos, Math::max);
        delegate.stageMediation(stage, nanos);
    }

    /**
     * Records that a stage was skipped because a previous attempt had finished it.
     *
     * <p>Framework-called, and deliberately not forwarded to the delegate: a caller's
     * {@link JobContext} reports what <i>happened</i>, and nothing happened here. It also does not
     * advance the stage index the same way a finished stage does — a skipped stage still occupies its
     * position in the chain.
     */
    void recordSkipped(String stage) {
        try {
            store.stageSkipped(runId, stage, nextStageIndex++);
        } catch (RuntimeException e) {
            quietly("stageSkipped", e);
        }
    }

    /**
     * Reports a failure to the caller. <b>Does not write the job row</b> — see
     * {@link #recordFailure}, which the framework calls with the verdict this method cannot carry.
     */
    @Override
    public void failed(String stage, String code, String message) {
        delegate.failed(stage, code, message);
    }

    /**
     * Records that this run stopped on request, and reports whether it did.
     *
     * @param stateDiscarded whether the run threw away what it had earned
     */
    void recordStopped(boolean stateDiscarded) {
        try {
            store.stopped(runId, stateDiscarded);
        } catch (RuntimeException e) {
            quietly("stopped", e);
        }
    }

     /**
     * Records a terminal failure, including whether another attempt could plausibly succeed.
     *
     * <p>Separate from {@link #failed} because {@link JobContext} has no parameter for the verdict, and
     * the previous version therefore stored a hardcoded {@code true} for every failure. That made
     * {@code MFT_JOB.RETRYABLE} a column of constants — and the consumer is a processor's re-queue
     * decision, so a permanently corrupt archive or a wrong decryption key would be retried forever,
     * failing identically each time. {@code StreamException} computes the verdict; this is how it
     * travels.
     *
     * <p>Also delegates to the caller, so exactly one call reports and records.
     */
    void recordFailure(String stage, String code, String message, boolean retryable) {
        if (!terminal) {
            terminal = true;
            try {
                store.failed(runId, stage, code, message, retryable);
            } catch (RuntimeException e) {
                quietly("failed", e);
            }
        }
        delegate.failed(stage, code, message);
    }

    /**
     * Records that the run completed. Not part of {@link JobContext} — success is the framework's
     * observation, not something a caller is told through this channel.
     */
    void succeeded() {
        if (terminal) {
            return;
        }
        terminal = true;
        try {
            store.succeeded(runId, bytes, records);
        } catch (RuntimeException e) {
            quietly("succeeded", e);
        }
    }

    // ---------------------------------------------------------------- pass-through

    @Override
    public String jobId() {
        return delegate.jobId();
    }

    @Override
    public String invokerType() {
        return delegate.invokerType();
    }

    @Override
    public String invokerName() {
        return delegate.invokerName();
    }

    @Override
    public Long expectedSize() {
        // The caller's own answer wins: a submitter that declared a Content-Length knows something the
        // probe does not. Otherwise fall back to what the source reported when it was identified.
        //
        // These are the same quantity -- sourceOpened's size IS the total expected byte count -- but
        // they were never connected, so a sequence that opened its own source answered null here for
        // every stage. An operator that sizes its work from the source could not, and silently used a
        // default instead.
        Long declared = delegate.expectedSize();
        if (declared != null) {
            return declared;
        }
        long probed = sourceSize;
        return probed == StreamSeed.UNKNOWN || probed < 0 ? null : probed;
    }

    @Override
    public void sourceOpened(long size, long lastModified) {
        this.sourceSize = size;
        delegate.sourceOpened(size, lastModified);
    }

    @Override
    public void checksum(String stage, String algorithm, String value) {
        delegate.checksum(stage, algorithm, value);
    }

    /**
     * Forwarded, and it has to be: this class wraps the caller's context and answers for it, so a
     * question it does not forward silently returns {@link JobContext}'s default. That default is
     * {@code false} -- keep the state -- which turned every stop into a pause.
     */
    @Override
    public boolean discardStateOnStop() {
        return delegate.discardStateOnStop();
    }

    @Override
    public boolean isCancelled() {
        // The shutdown signal reaches an operator through the JobContext it already holds, so no
        // caller has to be taught to pass anything new -- this decorator wraps every run.
        return delegate.isCancelled() || (stopsOnShutdown && StreamShutdown.requested());
    }

    private void quietly(String what, RuntimeException e) {
        log.warn("could not record " + what + " for run '" + runId + "'; the transfer is unaffected"
                + " but its job record is now incomplete", e);
    }
}
