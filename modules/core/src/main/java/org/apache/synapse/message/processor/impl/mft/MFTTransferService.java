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

package org.apache.synapse.message.processor.impl.mft;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.ManagedLifecycle;
import org.apache.synapse.MessageContext;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.message.processor.MessageProcessor;
import org.apache.synapse.stream.JobContext;
import org.apache.synapse.stream.spi.Invoker;
import org.apache.synapse.stream.StreamException;
import org.apache.synapse.stream.transfer.Transfer;
import org.apache.synapse.stream.transfer.TransferStatus;
import org.apache.synapse.stream.transfer.TransferStore;
import org.apache.synapse.stream.transfer.TransferStores;
import org.apache.synapse.stream.sequence.StreamSequence;
import org.apache.synapse.task.Task;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Claims queued transfers and runs them.
 *
 * <h2>One poll, many transfers</h2>
 * The scheduled fire claims at most as many transfers as the pool has free slots and submits each; it
 * never runs one inline. A transfer is minutes or hours of work, so a polling thread that waited for one
 * would stop serving every other transfer in the queue. This is {@code SamplingService}'s
 * {@code concurrency}, for the same reason it has one.
 *
 * <h2>What it does with a claimed transfer</h2>
 * Builds a message, sets the descriptor's parameters on it as {@code mft.}-prefixed properties, and
 * executes the sequence the descriptor names (ADR-0041). The message carries no payload and is not
 * mediated — it exists because a connector operation reads its configuration from the message it is bound
 * against (ADR-0026), so {@code file.streamRead} has nowhere to read {@code path} from without one.
 *
 * <h2>Outcomes</h2>
 * <ul>
 *   <li><b>Success</b> — the transfer row is moved to {@code DONE} here. It is a separate row from
 *       the run's own job record (ADR-0044), so finishing the run does not finish the submission;
 *       this is the acknowledgement, and failing to write it leaves the transfer looking claimed
 *       until this node's next startup sweep.</li>
 *   <li><b>Retryable failure</b> — requeued with the same job id, so the next attempt resumes from the
 *       checkpoint the last one left, and not claimable again until {@code retry.interval} has passed.</li>
 *   <li><b>Non-retryable failure</b> — left {@code FAILED}. An unparseable file or a rejected
 *       configuration fails identically every time; requeueing it would burn the queue forever.</li>
 * </ul>
 */
public class MFTTransferService implements Task, ManagedLifecycle {

    private static final Log log = LogFactory.getLog(MFTTransferService.class);

    /** Every descriptor parameter reaches the sequence under this prefix. */
    static final String PARAMETER_PREFIX = "mft.";

    // The hook parameters. The deactivate one keeps the name ForwardingService already uses, because an
    // operator who has configured one processor should not have to learn a second spelling.
    static final String SUCCESS_SEQUENCE = "sequence";
    static final String FAULT_SEQUENCE = "fault.sequence";
    static final String DEACTIVATE_SEQUENCE = "message.processor.deactivate.sequence";
    static final String MAX_DELIVERY_DROP = "max.delivery.drop";
    static final String DEACTIVATE_AFTER = "deactivate.after";
    static final String MEMBER_COUNT = "member.count";

    /**
     * The sequence this processor invokes. Required, and deliberately here rather than in a queue row:
     * the processor is what runs a transfer, exactly as a forwarding processor owns its endpoint. Two
     * processors on one store that disagreed about it would be draining the same queue into different
     * work (ADR-0043).
     */
    static final String SEQUENCE_PARAMETER = "streamSequence";

    // What a hook is told about the transfer it is reacting to.
    private static final String JOB_ID = PARAMETER_PREFIX + "jobId";
    private static final String SEQUENCE_PROPERTY = PARAMETER_PREFIX + "sequence";
    private static final String RUN_ID = PARAMETER_PREFIX + "runId";
    private static final String STATUS = PARAMETER_PREFIX + "status";
    private static final String ATTEMPT = PARAMETER_PREFIX + "attempt";
    private static final String ERROR_CODE = PARAMETER_PREFIX + "error.code";
    private static final String ERROR_MESSAGE = PARAMETER_PREFIX + "error.message";
    private static final String ERROR_STAGE = PARAMETER_PREFIX + "error.stage";
    private static final String RETRYABLE = PARAMETER_PREFIX + "retryable";
    private static final String DEACTIVATED_REASON = PARAMETER_PREFIX + "deactivated.reason";

    private final MessageProcessor messageProcessor;
    private final SynapseEnvironment synapseEnvironment;

    private final String concurrencyPropName;
    private final String maxAttemptsPropName;
    private final String retryIntervalPropName;

    private int concurrency = 1;
    private int maxDeliveryAttempts = 4;
    private long retryInterval = 1000L;

    /** Give a transfer up and keep going, rather than stopping the processor. See ADR-0042. */
    private boolean dropOnExhaustion = true;

    /** Consecutive give-ups that stop the processor. Zero means never. */
    private int deactivateAfter = 10;

    private String sequenceName;
    private String successSequence;
    private String faultSequence;
    private String deactivateSequence;

    /**
     * Transfers given up in a row, across different transfers.
     *
     * <p>The pattern is the signal: one unparseable file fails on its own, while a dead destination or a
     * rotated credential fails every transfer identically. Reset by any success.
     */
    private final AtomicInteger consecutiveGiveUps = new AtomicInteger();

    private ExecutorService workers;
    private final AtomicInteger running = new AtomicInteger();

    private volatile boolean initialized;
    private volatile boolean terminated;

    public MFTTransferService(MessageProcessor messageProcessor, SynapseEnvironment synapseEnvironment,
                              String concurrencyPropName, String maxAttemptsPropName,
                              String retryIntervalPropName) {
        this.messageProcessor = messageProcessor;
        this.synapseEnvironment = synapseEnvironment;
        this.concurrencyPropName = concurrencyPropName;
        this.maxAttemptsPropName = maxAttemptsPropName;
        this.retryIntervalPropName = retryIntervalPropName;
    }

    @Override
    public void init(SynapseEnvironment se) {
        Map<String, Object> parameters = messageProcessor.getParameters();
        concurrency = readPositiveInt(parameters, concurrencyPropName, concurrency);
        maxDeliveryAttempts = readPositiveInt(parameters, maxAttemptsPropName, maxDeliveryAttempts);
        retryInterval = readPositiveInt(parameters, retryIntervalPropName, (int) retryInterval);
        deactivateAfter = readCount(parameters, DEACTIVATE_AFTER, deactivateAfter);
        dropOnExhaustion = !"Disabled".equalsIgnoreCase(text(parameters, MAX_DELIVERY_DROP, "Enabled"));
        sequenceName = text(parameters, SEQUENCE_PARAMETER, null);
        if (sequenceName == null) {
            // Not fatal at init -- the processor is already deployed and a throw here would leave it
            // half-started -- but nothing can run, so this must be impossible to miss in a log.
            log.error("MFT message processor '" + messageProcessor.getName() + "' has no '"
                    + SEQUENCE_PARAMETER + "' parameter, so it does not know what to run. No transfer"
                    + " will be claimed from message store '" + messageProcessor.getMessageStoreName()
                    + "' until one is configured");
        }
        successSequence = text(parameters, SUCCESS_SEQUENCE, null);
        faultSequence = text(parameters, FAULT_SEQUENCE, null);
        deactivateSequence = text(parameters, DEACTIVATE_SEQUENCE, null);

        int members = readPositiveInt(parameters, MEMBER_COUNT, 1);
        if (members > 1) {
            // Not an error, but rarely what someone means on one node: each slot is a task with its own
            // pool, so the node runs members x concurrency transfers rather than concurrency.
            log.warn("MFT message processor '" + messageProcessor.getName() + "' has " + MEMBER_COUNT
                    + "=" + members + " and " + concurrencyPropName + "=" + concurrency + ", so this node"
                    + " may run up to " + (members * concurrency) + " transfers at once. " + MEMBER_COUNT
                    + " spreads task slots across a cluster; " + concurrencyPropName + " is the per-slot"
                    + " bound");
        }

        // Bounded, and bounded by the same number the claim loop uses: an unbounded pool would let a
        // slow destination accumulate transfers until the node runs out of file handles, with the queue
        // looking empty the whole time.
        workers = new ThreadPoolExecutor(concurrency, concurrency, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), runnable -> {
            Thread t = new Thread(runnable, "MFT-" + messageProcessor.getName() + "-transfer");
            t.setDaemon(true);
            return t;
        });
        initialized = true;
        log.info("MFT message processor '" + messageProcessor.getName() + "' will run up to "
                + concurrency + " transfer(s) at a time from message store '"
                + messageProcessor.getMessageStoreName() + "'");
    }

    @Override
    public void execute() {
        if (terminated || messageProcessor.isDeactivated()) {
            return;
        }
        if (!initialized) {
            init(synapseEnvironment);
        }

        if (sequenceName == null) {
            return;         // already reported at init; claiming would strand transfers as CLAIMED
        }
        TransferStore store = TransferStores.store();
        if (store == null) {
            // Deployment already refused a store with no datasource, so reaching here means one was
            // withdrawn under a running processor. Nothing to claim, and nothing this can fix.
            log.error("MFT message processor '" + messageProcessor.getName() + "' has no transfer store;"
                    + " no transfer will run until the MFT datasource is available again");
            return;
        }

        String storeName = messageProcessor.getMessageStoreName();
        int free = concurrency - running.get();
        for (int i = 0; i < free && !terminated; i++) {
            Transfer transfer;
            try {
                transfer = store.claim(storeName);
            } catch (StreamException e) {
                // The datasource is unreachable. Logged once per fire rather than per slot, and the
                // next fire tries again -- this is exactly the case the scheduled interval exists for.
                log.error("MFT message processor '" + messageProcessor.getName() + "' could not claim a"
                        + " transfer; it will try again on its next poll", e);
                return;
            }
            if (transfer == null) {
                return;                 // nothing waiting
            }
            running.incrementAndGet();
            try {
                workers.execute(() -> {
                    try {
                        run(store, transfer);
                    } finally {
                        running.decrementAndGet();
                    }
                });
            } catch (RuntimeException rejected) {
                // The pool refused it, so nothing will run it. Put it back rather than leaving a
                // CLAIMED row nothing owns.
                running.decrementAndGet();
                requeue(store, transfer, System.currentTimeMillis());
                log.error("MFT message processor '" + messageProcessor.getName() + "' could not start"
                        + " transfer " + transfer.jobId() + "; it was returned to the queue", rejected);
                return;
            }
        }
    }

    /**
     * Runs one claimed transfer, and decides what its outcome means for the queue.
     *
     * <p>Runs on a worker thread, so it must not throw: an escaping exception would kill the thread and
     * leave the row {@code CLAIMED} with nothing running it.
     */
    private void run(TransferStore store, Transfer transfer) {
        StreamSequence sequence = synapseEnvironment.getSynapseConfiguration()
                .getStreamSequence(sequenceName);
        if (sequence == null) {
            // Recoverable rather than corrupt: deploy the sequence and the retry runs. Failing the
            // transfer would make a deployment-ordering mistake permanent.
            log.error("MFT message processor '" + messageProcessor.getName() + "' is configured to run"
                    + " stream sequence '" + sequenceName + "', which is not deployed. Transfer '"
                    + transfer.jobId() + "' goes back to the queue");
            requeue(store, transfer, System.currentTimeMillis() + retryInterval);
            return;
        }

        MessageContext msg = synapseEnvironment.createMessageContext();
        for (Map.Entry<String, String> parameter : transfer.parameters().entrySet()) {
            msg.setProperty(PARAMETER_PREFIX + parameter.getKey(), parameter.getValue());
        }

        // Written before the run so that a node dying mid-transfer still leaves the two rows joined:
        // the run id is derived from the job id, so it is knowable here, and a link written afterwards
        // would be missing for exactly the transfers someone needs to investigate.
        linkRun(store, transfer);

        try {
            // No seed: a queued flow cannot carry one, which is why its sequence opens its own source.
            // The job id names the run, so an attempt lands in the workspace the last one left.
            TransferJobContext jobCtx = new TransferJobContext(transfer.runName(),
                    invocationPath(transfer), store, transfer.jobId());
            sequence.execute(msg, jobCtx, null);
            log.info("transfer " + transfer.jobId() + " completed through stream sequence '"
                    + sequenceName + "'");
            consecutiveGiveUps.set(0);
            finish(store, transfer, TransferStatus.DONE, null, null);
            describe(msg, transfer, TransferStatus.DONE.name());
            mediate(successSequence, msg, transfer, SUCCESS_SEQUENCE);
        } catch (StreamException e) {
            // Asked to stop takes precedence over everything the failure path would otherwise do. A
            // stopped run throws RETRYABLE -- correct for a shutdown, which wants the transfer back --
            // so without this check the processor would helpfully requeue the transfer someone had
            // just asked it to stop, and the request would look ignored.
            if (stoppedOnRequest(store, transfer)) {
                return;
            }
            afterFailure(store, transfer, msg, e);
        } catch (RuntimeException | Error e) {
            // Including Error: the sequence rethrows those unwrapped, and a worker thread that dies
            // silently leaves a CLAIMED row nothing will ever pick up.
            log.error("transfer " + transfer.jobId() + " failed unexpectedly through stream sequence '"
                    + sequenceName + "'", e);
            requeue(store, transfer, System.currentTimeMillis() + retryInterval);
        }
    }

    /**
     * Records that a run wound down because it was asked to, and reports whether it did.
     *
     * <p>Called before the failure path, and the ordering is the point: the row's status is the
     * request, so it is also the answer to "was this a failure or a deliberate stop". The run itself
     * writes the resting state, because only the thread that unwound knows it has finished doing so.
     *
     * @return {@code true} when the transfer was pausing or stopping and has now been recorded as
     *         paused or cancelled, so the caller must not treat the exception as a failure
     */
    private boolean stoppedOnRequest(TransferStore store, Transfer transfer) {
        TransferStatus resting;
        try {
            resting = store.stopped(transfer.jobId());
        } catch (StreamException e) {
            // The run has stopped either way. Reporting it as a failure instead would requeue it,
            // which for a pause is what the operator asked for anyway and for a stop is wrong -- but
            // an unreachable store is not something this thread can fix, and the startup sweep will
            // find the row.
            log.error("transfer " + transfer.jobId() + " stopped on request but the store could not be"
                    + " updated; it will be reconciled by a sweep", e);
            return false;
        }
        if (resting == null) {
            return false;               // nobody asked: an ordinary failure
        }
        consecutiveGiveUps.set(0);      // a deliberate stop is not a sign the destination is broken
        log.info("transfer " + transfer.jobId() + " stopped on request and is now "
                + resting.name().toLowerCase(java.util.Locale.ENGLISH)
                + (resting == TransferStatus.PAUSED
                        ? "; its workspace and checkpoints are kept, so start resumes from there"
                        : "; its workspace and checkpoints were discarded"));
        return true;
    }

    private void afterFailure(TransferStore store, Transfer transfer, MessageContext msg,
                              StreamException failure) {
        if (!failure.isRetryable()) {
            // Poison. The row stays FAILED with its diagnosis, which is what a reader of the job table
            // needs; retrying would produce the same failure on every attempt for ever.
            log.error("transfer " + transfer.jobId() + " failed at stage '" + failure.getStage()
                    + "' in a way that will not succeed on a retry, so it is not requeued", failure);
            giveUp(store, transfer, msg, failure);
            return;
        }
        if (maxDeliveryAttempts > 0 && transfer.attempt() >= maxDeliveryAttempts) {
            log.error("transfer " + transfer.jobId() + " has failed " + maxDeliveryAttempts
                    + " time(s) and will not be retried again. Its workspace and checkpoints are kept,"
                    + " so resubmitting the same job id resumes rather than starting over", failure);
            giveUp(store, transfer, msg, failure);
            return;
        }
        // Retried, so not given up: no fault hook, and the consecutive count is untouched. Four
        // retries producing four alert emails is how an alerting channel gets muted.
        log.warn("transfer " + transfer.jobId() + " failed at stage '" + failure.getStage()
                + "' and will be retried in " + retryInterval + " ms, resuming from its last"
                + " checkpoint: " + failure.getMessage());
        requeue(store, transfer, System.currentTimeMillis() + retryInterval);
    }

    /**
     * A transfer nobody will retry: run the fault hook, then decide whether the <i>pattern</i> of
     * failures means this processor should stop.
     */
    private void giveUp(TransferStore store, Transfer transfer, MessageContext msg,
                        StreamException failure) {
        String code = failure.isRetryable() ? "STREAM_SEQUENCE_EXHAUSTED" : "STREAM_SEQUENCE_FAILED";
        finish(store, transfer, TransferStatus.FAILED, code, withRootCause(failure));
        describe(msg, transfer, TransferStatus.FAILED.name());
        msg.setProperty(ERROR_CODE, code);
        msg.setProperty(ERROR_MESSAGE, failure.getMessage());
        msg.setProperty(ERROR_STAGE, failure.getStage());
        msg.setProperty(RETRYABLE, String.valueOf(failure.isRetryable()));
        mediate(faultSequence, msg, transfer, FAULT_SEQUENCE);

        int inARow = consecutiveGiveUps.incrementAndGet();
        if (!dropOnExhaustion) {
            // ForwardingService's behaviour, available for anyone who wants it: one transfer that
            // cannot be delivered stops the processor.
            deactivate(msg, transfer, "a transfer was given up and " + MAX_DELIVERY_DROP
                    + " is Disabled");
            return;
        }
        if (deactivateAfter > 0 && inARow >= deactivateAfter) {
            deactivate(msg, transfer, inARow + " transfers in a row were given up, which is what a"
                    + " destination that is down looks like rather than a bad file");
        }
    }

    /**
     * Stops this processor and says so.
     *
     * <p>Deliberately not self-healing. Whatever failed ten transfers in a row is still broken, and a
     * processor that quietly resumed would hide the outage while emptying the queue into {@code FAILED}.
     */
    private void deactivate(MessageContext msg, Transfer transfer, String reason) {
        log.error("MFT message processor '" + messageProcessor.getName() + "' is deactivating itself: "
                + reason + ". Queued transfers stay queued; activate it again once the cause is fixed");
        msg.setProperty(DEACTIVATED_REASON, reason);
        try {
            messageProcessor.deactivate();
        } catch (RuntimeException e) {
            log.error("MFT message processor '" + messageProcessor.getName() + "' could not deactivate"
                    + " itself; it will keep claiming transfers", e);
        }
        mediate(deactivateSequence, msg, transfer, DEACTIVATE_SEQUENCE);
    }

    /**
     * The failure, with the reason underneath it.
     *
     * <p>A sequence's own message names the sequence and nothing else -- "stream sequence 'x' failed" --
     * because the stage that failed is attributed separately. That is fine in a log, where the stack
     * trace follows, and useless on a job record read over HTTP by someone who cannot see the log. So
     * the root cause is appended, which is where "file not found on connection 'conn1'" lives.
     */
    private static String withRootCause(StreamException failure) {
        Throwable root = failure;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String reason = root.getMessage();
        if (reason == null || reason.equals(failure.getMessage())) {
            return failure.getMessage();
        }
        String stage = failure.getStage();
        return failure.getMessage() + (stage == null ? "" : " at stage '" + stage + "'") + ": " + reason;
    }

    /** What every hook is told about the transfer it is reacting to. */
    private void describe(MessageContext msg, Transfer transfer, String status) {
        msg.setProperty(JOB_ID, String.valueOf(transfer.jobId()));
        msg.setProperty(SEQUENCE_PROPERTY, sequenceName);
        msg.setProperty(RUN_ID, transfer.runName());
        msg.setProperty(STATUS, status);
        // Not +1: the claim already counted this attempt, and the store hands back the incremented
        // value. Adding one here reported "attempt 2" for every first run, which is exactly the kind of
        // number someone would trust in a support ticket.
        msg.setProperty(ATTEMPT, String.valueOf(transfer.attempt()));
    }

    /**
     * Runs a hook sequence, if one is configured and deployed.
     *
     * <p>Never changes the transfer's outcome. A transfer that moved its bytes has succeeded whatever
     * the notification did afterwards, and retrying a completed transfer because an SMTP server was
     * down is a worse failure than a missed email. ADR-0042.
     */
    private void mediate(String sequenceName, MessageContext msg, Transfer transfer,
                         String parameterName) {
        if (sequenceName == null) {
            return;
        }
        try {
            org.apache.synapse.Mediator sequence = msg.getSequence(sequenceName);
            if (sequence == null) {
                log.warn("MFT message processor '" + messageProcessor.getName() + "' names '"
                        + sequenceName + "' as its " + parameterName + ", which is not deployed;"
                        + " transfer '" + transfer.jobId() + "' ran, but nothing was notified");
                return;
            }
            sequence.mediate(msg);
        } catch (RuntimeException | Error e) {
            log.error("the " + parameterName + " '" + sequenceName + "' failed after transfer '"
                    + transfer.jobId() + "'. The transfer's own outcome is unaffected", e);
        }
    }

    /**
     * Joins the transfer row to the run that is executing it.
     *
     * <p>Not fatal: the bytes move whether or not the two rows can be joined, and failing a transfer
     * because a reporting write did not land would be the reporting tail wagging the transfer dog. It
     * is logged loudly because a job someone cannot trace back to its run is exactly what the job table
     * exists to prevent.
     */
    private void linkRun(TransferStore store, Transfer transfer) {
        try {
            store.linkRun(transfer.jobId(), transfer.runName());
        } catch (StreamException e) {
            log.error("transfer " + transfer.jobId() + " is running as '" + transfer.runName()
                    + "' but the two could not be linked; its status will still be recorded", e);
        }
    }

    /**
     * Records the outcome on the transfer row.
     *
     * <p>Also not fatal, and for a sharper reason: by the time this runs the bytes have already landed
     * or already failed, and the run's own job row says which. A retry driven by a failed status write
     * would repeat a transfer that succeeded.
     */
    private void finish(TransferStore store, Transfer transfer, TransferStatus status, String code,
                        String message) {
        try {
            store.finish(transfer.jobId(), status, code, message);
        } catch (StreamException e) {
            log.error("transfer " + transfer.jobId() + " finished as " + status + " but the status"
                    + " could not be recorded; it will look claimed until this node's next startup"
                    + " sweep", e);
        }
    }

    private void requeue(TransferStore store, Transfer transfer, long notBefore) {
        try {
            if (!store.requeue(transfer.jobId(), notBefore)) {
                log.warn("transfer " + transfer.jobId() + " could not be returned to the queue; it may"
                        + " have been dropped or already requeued");
            }
        } catch (StreamException e) {
            // The transfer stays CLAIMED. The owner-node startup sweep is what recovers it, which is
            // the same mechanism that recovers a node that died holding one.
            log.error("transfer " + transfer.jobId() + " could not be returned to the queue and is"
                    + " left claimed by this node; it will be swept when this node restarts", e);
        }
    }

    /** How many transfers this node is running right now. Package-private: for tests and diagnostics. */
    int inFlight() {
        return running.get();
    }

    /** Stops claiming. In-flight transfers are left to finish or to stop on the shutdown signal. */
    public void terminate() {
        terminated = true;
        if (workers != null) {
            workers.shutdown();
        }
    }

    /** Clears the flag set by {@link #terminate()}, so a resumed processor claims again. */
    public void resetTerminated() {
        terminated = false;
        if (workers == null || workers.isShutdown()) {
            initialized = false;            // rebuilt on the next fire
        }
    }

    @Override
    public void destroy() {
        terminate();
    }

    /** Like {@link #readPositiveInt}, but zero is meaningful: it turns the bound off. */
    private static int readCount(Map<String, Object> parameters, String name, int fallback) {
        String configured = text(parameters, name, null);
        if (configured == null) {
            return fallback;
        }
        try {
            int value = Integer.parseInt(configured);
            return Math.max(0, value);
        } catch (NumberFormatException e) {
            log.warn("'" + name + "' is '" + configured + "', which is not a number; using " + fallback);
            return fallback;
        }
    }

    private static String text(Map<String, Object> parameters, String name, String fallback) {
        Object configured = parameters == null ? null : parameters.get(name);
        if (configured == null) {
            return fallback;
        }
        String value = configured.toString().trim();
        return value.isEmpty() ? fallback : value;
    }

    private static int readPositiveInt(Map<String, Object> parameters, String name, int fallback) {
        Object configured = parameters == null ? null : parameters.get(name);
        if (configured == null) {
            return fallback;
        }
        try {
            int value = Integer.parseInt(configured.toString().trim());
            if (value <= 0) {
                log.warn("'" + name + "' is " + value + ", which is not a usable value; using "
                        + fallback);
                return fallback;
            }
            return value;
        } catch (NumberFormatException e) {
            log.warn("'" + name + "' is '" + configured + "', which is not a number; using " + fallback);
            return fallback;
        }
    }

    /**
     * The whole chain a queued transfer came through, in the order it passed them.
     *
     * <pre>mft-jobs -&gt; mft-queue -&gt; mft-runner</pre>
     *
     * <p>Assembled here because here is the only place all three are known at once. The submitter was
     * recorded when the transfer was stored and has long since returned; the store and this processor
     * are configuration this class holds. A run row that named only one of them would answer "which
     * processor ran it" while leaving "which API asked for it" unanswerable -- and the second is the
     * question an operator has when a caller reports a transfer that never arrived.
     *
     * @param transfer the claimed transfer
     * @return the path, never {@code null}: the store and processor are always known
     */
    private String invocationPath(Transfer transfer) {
        return Invoker.path(transfer.submitter(), transfer.storeName(), messageProcessor.getName());
    }

    /**
     * What the sequence is told about the transfer it is running.
     *
     * <p>The job id, which names the run -- naming it the same way on every attempt is what lets the
     * second attempt resume into the first one's workspace (ADR-0020) -- and the invocation path, which
     * the sequence cannot work out for itself because none of the artifacts in it are on this thread.
     */
    private static final class TransferJobContext implements JobContext {

        /**
         * How often the store is asked whether someone wants this transfer stopped.
         *
         * <p>{@code isCancelled()} is polled by the framework between buffer reads — every 64 KB in
         * {@code drain()} — so it cannot be a query. Two seconds is the control-plane cadence: an
         * order of magnitude finer than the 8 MB at which a file sink notices, and coarse enough that
         * a transfer moving 100 MB/s asks about 50 times fewer than it writes.
         */
        private static final long POLL_INTERVAL_MILLIS = 2000L;

        private final String jobId;
        private final String invocationPath;
        private final TransferStore store;
        private final long transferId;

        /** The last answer and when it was fetched. Volatile: the poll thread is not the run's. */
        private volatile long lastPolledAt;
        private volatile boolean stopRequested;
        private volatile boolean discardOnStop;

        private TransferJobContext(String jobId, String invocationPath, TransferStore store,
                                   long transferId) {
            this.jobId = jobId;
            this.invocationPath = invocationPath;
            this.store = store;
            this.transferId = transferId;
        }

        @Override
        public String jobId() {
            return jobId;
        }

        @Override
        public String invokerType() {
            return Invoker.QUEUED;
        }

        @Override
        public String invokerName() {
            return invocationPath;
        }

        /**
         * Whether someone has asked this transfer to stop, read from the row that carries the request.
         *
         * <p>The row rather than a field, because the request arrives on a management thread that may
         * not even be on this node: in a cluster the API call lands wherever the load balancer sent it,
         * and an in-memory flag would only work by luck. The status column is the one channel both
         * sides can see.
         *
         * <p>Throttled, and it never un-requests: once seen, the answer stays true. A request that was
         * withdrawn between two polls would otherwise let a half-unwound run carry on.
         */
        @Override
        public boolean isCancelled() {
            if (stopRequested) {
                return true;
            }
            long now = System.currentTimeMillis();
            if (now - lastPolledAt < POLL_INTERVAL_MILLIS) {
                return false;
            }
            lastPolledAt = now;
            try {
                Transfer current = store.find(transferId);
                if (current == null) {
                    return false;
                }
                TransferStatus status = TransferStatus.valueOf(current.status());
                if (status.isStopping()) {
                    discardOnStop = status == TransferStatus.STOPPING;
                    stopRequested = true;
                    return true;
                }
            } catch (StreamException | IllegalArgumentException e) {
                // Unreadable or unrecognised: carry on transferring. Stopping a healthy transfer
                // because a control query failed would be the worse of the two mistakes.
                if (log.isDebugEnabled()) {
                    log.debug("could not read the stop state of transfer " + transferId, e);
                }
            }
            return false;
        }

        @Override
        public boolean discardStateOnStop() {
            return discardOnStop;
        }
    }
}
