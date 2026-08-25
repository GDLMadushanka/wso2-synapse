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

package org.apache.synapse.stream.pipeline;

import org.apache.axiom.om.OMElement;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.ManagedLifecycle;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseException;
import org.apache.synapse.util.logging.LoggingUtils;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.stream.JobContext;
import org.apache.synapse.stream.ResourceScope;
import org.apache.synapse.stream.StreamException;
import org.apache.synapse.stream.StreamOperator;
import org.apache.synapse.stream.StreamOrigin;
import org.apache.synapse.stream.StreamSeed;
import org.apache.synapse.stream.StreamSink;
import org.apache.synapse.stream.StreamSource;
import org.apache.synapse.stream.StreamTransform;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * A deployable ordered list of {@link StreamOperator}s, and the only class that knows the chain
 * exists.
 * <p>
 * {@link #execute} does two things, strictly in order: it <b>builds</b> the chain, moving no bytes,
 * and then it <b>pulls</b> it, moving all of them. There is no scheduler, no thread pool, no queue
 * between stages and no buffer this class owns.
 * <p>
 * The <b>terminal stage</b> is what gets pulled to completion, and it can be one of two things. A
 * {@link StreamSink} pulls itself inside {@code consume()}. A {@link StreamTransform} that declares
 * {@code materialises()} may also be terminal, in which case this class drains it — because a
 * materialising transform already drains its input and writes the result durably, and whether
 * anything reads its artifact afterwards is not its concern. That is what lets an operator such as
 * a per-record {@code forEach} be written once and used either mid-chain or last.
 * <p>
 * Backpressure is therefore not a feature that had to be designed — it is what a pull chain is. The
 * cost is that a chain is single-threaded end to end: one transfer saturates one thread, and a slow
 * stage stalls the chain rather than buffering around it. Concurrency in MFT is across transfers.
 *
 * <h2>Thread safety</h2>
 * One instance serves concurrent runs. It holds no per-run state: everything belongs to the
 * {@link ResourceScope} and the per-operator {@link DefaultStreamContext} created inside
 * {@code execute}. The same is required of operators.
 *
 * <h2>Not yet implemented</h2>
 * Workspace, materialisation, restart segments and checkpointing. {@link #validate()} therefore
 * rejects a pipeline whose operators declare {@code materialises()} or {@code checkpointed()},
 * rather than accepting it and failing later.
 */
public class StreamPipeline implements ManagedLifecycle {

    private static final Log log = LogFactory.getLog(StreamPipeline.class);

    /** Stage name used for a caller-supplied stream, which has no operator of its own. */
    public static final String PROVIDED_STAGE = "(provided)";

    /** Buffer used when the pipeline itself has to pull a terminal materialising transform. */
    private static final int DRAIN_BUFFER_BYTES = 64 * 1024;


    private String name;
    private String description;
    private String fileName;
    private String artifactContainerName;
    private boolean deployedFromCApp;

    /** True when the caller supplies the bytes and the chain has no source. */
    private boolean sourceProvided;

    private final List<StreamOperator> operators = new ArrayList<>();

    /** Names that came from configuration rather than being derived from a position. */
    private final List<Boolean> explicitNames = new ArrayList<>();

    /**
     * One entry per configured stage, in chain order. An entry either holds the operator instance
     * outright or knows the connector template to reach it through — see {@link OperatorEntry}.
     */
    private final List<OperatorEntry> entries = new ArrayList<>();

    /**
     * Whether the structural rules have been checked. False only while a connector operation is still
     * unreachable — see {@link #bindDeferredStages}.
     */
    private volatile boolean validated;

    /** Whether any stage had to be reached through a connector template, fixed once at parse. */
    private boolean wasDeferred;

    /**
     * Per-stage checkpoint cadence, in the operator's own checkpoint unit; {@code null} means "use
     * the operator's default". Set from the {@code maxReprocessed} stage attribute.
     */
    private final List<Integer> maxReprocessed = new ArrayList<>();

    private SynapseEnvironment environment;

    /**
     * The element this pipeline was built from, kept so it can be serialized back without every
     * operator author having to write a serializer alongside their factory. Deployment metadata, in
     * the same spirit as {@code fileName} — not something the runtime reads.
     */
    private OMElement sourceElement;

    // ---------------------------------------------------------------- lifecycle

    @Override
    public void init(SynapseEnvironment se) {
        this.environment = se;
        auditInfo("Initializing Stream Pipeline: " + name);

        // A connector's operators do not exist at parse time, so a pipeline holding one is completed
        // here instead — see bindDeferredStages for why this is attempted rather than required.
        wasDeferred = hasDeferredStages();
        if (wasDeferred) {
            bindDeferredStages();
        }

        for (StreamOperator op : operators) {
            if (op instanceof ManagedLifecycle managed) {
                managed.init(se);
            }
        }

        // A deferred pipeline is announced by bindDeferredStages, whenever it completes — which may be
        // here or on first use. Announcing it again would double the line for the common case.
        if (!wasDeferred) {
            auditInfo("Successfully deployed Stream Pipeline: " + name + " with "
                    + operators.size() + " operator(s)");
        }
    }

    /**
     * Logs in the same shape as every other artifact — {@code {streamPipeline:name} message} — so a
     * pipeline is greppable alongside the proxies, APIs and inbound endpoints around it in a startup
     * log.
     */
    private void auditInfo(String message) {
        log.info(LoggingUtils.getFormattedLog(SynapseConstants.STREAM_PIPELINE_TYPE, name, message));
    }

    @Override
    public void destroy() {
        auditInfo("Destroying Stream Pipeline: " + name);
        // Reverse order, mirroring construction.
        for (int i = operators.size() - 1; i >= 0; i--) {
            if (operators.get(i) instanceof ManagedLifecycle managed) {
                try {
                    managed.destroy();
                } catch (Throwable t) {
                    log.warn("Failed to destroy operator '" + operators.get(i).name()
                            + "' of pipeline '" + name + "'; continuing", t);
                }
            }
        }
    }

    // ---------------------------------------------------------------- validation

    /**
     * Checks every structural rule, once, at deployment. Performs no I/O.
     * <p>
     * These are rules about the <b>current topology kind</b> — a linear chain — rather than
     * assumptions to be threaded through the executor. Keeping them here is what would let a
     * different topology be added later without rewriting the runtime.
     *
     * @throws StreamException naming the offending operator and its index
     */
    public void validate() throws StreamException {
        if (name == null || name.isBlank()) {
            throw new StreamException("a stream pipeline must have a name");
        }
        if (operators.isEmpty()) {
            throw new StreamException("stream pipeline '" + name + "' has no operators");
        }

        int last = operators.size() - 1;
        int sinks = 0;

        for (int i = 0; i <= last; i++) {
            StreamOperator op = operators.get(i);
            String where = "operator " + i + " ('" + safeName(op) + "') of pipeline '" + name + "'";

            // Role exclusivity. execute() dispatches on instanceof StreamSource before position,
            // so a multi-role operator mid-chain would silently discard its upstream.
            int roles = (op instanceof StreamSource ? 1 : 0)
                    + (op instanceof StreamTransform ? 1 : 0)
                    + (op instanceof StreamSink ? 1 : 0);
            if (roles == 0) {
                throw new StreamException(where + " implements none of StreamSource, StreamTransform"
                        + " or StreamSink");
            }
            if (roles > 1) {
                throw new StreamException(where + " implements more than one role interface; write"
                        + " two classes and share logic by composition, because dispatch tests"
                        + " instanceof StreamSource before position and would silently discard its"
                        + " upstream");
            }

            String opName = op.name();
            if (opName == null || opName.isBlank()) {
                throw new StreamException(where + " has no name");
            }
            if (opName.indexOf('/') >= 0 || opName.indexOf('\\') >= 0 || opName.indexOf('\0') >= 0
                    || !opName.equals(opName.trim())) {
                throw new StreamException(where + " has a name that cannot be used as a path"
                        + " segment: '" + opName + "'");
            }

            if ((op.checkpointed() || op.materialises()) && !Boolean.TRUE.equals(explicitNames.get(i))) {
                throw new StreamException(where + " declares checkpointed() or materialises() and so"
                        + " requires an explicit name: durable state is keyed by name, and a name"
                        + " derived from chain position stops being meaningful as soon as a stage can"
                        + " appear in more than one branch");
            }

            if (op instanceof StreamSink) {
                sinks++;
                if (i != last) {
                    throw new StreamException(where + " is a StreamSink but is not last; a sink is"
                            + " the only operator that reads, so nothing can follow it");
                }
            }
            if (op instanceof StreamSource && i != 0) {
                throw new StreamException(where + " is a StreamSource but is not first");
            }
        }

        // A pipeline may end in one of two ways, and the second is not a special case so much as a
        // consequence of what materialisation already is. A materialising transform drains its input
        // to completion and writes the result durably — which is sink-like work. The only thing that
        // makes it a "transform" is that it also offers its artifact as a stream for whatever comes
        // next. So it is a segment boundary, and whether anything follows it merely decides whether
        // a next segment exists.
        //
        // That is what lets an operator like forEach be written once, as a transform, and used
        // either mid-chain or as the last thing in the pipeline without knowing which.
        if (sinks == 0) {
            StreamOperator terminal = operators.get(last);
            if (!(terminal instanceof StreamTransform) || !terminal.materialises()) {
                throw new StreamException("stream pipeline '" + name + "' must end with a StreamSink,"
                        + " or with a StreamTransform that declares materialises(); operator " + last
                        + " ('" + safeName(terminal) + "') is neither, so this pipeline would pull"
                        + " bytes and discard them with no durable effect anywhere");
            }
        }

        if (sourceProvided) {
            if (operators.get(0) instanceof StreamSource) {
                throw new StreamException("stream pipeline '" + name + "' declares"
                        + " sourceProvided=\"true\", so its first operator must not be a"
                        + " StreamSource — the caller supplies the bytes");
            }
        } else {
            if (operators.size() < 2) {
                throw new StreamException("stream pipeline '" + name + "' needs at least a source and"
                        + " a sink; declare sourceProvided=\"true\" if the caller supplies the bytes");
            }
            if (!(operators.get(0) instanceof StreamSource)) {
                throw new StreamException("stream pipeline '" + name + "' must begin with a"
                        + " StreamSource, or declare sourceProvided=\"true\"");
            }
        }

        for (int i = 0; i < operators.size(); i++) {
            StreamOperator op = operators.get(i);
            if (op.materialises() || op.checkpointed()) {
                throw new StreamException("operator " + i + " ('" + safeName(op) + "') of pipeline '"
                        + name + "' declares materialises() or checkpointed(), which requires a"
                        + " workspace; workspace and checkpoint support are not implemented yet");
            }
        }
    }

    /**
     * Whether any operator in this pipeline keeps durable state.
     *
     * <p>This is a property of the <b>artifact</b> and it is only half of the question. It says the
     * operators are <i>capable</i> of resuming; whether a given run <i>can</i> is decided by where
     * its bytes came from — see {@link #resumable(StreamOrigin)}, which is the method callers
     * actually want.
     *
     * @return {@code true} if any operator declares {@code materialises()} or {@code checkpointed()}
     */
    public boolean hasDurableState() {
        for (StreamOperator op : operators) {
            if (op.materialises() || op.checkpointed()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether a run of this pipeline over the given origin can be resumed after a failure.
     *
     * <p>Both halves are required, and this is the correction to an earlier design that asked only
     * the first:
     *
     * <ul>
     *   <li>{@link #hasDurableState()} — the operators are capable of resuming.</li>
     *   <li>{@link StreamOrigin#REOPENABLE} — there is a second attempt to resume <i>into</i>.</li>
     * </ul>
     *
     * <p>Asking only about the operators makes durability a property of the artifact, which is wrong
     * in a way that shows up immediately in practice: the same pipeline referenced from a file
     * inbound endpoint and from an API resource handling a multipart body would be forced into one
     * answer for both. The operators are identical in those two cases. Only the origin differs, so
     * only the origin can distinguish them.
     *
     * @param origin where this run's bytes come from
     * @return {@code true} if this run should keep checkpoints and needs a real job id
     */
    public boolean resumable(StreamOrigin origin, String runId, boolean runIdStable) {
        return hasDurableState() && origin == StreamOrigin.REOPENABLE && runIdStable && runId != null;
    }

    /**
     * Names this run's workspace. <b>Never asks the caller for one.</b>
     *
     * <p>An earlier design demanded a real job id from any caller running a pipeline that kept durable
     * state, and threw otherwise. That was a mistake with a user-visible cost: a job id is the handle a
     * caller is <i>given</i> so it can ask about a transfer later, and whether it exists should depend
     * on whether the caller needs one — not on whether some operator deep in the chain happens to
     * materialise. Materialisation is an implementation detail of the pipeline; it has no business
     * appearing in the caller's contract.
     *
     * <p>So the framework always names its own workspace, from whatever stable identity it can find:
     *
     * <ol>
     *   <li><b>The caller's job id</b>, when there is one. Every queued transfer has one before it
     *       runs, and a retry deliberately keeps it, which is what makes resume work.</li>
     *   <li><b>The seed's identity</b>, when the caller supplied a stream instead. A file inbound
     *       endpoint knows the URI, size and modification time, so the run can be content-addressed
     *       and re-detecting the same file lands on the same workspace. This needs no I/O — the fields
     *       are already on the seed.</li>
     *   <li><b>A fresh id</b> otherwise. Nothing stable was available, so nothing will resume: the run
     *       is scratch and its workspace is reclaimed when it ends.</li>
     * </ol>
     *
     * <p>The pipeline name is folded into case 2 so that two pipelines processing the same file
     * concurrently do not collide on one directory.
     *
     * @return the run id, and whether it is stable enough to resume from
     */
    RunId resolveRunId(JobContext jobCtx, StreamSeed seed, StreamOrigin origin) {
        if (!JobContext.NOOP_JOB_ID.equals(jobCtx.jobId())) {
            return new RunId(jobCtx.jobId(), true);
        }
        if (seed != null && origin == StreamOrigin.REOPENABLE) {
            // Content-addressed: same pipeline over the same bytes resumes into the same workspace, and
            // a changed source lands somewhere else rather than resuming into stale artifacts.
            String material = name + '\0' + seed.sourceId() + '\0' + seed.size()
                    + '\0' + seed.lastModified();
            return new RunId("src-" + Integer.toHexString(material.hashCode())
                    + '-' + Long.toHexString(fnv1a(material)), true);
        }
        return new RunId("run-" + UUID.randomUUID(), false);
    }

    /** A second, independent hash so a 32-bit collision alone cannot merge two runs' workspaces. */
    private static long fnv1a(String s) {
        long h = 0xcbf29ce484222325L;
        for (int i = 0; i < s.length(); i++) {
            h = (h ^ s.charAt(i)) * 0x100000001b3L;
        }
        return h;
    }

    /** A run's workspace name, and whether anything may resume from it. */
    record RunId(String value, boolean stable) {
    }


    /**
     * Where this pipeline's bytes come from for a run, which is the seed's origin when the caller
     * supplies one and the source operator's otherwise.
     */
    private StreamOrigin originOf(StreamSeed seed) {
        if (seed != null) {
            return seed.origin();
        }
        // validate() guarantees a non-sourceProvided pipeline starts with a StreamSource.
        return ((StreamSource) operators.get(0)).origin();
    }

    // ---------------------------------------------------------------- execution

    /**
     * Runs this pipeline, which must not declare {@code sourceProvided}.
     *
     * @param msg the invoking message context
     * @param job the caller's job context, or {@code null} for {@link JobContext#NOOP}
     * @throws StreamException if the chain could not be built or pulled to completion
     */
    public void execute(MessageContext msg, JobContext job) throws StreamException {
        execute(msg, job, null);
    }

    /**
     * Runs this pipeline over a caller-supplied stream.
     * <p>
     * The seed's stream is <b>close-shielded</b>: the provider opened it and will close it, but a
     * transform's {@code close()} cascades downward and the resource scope closes what it
     * registers, so without shielding it would be closed twice.
     *
     * @param msg  the invoking message context
     * @param job  the caller's job context, or {@code null} for {@link JobContext#NOOP}
     * @param seed the caller's stream and its identity; required if and only if this pipeline
     *             declares {@code sourceProvided}
     * @throws StreamException if the chain could not be built or pulled to completion
     */
    public void execute(MessageContext msg, JobContext job, StreamSeed seed) throws StreamException {
        if (sourceProvided && seed == null) {
            throw new StreamException("stream pipeline '" + name + "' declares sourceProvided=\"true\""
                    + " but no stream was supplied by the caller");
        }
        if (!sourceProvided && seed != null) {
            throw new StreamException("stream pipeline '" + name + "' opens its own source but a"
                    + " stream was supplied by the caller; declare sourceProvided=\"true\" to accept"
                    + " one");
        }

        // Everything below reads the resolved operators, so a pipeline whose connector was not yet
        // deployed at init has to be completed now. By this point it certainly is: a run cannot start
        // before the server finished starting.
        // Normally already done by init(). Reached only if this pipeline is executed without having
        // been initialised, which the tests do; a failure here is the same misconfiguration.
        if (hasDeferredStages() && !validated) {
            bindDeferredStages();
        }

        JobContext jobCtx = job == null ? JobContext.NOOP : job;

        StreamOrigin origin = originOf(seed);
        RunId runId = resolveRunId(jobCtx, seed, origin);
        boolean resumable = resumable(origin, runId.value(), runId.stable());

        // Capable of resuming, but with nothing to resume from. Never an error — the caller is not
        // required to supply anything for this to be the right answer — but not silent either, because
        // the operators here were written expecting their checkpoints to matter. Structural writes
        // still get space; that space is scratch, reclaimed when the run ends.
        if (hasDurableState() && !resumable && log.isDebugEnabled()) {
            log.debug("stream pipeline '" + name + "' keeps durable state but run '" + runId.value()
                    + "' cannot be resumed (" + (origin == StreamOrigin.ONE_SHOT
                            ? "its bytes are one-shot, so there is no second attempt"
                            : "no stable identity was available to name a workspace across attempts")
                    + "): checkpointing is off and any materialised output is scratch");
        }

        List<StageStream> stages = new ArrayList<>();
        StreamException primary = null;

        ResourceScope resources = new ResourceScope();
        try {
            // ---- phase 1: build. No bytes move. -------------------------------------------
            InputStream current = null;
            StageStream below = null;
            StreamSink sink = null;
            DefaultStreamContext sinkCtx = null;

            if (sourceProvided) {
                below = new StageStream(new CloseShield(seed.stream()), PROVIDED_STAGE, null);
                current = below;
                stages.add(below);
            }

            // Resolved per run rather than reused from init(), so a connector redeployed since then
            // takes effect — and so concurrent runs never share a resolution. See OperatorEntry.
            List<OperatorEntry.Resolution> resolved = resolveForRun();
            OperatorEntry.Resolution sinkResolution = null;

            for (int i = 0; i < resolved.size(); i++) {
                OperatorEntry.Resolution resolution = resolved.get(i);
                StreamOperator op = resolution.operator();
                DefaultStreamContext ctx = contextFor(msg, jobCtx, resources, op);

                if (op instanceof StreamSink s) {
                    sink = s;
                    sinkCtx = ctx;
                    sinkResolution = resolution;
                    continue;                       // the sink wraps nothing; it reads
                }

                // A connector operation reads its configuration from the function stack, the ordinary
                // connector way, so its parameters are bound only for the duration of this call. The
                // operator must capture what it needs here: the stream it returns is read later, when
                // nothing is bound. For an SPI-built operator this is a no-op — its configuration is
                // already in its own fields.
                boolean bound = resolution.bind(msg);
                try {
                    if (op instanceof StreamSource src) {
                        current = src.open(ctx);
                        if (current == null) {
                            throw new StreamException("operator '" + op.name() + "' returned a null"
                                    + " stream from open(); emptiness is -1 on first read, not null")
                                    .withStage(op.name());
                        }
                    } else {
                        InputStream upstream = current;
                        current = ((StreamTransform) op).wrap(upstream, ctx);
                        if (current == null) {
                            throw new StreamException("operator '" + op.name() + "' returned a null"
                                    + " stream from wrap()").withStage(op.name());
                        }
                        if (current == upstream) {
                            throw new StreamException("operator '" + op.name() + "' returned its own"
                                    + " upstream from wrap(); a pass-through must still be a distinct"
                                    + " wrapper or per-stage accounting is wrong").withStage(op.name());
                        }
                    }
                } finally {
                    if (bound) {
                        resolution.release(msg);
                    }
                }

                below = new StageStream(current, op.name(), below);
                current = below;
                stages.add(below);
            }

            jobCtx.sourceOpened(seed == null ? StreamSeed.UNKNOWN : seed.size(),
                    seed == null ? StreamSeed.UNKNOWN : seed.lastModified());

            // ---- phase 2: pull. Everything happens here. ----------------------------------
            if (sink != null) {
                // Held for the whole pull rather than just the call, because everything upstream
                // executes inside consume().
                boolean sinkBound = sinkResolution != null && sinkResolution.bind(msg);
                try {
                    sink.consume(current, sinkCtx);
                } finally {
                    if (sinkBound) {
                        sinkResolution.release(msg);
                    }
                }
            } else {
                // No sink: the terminal is a materialising transform, so the pipeline pulls it.
                // Draining is how its work happens — a lazily materialising operator writes its
                // artifact as it is read, so this is one pass and the discarded bytes are the same
                // ones it was already writing.
                drain(current, jobCtx);
            }

        } catch (StageStream.StageIOException e) {
            primary = new StreamException("stream pipeline '" + name + "' failed", e, true)
                    .withStage(e.getStage());
        } catch (IOException e) {
            // Not attributed, so it can only have come from the sink — the one unwrapped position.
            primary = new StreamException("stream pipeline '" + name + "' failed", e, true)
                    .withStage(sinkName());
        } catch (StreamException e) {
            primary = e.getStage() == null ? e.withStage(sinkName()) : e;
        } catch (RuntimeException e) {
            primary = new StreamException("stream pipeline '" + name + "' failed unexpectedly", e);
        } finally {
            try {
                if (primary == null) {
                    resources.close();
                } else {
                    // The run failed. Release everything and publish nothing: a committing resource
                    // closed normally here would rename a partial artifact into place, and the next
                    // attempt would read its presence as "this segment completed".
                    resources.abort();
                }
            } catch (IOException closeFailure) {
                if (primary != null) {
                    // The original failure is the diagnosis; a commit failure during an
                    // already-failing unwind is a consequence of it and must not replace it.
                    primary.addSuppressed(closeFailure);
                } else {
                    primary = new StreamException("stream pipeline '" + name + "' read every byte but"
                            + " failed to commit its output", closeFailure, true);
                }
            }
            reportStagesQuietly(jobCtx, stages);
        }

        if (primary != null) {
            jobCtx.failed(primary.getStage(), "STREAM_PIPELINE_FAILED", primary.getMessage());
            throw primary;
        }
    }

    /**
     * Pulls a terminal stage to completion when there is no sink to do it.
     * <p>
     * Polls cancellation between reads for the same reason a sink must: otherwise a pipeline ending
     * in a materialising transform would be the one shape that cannot be cancelled.
     */
    private void drain(InputStream terminal, JobContext job) throws IOException, StreamException {
        byte[] buffer = new byte[DRAIN_BUFFER_BYTES];
        while (terminal.read(buffer, 0, buffer.length) != -1) {
            if (job.isCancelled()) {
                throw new StreamException("stream pipeline '" + name + "' was cancelled", true);
            }
        }
    }

    private DefaultStreamContext contextFor(MessageContext msg, JobContext job,
                                            ResourceScope resources, StreamOperator op) {
        boolean mayUseWorkspace = op.materialises() || op.checkpointed();
        return new DefaultStreamContext(msg, job, resources, op.name(), mayUseWorkspace,
                op.checkpointed(), null, null);
    }

    private String sinkName() {
        return operators.isEmpty() ? name : safeName(operators.get(operators.size() - 1));
    }

    private static String safeName(StreamOperator op) {
        try {
            String n = op.name();
            return n == null ? op.getClass().getSimpleName() : n;
        } catch (RuntimeException e) {
            return op.getClass().getSimpleName();
        }
    }

    /** Telemetry must never fail a transfer, on either path. */
    private void reportStagesQuietly(JobContext job, List<StageStream> stages) {
        for (StageStream s : stages) {
            try {
                job.stageFinished(s.stage(), s.bytesIn(), s.bytes(), s.selfNanos());
            } catch (Throwable t) {
                log.warn("Failed to report stage '" + s.stage() + "'; continuing", t);
            }
        }
    }

    /**
     * Keeps a caller-supplied stream from being closed by the chain built over it. Ownership stays
     * with whoever opened it.
     */
    private static final class CloseShield extends FilterInputStream {

        private CloseShield(InputStream in) {
            super(in);
        }

        @Override
        public void close() {
            // Deliberately nothing.
        }
    }

    // ---------------------------------------------------------------- assembly

    /**
     * Appends an operator.
     *
     * @param op            the operator
     * @param nameIsExplicit whether the name came from configuration rather than being derived
     *                       from the operator's position; required for durable state
     */
    public void addOperator(StreamOperator op, boolean nameIsExplicit) {
        addOperator(op, nameIsExplicit, null);
    }

    /**
     * Adds an operator, with an explicit bound on how much work a failure may cost.
     *
     * @param op             the operator
     * @param nameIsExplicit whether its name was written down rather than derived from position
     * @param maxReprocessed the deployer's bound in the operator's checkpoint unit, or {@code null}
     *                       to take the operator's default
     */
    public void markValidated() {
        this.validated = true;
    }

    public void addOperator(StreamOperator op, boolean nameIsExplicit, Integer maxReprocessed) {
        if (op == null) {
            throw new IllegalArgumentException("cannot add a null operator to pipeline '" + name + "'");
        }
        if (maxReprocessed != null && maxReprocessed < 1) {
            throw new IllegalArgumentException("maxReprocessed must be at least 1: " + maxReprocessed);
        }
        operators.add(op);
        entries.add(OperatorEntry.ofOperator(op, op.name()));
        explicitNames.add(nameIsExplicit);
        this.maxReprocessed.add(maxReprocessed);
    }

    /**
     * Adds a connector operation, whose operator lives inside a template and cannot be reached until
     * that connector is deployed.
     *
     * <p>This is the route that needs no change to how connectors are written. Nothing is loaded here —
     * the entry holds a template name — so a pipeline naming {@code <file.streamRead>} parses long
     * before the library deployer has run.
     *
     * @param invoke         the invocation aimed at the connector's template
     * @param elementName    the configured element name, for error messages
     * @param nameIsExplicit whether a name attribute was written down
     * @param maxReprocessed the deployer's bound, or {@code null} for the default
     */
    public void addOperation(org.apache.synapse.mediators.template.InvokeMediator invoke,
                             String elementName, boolean nameIsExplicit, Integer maxReprocessed) {
        if (invoke == null) {
            throw new IllegalArgumentException("cannot add a null operation to pipeline '" + name + "'");
        }
        entries.add(OperatorEntry.ofOperation(invoke, elementName));
        explicitNames.add(nameIsExplicit);
        this.maxReprocessed.add(maxReprocessed);
    }

    /**
     * Resolves every deferred stage and checks the structural rules, once.
     *
     * <h2>Why this is a hard failure</h2>
     * A connector's operators cannot be resolved when the configuration is parsed — the library deployer
     * has not run yet — so a pipeline holding one is completed here instead. By the time {@code init()}
     * is reached the connector <i>is</i> deployed, on both routes that exist:
     *
     * <ul>
     *   <li><b>From a CApp</b> — {@code SynapseAppDeployer} deploys libraries before the artifacts that
     *       reference them.</li>
     *   <li><b>From {@code synapse-configs/default/stream-pipelines/}</b> —
     *       {@code deployMediationLibraryArtifacts()} runs earlier in
     *       {@code createSynapseEnvironment()} than {@code synapseConfiguration.init()} does.</li>
     * </ul>
     *
     * So an operation that cannot be reached here is a misconfiguration, not a race, and saying so at
     * startup is strictly better than saying it mid-transfer. An earlier version of this method treated
     * the failure as tolerable and deferred it to first use; that was built on a mis-reading of the
     * startup order and let a broken pipeline start up looking healthy.
     *
     * <p>{@code resolveForRun()} still re-resolves every run, so nothing is cached across a connector
     * redeploy.
     */
    private synchronized void bindDeferredStages() {
        if (validated) {
            return;
        }
        operators.clear();
        for (OperatorEntry.Resolution resolution : resolveForRun()) {
            operators.add(resolution.operator());
        }
        try {
            validate();
        } catch (StreamException e) {
            throw new SynapseException("stream pipeline '" + name + "' is not valid: "
                    + e.getMessage(), e);
        }
        validated = true;
        auditInfo("Successfully deployed Stream Pipeline: " + name + " with " + operators.size()
                + " operator(s)");
    }

    /** Whether any stage still has to be reached through a connector template. */
    public boolean hasDeferredStages() {
        for (OperatorEntry entry : entries) {
            if (entry.isDeferred()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Resolves every stage as of now, for one run.
     *
     * <p>Repeated per run rather than reusing what {@code init()} found, so that a connector redeployed
     * since then takes effect. The result is local to the caller, so concurrent runs never share a
     * reference.
     */
    private List<OperatorEntry.Resolution> resolveForRun() {
        org.apache.synapse.config.SynapseConfiguration config =
                environment == null ? null : environment.getSynapseConfiguration();
        List<OperatorEntry.Resolution> resolved = new ArrayList<>(entries.size());
        for (OperatorEntry entry : entries) {
            resolved.add(entry.resolve(config, name));
        }
        return resolved;
    }

    /**
     * How many checkpoint units a failure at this stage may cost, which is also how often it
     * checkpoints. <b>The cadence is the duplicate window.</b>
     *
     * <p>Two outcomes only, and deliberately no third: the deployer's {@code maxReprocessed} if they set
     * one, otherwise <b>1</b>.
     *
     * <h2>Why no operator gets a say</h2>
     * An earlier design let an operator declare {@code repeatable()} and thereby widen this default to
     * 1000 on the grounds that replay of its own work was unobservable. That reintroduced the defect it
     * was meant to avoid: a deployer who configured <b>nothing</b> silently got a thousand-unit duplicate
     * window because a connector author had made a claim they never saw. Having configured nothing, they
     * would reasonably believe nothing had been widened.
     *
     * <p>So the cautious value is the only default, and the sole way to a wider window is for a deployer
     * to ask for one explicitly, in an attribute that states its own consequence. Nobody can be
     * surprised. Throughput is not lost — it is recommended in a connector's documentation rather than
     * assumed on the deployer's behalf.
     *
     * <h2>This is the only lever on the real bottleneck</h2>
     * A checkpoint requires the artifact to be durable first, so appends between checkpoints need no
     * individual {@code fsync} — they are truncated away on resume. Raising this from 1 to 1000 therefore
     * removes 999 of every 1000 network round-trips, which is the ceiling identified in ADR-0017's
     * amendment.
     *
     * @param index the operator's position in the chain
     * @return the maximum number of checkpoint units a failure here may cost; always at least 1
     */
    public int maxReprocessed(int index) {
        Integer configured = maxReprocessed.get(index);
        return configured != null ? configured : 1;
    }

    /** The operators, in chain order. */
    public List<StreamOperator> getOperators() {
        return Collections.unmodifiableList(operators);
    }

    public boolean isSourceProvided() {
        return sourceProvided;
    }

    public void setSourceProvided(boolean sourceProvided) {
        this.sourceProvided = sourceProvided;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getArtifactContainerName() {
        return artifactContainerName;
    }

    public void setArtifactContainerName(String artifactContainerName) {
        this.artifactContainerName = artifactContainerName;
    }

    public boolean isDeployedFromCApp() {
        return deployedFromCApp;
    }

    public void setDeployedFromCApp(boolean deployedFromCApp) {
        this.deployedFromCApp = deployedFromCApp;
    }

    /** The configuration element this pipeline was built from, or {@code null} if assembled in code. */
    public OMElement getSourceElement() {
        return sourceElement;
    }

    public void setSourceElement(OMElement sourceElement) {
        this.sourceElement = sourceElement;
    }

    public SynapseEnvironment getEnvironment() {
        return environment;
    }
}
