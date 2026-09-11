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
import org.apache.synapse.config.SynapsePropertiesLoader;
import org.apache.synapse.util.logging.LoggingUtils;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.rest.RESTConstants;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.stream.CheckpointStore;
import org.apache.synapse.stream.CheckpointStores;
import org.apache.synapse.stream.JobContext;
import org.apache.synapse.stream.JobRecord;
import org.apache.synapse.stream.JobRun;
import org.apache.synapse.stream.JobStore;
import org.apache.synapse.stream.JobStores;
import org.apache.synapse.stream.ResourceScope;
import org.apache.synapse.stream.StageArtifact;
import org.apache.synapse.stream.SourceIdentityPolicy;
import org.apache.synapse.stream.StreamException;
import org.apache.synapse.stream.StreamOperator;
import org.apache.synapse.stream.StreamOrigin;
import org.apache.synapse.stream.StreamSeed;
import org.apache.synapse.stream.StreamSink;
import org.apache.synapse.stream.StreamSource;
import org.apache.synapse.stream.StreamTransform;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.Files;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

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
     * The identity of each stage, in chain order.
     *
     * <p>Not {@code op.name()}. A connector operation reaches a <b>shared</b> class whose {@code name()}
     * is a constant, so two stages of the same operation would otherwise report the same name and — far
     * worse — resolve to the same workspace directory. The configured {@code name} attribute is what
     * distinguishes them, which is also why validate() insists on one for any operator keeping durable
     * state.
     */
    private final List<String> stageNames = new ArrayList<>();

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
     * Root that materialised artifacts are written beneath, from synapse.properties, or {@code null}
     * when none is configured — in which case materialisation is not permitted.
     */
    private String workspaceRoot;

    /**
     * Root for stage scratch space, from synapse.properties, or {@code null} to place it inside the
     * workspace. Unlike {@link #workspaceRoot} its absence is not an error: scratch is not durable
     * state, so there is always somewhere to put it.
     */
    private String scratchRoot;

    /**
     * Per-stage checkpoint cadence, in the operator's own checkpoint unit; {@code null} means "use
     * the operator's default". Set from the {@code maxReprocessed} stage attribute.
     */
    private final List<Integer> maxReprocessed = new ArrayList<>();

    /**
     * What to do when a retry finds a different source than the attempt it is resuming. Defaults to
     * {@link SourceIdentityPolicy#WARN}, which never resumes across a change but never blocks a run.
     */
    private SourceIdentityPolicy sourceIdentity = SourceIdentityPolicy.WARN;

    /**
     * Whether the deployer permits this pipeline to resume. Default {@code true} — meaning "if the run
     * qualifies", never "make it qualify".
     *
     * <p><b>One-directional by design.</b> Setting it false can only make a run <i>less</i> durable
     * than its origin permits, which is a choice a deployer is entitled to make: {@code maxReprocessed}
     * bottoms out at one unit, and a sink that is not idempotent at all needs zero replay. There is
     * deliberately no way to set it true and mean it, because no attribute can make a {@code ONE_SHOT}
     * source re-readable — that was ADR-0018's mistake, which ADR-0019 removed, and this must not
     * reintroduce it. See ADR-0033.
     */
    private boolean resume = true;

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
        this.workspaceRoot = SynapsePropertiesLoader.getPropertyValue(
                SynapseConstants.STREAM_WORKSPACE_ROOT, null);
        this.scratchRoot = SynapsePropertiesLoader.getPropertyValue(
                SynapseConstants.STREAM_SCRATCH_ROOT, null);
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
                    log.warn("Failed to destroy operator '" + nameForDiagnosis(operators.get(i))
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
            // Probed before anything else, because every message below interpolates it. Once this
            // loop has passed, name() is known not to throw for any operator in the chain, and the
            // rest of validate() and stageNameAt() can call it directly.
            String opName = requireName(i, op);
            String where = "operator " + i + " ('" + opName + "') of pipeline '" + name + "'";

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

            // Absent, blank and throwing were all handled by requireName() above, before this
            // message's own text could depend on the answer. What is left is path safety — and it is
            // checked on the CONFIGURED stage name, not on op.name(). Those differ for a connector
            // operation, whose op.name() is a safe class constant while the configured name is what
            // reaches runDir.resolve(): checking the wrong one let name="../../escape" through.
            String stageName = stageNameAt(i, op);
            requirePathSegment(where + " has a name", stageName);

            // Two stages sharing a name share a workspace directory, a checkpoint key and a
            // per-stage job row. The second silently overwrites the first's artifact, and the next
            // attempt "resumes" the first from the second's output.
            for (int j = 0; j < i; j++) {
                if (stageName.equals(stageNameAt(j, operators.get(j)))) {
                    throw new StreamException("stream pipeline '" + name + "' has two stages named '"
                            + stageName + "' (operators " + j + " and " + i + "). A stage's name keys"
                            + " its workspace directory and its checkpoints, so names must be"
                            + " distinct; give one an explicit name attribute");
                }
            }

            if ((op.checkpointed() || op.materialises()) && !Boolean.TRUE.equals(explicitNames.get(i))) {
                throw new StreamException(where + " declares checkpointed() or materialises() and so"
                        + " requires an explicit name: durable state is keyed by name, and a name"
                        + " derived from chain position stops being meaningful as soon as a stage can"
                        + " appear in more than one branch");
            }

            // A position that survives a failure, in a stage whose output does not, is the one
            // combination of the flags that corrupts silently.
            //
            // Consider source -> forEach(checkpointed, no artifact) -> fileSink. forEach mediates
            // records 1..50 and records the position; the sink has their bytes but has not published
            // them. The run fails, the scope aborts, and the sink's partial output is discarded --
            // correctly. But the checkpoint is a database row that abort() never touched, so the retry
            // resumes forEach at record 51 while the sink starts from nothing, and the output is
            // silently missing fifty records.
            //
            // Checkpointing the sink as well does not fix it: two independent positions in one segment
            // have nothing to reconcile against. L <= A compares a recorded length against an
            // artifact, and there is no artifact. Materialising is what puts the two stages in
            // DIFFERENT segments, so their positions never have to agree -- the later one re-runs from
            // a complete artifact instead.
            //
            // A terminal stage is exempt because nothing downstream can be missing anything: a sink is
            // always terminal, which is why the remote-authoritative sink (checkpointed, no artifact)
            // stays legal.
            // ADR-0034. A position counts records of the stage above it. If that stage materialises
            // and is not deterministic, a partial resume re-derives its unpersisted tail and may
            // re-derive it DIFFERENTLY -- so this stage's position would skip records of different
            // bytes. Silent corruption, and the exact failure deterministic()'s javadoc warns about.
            //
            // Every stage above is scanned, not just the nearest: non-determinism propagates through
            // a deterministic stage, because a deterministic stage fed different input produces
            // different output.
            if (op.checkpointed()) {
                for (int j = 0; j < i; j++) {
                    StreamOperator above = operators.get(j);
                    if (above.materialises() && !above.deterministic()) {
                        throw new StreamException(where + " declares checkpointed(), but operator "
                                + j + " ('" + stageNameAt(j, above) + "') above it materialises"
                                + " without declaring deterministic(). A failure re-derives that"
                                + " stage's unpersisted tail, which may differ, so this stage's"
                                + " recorded position would skip records of different bytes --"
                                + " silently. Drop checkpointed() here and declare materialises()"
                                + " instead, which re-runs this segment after a failure but cannot"
                                + " corrupt it; or make the stage above deterministic if it truly is");
                    }
                }
            }

            if (op.checkpointed() && !op.materialises() && i != last) {
                throw new StreamException(where + " declares checkpointed() without materialises() but"
                        + " is not the last stage. Its position would survive a failure that discarded"
                        + " the output of whatever consumes it, so the next attempt would skip work"
                        + " whose result was never published — silently, and with no way to detect it"
                        + " afterwards. Declare materialises() as well, which ends a segment here and"
                        + " lets the stages after it resume from a complete artifact, or move the"
                        + " checkpoint to the last stage");
            }

            // G2. A materialising stage writes an artifact so the NEXT segment can read it. A sink
            // has no next, so the artifact is written, sealed and never opened -- disk equal to the
            // whole output, spent on nothing. Refused rather than warned: the flag reads as durability
            // and buys none, and a deployer who wanted a durable copy wants a second sink, not this.
            if (op instanceof StreamSink && op.materialises()) {
                throw new StreamException(where + " is a StreamSink and declares materialises(), but"
                        + " a sink is always last, so nothing can ever read the artifact it would"
                        + " write. It would cost disk equal to the whole output and be deleted"
                        + " unread. Drop materialises(); a sink that also needs a durable copy is a"
                        + " second sink, not a flag");
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
                        + " ('" + terminal.name() + "') is neither, so this pipeline would pull"
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

        // The guard compares this attempt's source identity against the previous attempt's, and that
        // identity only exists when the caller supplies a StreamSeed. A pipeline that opens its own
        // source records none, so the comparison can never happen — and STRICT promises to refuse a
        // changed source. A promise that cannot be kept is refused here rather than left inert.
        if (sourceIdentity == SourceIdentityPolicy.STRICT && !sourceProvided) {
            throw new StreamException("stream pipeline '" + name + "' declares"
                    + " sourceIdentity=\"strict\" but opens its own source, so no source identity is"
                    + " recorded and nothing can be compared against a later attempt. strict is only"
                    + " honoured for sourceProvided=\"true\" pipelines, whose caller supplies the"
                    + " identity along with the stream");
        }

        for (int i = 0; i < operators.size(); i++) {
            StreamOperator op = operators.get(i);
            if (op.checkpointed() && !CheckpointStores.isAvailable()) {
                throw new StreamException("operator " + i + " ('" + op.name() + "') of pipeline '"
                        + name + "' declares checkpointed(), but no checkpoint store is registered."
                        + " Checkpoints live in the MFT datasource, so this needs one configured. An"
                        + " operator may declare materialises() on its own, which gives it a workspace"
                        + " directory and makes its stage a segment boundary, at the cost of re-running"
                        + " the whole segment after a failure");
            }
            if (op.materialises() && workspaceRoot == null) {
                throw new StreamException("operator " + i + " ('" + op.name() + "') of pipeline '"
                        + name + "' materialises but no workspace is configured, so it has nowhere to"
                        + " write. Set '" + SynapseConstants.STREAM_WORKSPACE_ROOT + "' in"
                        + " synapse.properties");
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
    /**
     * Whether a run named {@code runId}, over a source with this {@code origin}, could resume.
     *
     * <p>Four conditions, and every one is necessary:
     * <ul>
     *   <li>something durable to resume <i>into</i> — a checkpoint or a materialised artifact;</li>
     *   <li>a {@code REOPENABLE} origin, or the bytes cannot be read a second time;</li>
     *   <li>a stable run id, which for a seed means the provider gave it an identity — a source that
     *       cannot be <i>recognised</i> cannot be resumed, however re-readable it is;</li>
     *   <li>the deployer not having declined it with {@code resume="false"}.</li>
     * </ul>
     */
    public boolean resumable(StreamOrigin origin, String runId, boolean runIdStable) {
        return hasDurableState() && origin == StreamOrigin.REOPENABLE && runIdStable && runId != null
                && resume;
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
     *   <li><b>The seed's identity</b>, when the caller supplied a stream instead and said what it is.
     *       Re-presenting the same source lands on the same workspace, so a re-poll continues rather
     *       than starting over. This needs no I/O — the provider computed the identity when it built
     *       the seed.</li>
     *   <li><b>A fresh id</b> otherwise — no seed, no identity on it, or a {@code ONE_SHOT} origin.
     *       Nothing stable was available, so nothing will resume: the run is scratch and its workspace
     *       is reclaimed when it ends.</li>
     * </ol>
     *
     * <p>The pipeline name is folded into case 2 so that two pipelines processing the same file
     * concurrently do not collide on one directory.
     *
     * @return the run id, and whether it is stable enough to resume from
     */
    RunId resolveRunId(JobContext jobCtx, StreamSeed seed, StreamOrigin origin)
            throws StreamException {
        String callerJobId = jobCtx.jobId();
        if (!JobContext.NOOP_JOB_ID.equals(callerJobId)) {
            // The only run id the framework does not mint itself, and it becomes a directory name that
            // a scratch run later deletes recursively. jobId()'s javadoc asks for a path-safe value;
            // nothing made it so, and the penalty for an empty one was deleting every run of this
            // pipeline. Checked before anything is created, so a refusal costs nothing.
            requirePathSegment("job id", callerJobId);
            return new RunId(callerJobId, true);
        }
        if (seed != null && seed.hasIdentity() && origin == StreamOrigin.REOPENABLE) {
            // Content-addressed: the same pipeline over the same source resumes into the same
            // workspace, and a different source lands somewhere else rather than resuming into stale
            // artifacts.
            //
            // Only the identity is hashed. This used to fold in the seed's size and lastModified as
            // well, which was a filesystem's notion of sameness promoted to a universal rule: an email
            // attachment has no modification time, so it hashed UNKNOWN and diluted an identity that
            // was already stronger than anything the composition could express. Deciding what
            // distinguishes two sources is the provider's job -- ADR-0033.
            String material = name + '\0' + seed.identity();
            return new RunId("src-" + Integer.toHexString(material.hashCode())
                    + '-' + Long.toHexString(fnv1a(material)), true);
        }
        return new RunId("run-" + UUID.randomUUID(), false);
    }

    /**
     * A failure message with the root cause appended, for the job record and the fault sequence.
     *
     * <p>{@code getMessage()} alone reads "stream pipeline 'x' failed at stage 'rows'", which says
     * where and not why. The why is always one or more causes down — a sequence that is not deployed,
     * a malformed record, a rejected write — and a caller formatting a response from
     * {@code ERROR_MESSAGE} sees only the outer layer.
     */
    private static String withRootCause(Throwable failure) {
        Throwable root = failure;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        if (root == failure) {
            return failure.getMessage();
        }
        return failure.getMessage() + " -- caused by " + root.getClass().getSimpleName() + ": "
                + root.getMessage();
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
     * Serves a stage's already-produced output, then its live output — one unbroken stream.
     *
     * <p>The prefix is opened on the <b>first read</b>, never during the build, so invariant 1 still
     * holds: obtaining an artifact does no I/O, and this does none until something pulls. That timing
     * also gets the ordering right for free — {@code openPrefix()} reconciles and truncates, and it
     * runs before the operator's own first read, which is where the operator would otherwise have
     * triggered the same reconciliation.
     *
     * <p>The seam is invisible downstream. The recorded artifact length is always the byte offset just
     * past a whole unit's output, because {@code unitDone} is called after {@code append}, so the
     * prefix can never end mid-record; and the operator resumes at the input position recorded with
     * that same length, so the first live byte is exactly the one that followed.
     *
     * <p>On a fresh run the prefix is empty and this costs one extra virtual call per read.
     */
    private static final class ArtifactPrefixStream extends InputStream {

        private final StageArtifact artifact;
        private final InputStream live;
        private InputStream prefix;
        private boolean prefixDone;

        private ArtifactPrefixStream(StageArtifact artifact, InputStream live) {
            this.artifact = artifact;
            this.live = live;
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            int n = read(one, 0, 1);
            return n == -1 ? -1 : one[0] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (len == 0) {
                return 0;
            }
            if (!prefixDone) {
                if (prefix == null) {
                    prefix = artifact.openPrefix();
                }
                int n = prefix.read(b, off, len);
                if (n != -1) {
                    return n;
                }
                prefix.close();
                prefixDone = true;
            }
            return live.read(b, off, len);
        }

        @Override
        public void close() throws IOException {
            try {
                if (prefix != null && !prefixDone) {
                    prefix.close();
                }
            } finally {
                live.close();
            }
        }
    }

    /** What invoked a run: a kind, and the invoking artifact's name when one could be identified. */
    record Invoker(String type, String name) {
    }

    /**
     * Works out what invoked this run, so nobody has to remember to say.
     *
     * <p>Synapse already stamps the invoking artifact onto the message context, and this reads the same
     * three properties, in the same order, as {@code TimeoutHandler} and {@code Axis2FlexibleMEPClient}
     * do when they answer the same question for a timeout or an outbound call. Ordering is theirs, not
     * ours: a proxy's own dispatch beats an API's, and an API beats the inbound endpoint that fed it.
     *
     * <p>All three are <b>Synapse</b> properties rather than Axis2 ones, so a plain
     * {@code getProperty} reads them.
     *
     * <h2>Why this is derived and not declared</h2>
     * The first version of this asked the caller, through {@code JobContext.flow()}. Nothing ever set a
     * {@code JobContext}, so every row recorded the same default and the column was worthless. A value
     * the framework can see for itself should never be a caller's obligation — see ADR-0030.
     *
     * <p>{@link JobContext} may still override, and one caller must: a message processor's context is
     * synthesised and names none of these artifacts.
     *
     * @param msg    the invoking message context, which may be {@code null}
     * @param jobCtx the caller's context, consulted first
     * @return the invoker; its type is never {@code null}, its name may be
     */
    private Invoker resolveInvoker(MessageContext msg, JobContext jobCtx) {
        String declaredType = trimToNull(jobCtx.invokerType());
        if (declaredType != null) {
            return new Invoker(declaredType, trimToNull(jobCtx.invokerName()));
        }

        if (msg != null) {
            String proxy = trimToNull(asString(msg.getProperty(SynapseConstants.PROXY_SERVICE)));
            if (proxy != null) {
                return new Invoker("PROXY", proxy);
            }
            // Carries "name:vX" for a versioned API, which is that API's real identity and is stored
            // as such — anything filtering on it later has to match the same string.
            String api = trimToNull(asString(msg.getProperty(RESTConstants.SYNAPSE_REST_API)));
            if (api != null) {
                return new Invoker("API", api);
            }
            String inbound = trimToNull(
                    asString(msg.getProperty(SynapseConstants.INBOUND_ENDPOINT_NAME)));
            if (inbound != null) {
                return new Invoker("INBOUND", inbound);
            }
        }

        // Reached by a pipeline run from a plain sequence, and by tests, which pass no context at all.
        return new Invoker("DIRECT", trimToNull(jobCtx.invokerName()));
    }

    private static String asString(Object value) {
        return value == null ? null : value.toString();
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
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

        Path runDir = prepareWorkspace(runId.value());

        // Every run is recorded, whether or not the caller wanted reporting: a partial answer to "what
        // is running" is worse than none. NOOP when no provider is registered, which is the normal
        // state for a Synapse with no datasource behind it.
        JobStore store = JobStores.storeFor(name);
        Invoker invoker = resolveInvoker(msg, jobCtx);
        JobRecord prior = store.start(new JobRun(runId.value(), name, invoker.type(), invoker.name(),
                resumable, runDir == null ? null : runDir.toString(),
                seed == null ? null : seed.identity(),
                seed == null ? StreamSeed.UNKNOWN : seed.size(),
                seed == null ? StreamSeed.UNKNOWN : seed.lastModified()));

        runDir = guardSourceIdentity(prior, seed, runId, runDir);

        // From here on the caller's context is wrapped, so an operator reporting progress writes a row
        // without knowing this happens. Both obligations are met by one call.
        RecordingJobContext recorder = new RecordingJobContext(jobCtx, store, runId.value());
        jobCtx = recorder;

        List<StageStream> stages = new ArrayList<>();
        StreamException primary = null;

        /**
         * Whether the pull ran to completion. Publishing is gated on this rather than on
         * {@code primary == null}, because an Error matches none of the catch clauses below and would
         * otherwise leave primary null on a run that plainly did not succeed — committing a partial
         * artifact that the next attempt reads as a finished segment. Any throwable that skips the
         * assignment aborts, without this class having to enumerate what those are.
         */
        boolean pulled = false;

        // The sink is not a StageStream — it consumes rather than producing a stream to decorate — so
        // its figures have to be gathered here or it gets no per-stage row at all.
        String sinkStage = null;
        long sinkElapsedNanos = -1L;

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
                String stage = stageNameAt(i, op);
                DefaultStreamContext ctx = contextFor(msg, jobCtx, resources, op, stage, runId.value(),
                        runDir, maxReprocessed(i));

                if (op instanceof StreamSink s) {
                    sink = s;
                    sinkCtx = ctx;
                    sinkResolution = resolution;
                    sinkStage = stage;
                    continue;                       // the sink wraps nothing; it reads
                }

                // A connector operation reads its configuration from the function stack, the ordinary
                // connector way, so its parameters are bound only for the duration of this call. The
                // operator must capture what it needs here: the stream it returns is read later, when
                // nothing is bound. For an SPI-built operator this is a no-op — its configuration is
                // already in its own fields.
                // Everything registered with the scope up to here belongs to a stage upstream of this
                // one. If this stage turns out to have resumed from an artifact, that is exactly the
                // set to release — see ResourceScope.closeNowRegisteredBefore.
                int upstreamMark = resources.registered();

                boolean bound = resolution.bind(msg);
                try {
                    if (op instanceof StreamSource src) {
                        current = src.open(ctx);
                        if (current == null) {
                            throw new StreamException("operator '" + stage + "' returned a null"
                                    + " stream from open(); emptiness is -1 on first read, not null")
                                    .withStage(stage);
                        }
                    } else {
                        InputStream upstream = current;
                        current = ((StreamTransform) op).wrap(upstream, ctx);
                        if (current == null) {
                            throw new StreamException("operator '" + stage + "' returned a null"
                                    + " stream from wrap()").withStage(stage);
                        }
                        if (current == upstream) {
                            throw new StreamException("operator '" + stage + "' returned its own"
                                    + " upstream from wrap(); a pass-through must still be a distinct"
                                    + " wrapper or per-stage accounting is wrong").withStage(stage);
                        }
                    }
                } catch (StreamException e) {
                    // Attributed here, because only this loop knows which stage it was building. The
                    // outer handler cannot: an unattributed StreamException reaching it is assumed to
                    // have come from the sink, which is right for the pull phase and wrong for this
                    // one -- a forEach refusing its configuration was reported against the file sink
                    // three stages away, which is worse than no attribution at all.
                    throw e.getStage() == null ? e.withStage(stage) : e;
                } catch (RuntimeException e) {
                    throw new StreamException("operator '" + stage + "' failed while being built", e,
                            false).withStage(stage);
                } finally {
                    if (bound) {
                        resolution.release(msg);
                    }
                }

                // A resumed materialising stage must hand its downstream the WHOLE output stream it
                // produced before, not only the part it is about to re-derive. The downstream counts
                // records from 1 and skips its own recorded position; given only the tail, that count
                // lands past the end or over different records and rows vanish with the run reporting
                // success.
                //
                // Done here rather than in the operator because every materialising transform needs
                // it and forgetting it is silent -- the same argument that put fsync-then-advance
                // inside unitDone. No operator calls openPrefix().
                if (op.materialises() && !(op instanceof StreamSink)) {
                    StageArtifact produced = ctx.canonicalArtifact();
                    if (produced != null) {
                        current = new ArtifactPrefixStream(produced, current);
                    }
                }

                // The operator may have short-circuited to an artifact a previous run left behind, in
                // which case everything built above it will never be read. Release it now rather than
                // holding a remote connection, unused, for the length of the run. Costs nothing to have
                // built, because open() and wrap() perform no I/O.
                if (ctx.hasResumedFromArtifact() && !stages.isEmpty()) {
                    // Recorded before the list is cleared, or a resumed run's per-stage table is
                    // simply missing rows for the segments it skipped -- sparse, and with nothing
                    // saying why. The status column already exists for this.
                    for (StageStream released : stages) {
                        recorder.recordSkipped(released.stage());
                    }
                    // By watermark, not by identity. The stages list holds StageStream decorators the
                    // pipeline built; the handles worth releasing are what the operators registered,
                    // which the scope knows and this class does not.
                    resources.closeNowRegisteredBefore(upstreamMark);
                    log.info("stream pipeline '" + name + "' resumed at stage '" + stage
                            + "' from an existing artifact; released " + upstreamMark
                            + " orphaned upstream resource(s) across " + stages.size() + " stage(s)");
                    stages.clear();
                    below = null;
                }

                below = new StageStream(current, stage, below);
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
                long sinkStartNanos = System.nanoTime();
                try {
                    sink.consume(current, sinkCtx);
                } finally {
                    // Measured even on failure: a sink that died after twenty minutes is exactly the
                    // thing someone wants to see afterwards.
                    sinkElapsedNanos = System.nanoTime() - sinkStartNanos;
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

            pulled = true;

        } catch (StageStream.StageIOException e) {
            primary = new StreamException("stream pipeline '" + name + "' failed", e,
                    retryable(e.getCause())).withStage(e.getStage());
        } catch (IOException e) {
            // Not attributed, so it can only have come from the sink — the one unwrapped position.
            primary = new StreamException("stream pipeline '" + name + "' failed", e, retryable(e))
                    .withStage(sinkName());
        } catch (StreamException e) {
            primary = e.getStage() == null ? e.withStage(sinkName()) : e;
        } catch (RuntimeException e) {
            primary = new StreamException("stream pipeline '" + name + "' failed unexpectedly", e);
        } catch (Throwable t) {
            // An Error: OutOfMemory, StackOverflow from a recursive mediation sequence, a
            // NoClassDefFoundError from a connector missing a transitive dependency. Record the run as
            // failed, then let it propagate UNWRAPPED — turning an OutOfMemoryError into a
            // StreamException would invite a caller to retry it. `pulled` is false, so the finally
            // aborts and nothing is published.
            // Logged here, not left to the caller. An Error propagates unwrapped, so nothing
            // downstream necessarily reports it either.
            log.error(LoggingUtils.getFormattedLog(SynapseConstants.STREAM_PIPELINE_TYPE, name,
                    "run '" + runId.value() + "' failed with an unrecoverable error"), t);
            recorder.recordFailure(sinkName(), "STREAM_PIPELINE_ERROR", t.toString(), false);
            throw t;
        } finally {
            try {
                if (pulled && primary == null) {
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
            reportStagesQuietly(jobCtx, stages, sinkStage, sinkElapsedNanos);
            reclaimScratchWorkspace(runDir, resumable, runId.value());
        }

        if (primary != null) {
            // Logged here rather than left to whoever catches it, and this is the whole reason:
            // execute() throws, the mediator rethrows as a SynapseException, and a fault sequence
            // that formats a response discards the cause chain entirely. The message a caller sees
            // names the stage; the reason the stage failed is only ever in the cause. A failed
            // transfer with nothing in the log is not diagnosable.
            log.error(LoggingUtils.getFormattedLog(SynapseConstants.STREAM_PIPELINE_TYPE, name,
                    "run '" + runId.value() + "' failed at stage '" + primary.getStage()
                            + "' (retryable=" + primary.isRetryable() + ")"), primary);
            recorder.recordFailure(primary.getStage(), "STREAM_PIPELINE_FAILED",
                    withRootCause(primary), primary.isRetryable());
            throw primary;
        }
        recorder.succeeded();
    }

    /**
     * Refuses, resets or ignores a run whose source is not the one the previous attempt read.
     *
     * <p>Only reached when a previous attempt recorded an identity, which in practice means a
     * caller-supplied job id: a content-addressed run folds the source's identity into its own name, so
     * a changed source already lands in a different workspace and has nothing to collide with.
     *
     * @return the workspace to use, which is a fresh one when a stale workspace was discarded
     * @throws StreamException under {@link SourceIdentityPolicy#STRICT}, or if the discard failed
     */
    private Path guardSourceIdentity(JobRecord prior, StreamSeed seed, RunId runId, Path runDir)
            throws StreamException {

        if (prior == null || seed == null || !prior.hasSourceIdentity()
                || sourceIdentity == SourceIdentityPolicy.OFF || sameSource(prior, seed)) {
            return runDir;
        }

        String detail = "run '" + runId.value() + "' of stream pipeline '" + name + "' previously read "
                + describeSource(prior.sourceId(), prior.sourceSize(), prior.sourceLastModified())
                + " but this attempt found "
                + describeSource(seed.identity(), seed.size(), seed.lastModified());

        if (sourceIdentity == SourceIdentityPolicy.STRICT) {
            throw new StreamException(detail + "; sourceIdentity=\"strict\" refuses to continue", false);
        }

        // Warning and then resuming anyway would splice two different sources into one artifact and
        // commit it as complete. The discard is the point of this branch, not a tidy-up after it.
        log.warn(detail + ": discarding the previous attempt's workspace and starting over, because"
                + " resuming would join bytes from two different sources into one artifact");
        discardWorkspace(runDir);
        clearCheckpoints(runId.value());
        return prepareWorkspace(runId.value());
    }

    /**
     * Forgets every checkpointed stage's position, so the segment restarts from clean.
     *
     * <p>Discarding the workspace alone was not enough. A stage declaring {@code checkpointed()} without
     * {@code materialises()} has no artifact, so {@code L <= A} could not catch a stale position: the
     * artifact was gone but the recorded byte offset survived, and the stage resumed at an offset into a
     * source it was no longer reading.
     *
     * @throws StreamException if a position could not be cleared — continuing would resume from a
     *                         position already known to be wrong
     */
    private void clearCheckpoints(String runId) throws StreamException {
        if (!CheckpointStores.isAvailable()) {
            return;
        }
        for (int i = 0; i < operators.size(); i++) {
            StreamOperator op = operators.get(i);
            if (!op.checkpointed()) {
                continue;
            }
            String stage = stageNameAt(i, op);
            try {
                CheckpointStores.storeFor(name, runId, stage).clear();
            } catch (IOException | RuntimeException e) {
                throw new StreamException("stream pipeline '" + name + "' found run '" + runId
                        + "' reading a different source, but could not clear stage '" + stage
                        + "''s checkpoint; refusing to resume from a position known to be wrong", e,
                        true);
            }
        }
    }

    /** Identity is all three fields: a same-sized rewrite at a new mtime is still a different source. */
    /**
     * Whether this attempt is reading what the previous one read.
     *
     * <p>Equality of two opaque strings, and nothing more. This used to compare the identity, the size
     * and the modification time separately, which made the framework hold a second identity policy
     * alongside the one in {@link #resolveRunId} — and a contradictory one. If a provider folds size
     * and mtime into its identity, comparing them again is unreachable; if it deliberately leaves them
     * out, comparing them overrides a decision the provider already made. Either way it was wrong, so
     * there is now exactly one place that decides what "the same source" means, and it is not here.
     */
    private static boolean sameSource(JobRecord prior, StreamSeed seed) {
        return Objects.equals(prior.sourceId(), seed.identity());
    }

    /**
     * A source for a human, in a message.
     *
     * <p>Size and modification time appear here and <b>only</b> here. With an opaque identity the
     * framework can no longer say <i>what</i> about a source changed, only that it did, so reporting
     * them alongside keeps the diagnostic actionable. Reported, never decided upon.
     */
    private static String describeSource(String id, long size, long lastModified) {
        return "'" + id + "' (size=" + (size == StreamSeed.UNKNOWN ? "unknown" : size)
                + ", lastModified=" + (lastModified == StreamSeed.UNKNOWN ? "unknown" : lastModified)
                + ")";
    }

    /**
     * Deletes a scratch run's workspace once the run has ended.
     *
     * <p>A scratch run is one nothing can ever resume — no stable identity to resume <i>into</i>, or a
     * one-shot origin with no second attempt to resume <i>from</i>. Its artifacts stop being useful the
     * moment the run ends, so leaving them accumulates data nothing tracks and nothing will ever read.
     * That is exactly what a 1 GB spill from a completed copy is.
     *
     * <p>Runs after the resource scope has been closed or aborted, so nothing here races an open
     * handle, and after reporting, so a stage's figures are recorded before its directory disappears.
     *
     * <h2>Nothing here asks what the pipeline produced</h2>
     * The workspace never holds output — ADR-0029. It is pipeline-owned space for restart state, and
     * durable results leave through a sink, to a path the deployer chose. So "was there a sink" is not
     * a question this needs to ask: a pipeline ending in a materialising transform is doing its work
     * through side effects, and its artifact is spill exactly like any other stage's.
     *
     * <p>The one case left alone is a <b>resumable</b> run, whose artifacts are resume candidates while
     * it is in flight. Once it has succeeded they are arguably garbage too, but that is retention
     * policy — an open question, and not one to decide from inside a run.
     *
     * <p>Never throws. The bytes have already landed and the run's outcome is settled; failing it now
     * over a directory that could not be removed would turn a successful transfer into a failed one.
     */
    private void reclaimScratchWorkspace(Path runDir, boolean resumable, String runId) {
        if (resumable) {
            return;
        }
        try {
            // Positions as well as files, and note this runs even when runDir is null. Checkpoints
            // live in the database, so discarding the directory does not touch them -- and a stage
            // that is checkpointed() without materialises() has no artifact for L <= A to catch a
            // stale position against. Exactly the reasoning already recorded in clearCheckpoints,
            // arrived at there for the source-identity guard.
            //
            // Reachable only since resume="false", which is the one way a run can be non-resumable
            // and still land on the same identity-derived run id next time; before it, every
            // non-resumable run got a fresh UUID and could never collide with its own leftovers.
            clearCheckpoints(runId);
            if (runDir != null) {
                discardWorkspace(runDir);
            }
            if (log.isDebugEnabled()) {
                log.debug("stream pipeline '" + name + "' reclaimed the scratch state of run '"
                        + runId + "'; nothing could have resumed from it");
            }
        } catch (StreamException | RuntimeException e) {
            log.warn("stream pipeline '" + name + "' could not reclaim the scratch state of run '"
                    + runId + "' at '" + runDir + "'. The transfer is unaffected, but a stale position"
                    + " or directory is left behind: a later run of the same source could resume from"
                    + " state this run meant to discard", e);
        }
    }

    /** Removes a run directory, whether stale, scratch, or belonging to a different source. */
    private void discardWorkspace(Path runDir) throws StreamException {
        if (runDir == null || !Files.exists(runDir)) {
            return;
        }
        try (Stream<Path> tree = Files.walk(runDir)) {
            tree.sorted(Comparator.reverseOrder()).forEach(entry -> {
                try {
                    Files.delete(entry);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (IOException e) {
            throw new StreamException("could not discard the stale workspace at '" + runDir + "'", e,
                    true);
        } catch (UncheckedIOException e) {
            throw new StreamException("could not discard the stale workspace at '" + runDir + "'",
                    e.getCause(), true);
        }
    }

    /**
     * Whether another attempt at this failure could plausibly succeed.
     *
     * <p>Every {@code IOException} from the chain used to be recorded as retryable, which is wrong in
     * the direction that hurts: {@code StreamException}'s own javadoc lists a truncated input, a wrong
     * decryption key and a failed integrity check as <b>not</b> retryable, and warns that a false
     * {@code true} "produces an infinite retry loop that presents as a hang". A processor re-queueing
     * off that verdict would retry a permanently corrupt archive forever.
     *
     * <p>An operator that knows better says so, by throwing a {@link StreamException} with its own
     * verdict; that is honoured ahead of anything guessed here. Otherwise the judgement is made on the
     * exception type, and the listed cases are the ones a second attempt cannot fix.
     */
    private static boolean retryable(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t instanceof StreamException streamException) {
                return streamException.isRetryable();
            }
            // Data that will not become valid by being read again.
            if (t instanceof java.util.zip.ZipException
                    || t instanceof java.nio.charset.CharacterCodingException
                    || t instanceof java.io.CharConversionException
                    || t instanceof java.io.EOFException) {
                return false;
            }
        }
        return true;
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
                                            ResourceScope resources, StreamOperator op, String stage,
                                            String runId, Path runDir, int maxReprocessed) {
        boolean mayUseWorkspace = op.materialises() || op.checkpointed();
        Path stageDir = (mayUseWorkspace && runDir != null) ? runDir.resolve(stage) : null;

        // Scoped to (run, stage): two stages resume independently, and two runs of one pipeline must
        // never see each other's positions.
        CheckpointStore store = op.checkpointed() && CheckpointStores.isAvailable()
                ? CheckpointStores.storeFor(name, runId, stage)
                : null;

        return new DefaultStreamContext(msg, job, resources, stage, mayUseWorkspace,
                op.checkpointed(), stageDir, store, maxReprocessed, scratchTarget(stage, runId, runDir));
    }

    /**
     * Where a stage's scratch directory should go — not created here, and not created at all unless
     * the stage asks.
     *
     * <p>Ungated by the capability flags, unlike the workspace: scratch is not durable state, and a
     * pure decorator that needs a working file should not have to claim {@code materialises()} to get
     * one. A configured root gets a parallel tree, which is the point of configuring it — scratch is
     * node-local, so it belongs on local disk rather than on the cluster mount.
     *
     * @return the target directory, or {@code null} to let the context fall back to a JVM temp
     *         directory because there is nowhere else
     */
    private Path scratchTarget(String stage, String runId, Path runDir) {
        if (scratchRoot != null) {
            return Paths.get(scratchRoot, name, runId, stage);
        }
        if (runDir != null) {
            return runDir.resolve(stage).resolve(".scratch");
        }
        return null;
    }

    /** This stage's identity — the configured name, falling back to the operator's own. */
    private String stageNameAt(int index, StreamOperator op) {
        String configured = index < stageNames.size() ? stageNames.get(index) : null;
        return (configured == null || configured.isEmpty()) ? op.name() : configured;
    }

    /**
     * Creates this run's directory, and the stage directory of every operator that will need one.
     *
     * <p>The layout is {@code <root>/<pipeline>/<runId>/<stage>/}. The pipeline level is not needed for
     * uniqueness — a run id is either a job id or a hash that already folds the pipeline name in — it is
     * there so the workspace can be operated: retention set per pipeline, one pipeline's leftovers
     * cleared without touching the rest, and a directory's owner readable from its path rather than by
     * inspecting it. A flat root is thousands of opaque run ids.
     *
     * <p>The cost, accepted: the pipeline name is now part of a durable path, so renaming a pipeline
     * orphans its in-flight runs. A rename already invalidated content-addressed run ids; this extends
     * that to job-id runs too.
     *
     * <p>Created up front rather than on first use, so a permissions mistake or an absent mount fails
     * where someone is looking instead of part-way through a transfer. A stage owns a <b>directory</b>,
     * not a file: a routing sink writing three outputs needs three artifacts, and a layout assuming
     * "stage N, artifact N" would have to be migrated to allow it.
     *
     * @return the run's directory, or {@code null} if nothing in this pipeline needs one
     */
    private Path prepareWorkspace(String runId) throws StreamException {
        if (workspaceRoot == null || !hasDurableState()) {
            return null;
        }
        Path runDir = Paths.get(workspaceRoot, name, runId);
        try {
            Files.createDirectories(runDir);
            for (int i = 0; i < operators.size(); i++) {
                StreamOperator op = operators.get(i);
                if (op.materialises() || op.checkpointed()) {
                    Files.createDirectories(runDir.resolve(stageNameAt(i, op)));
                }
            }
        } catch (IOException e) {
            throw new StreamException("stream pipeline '" + name + "' could not prepare its workspace"
                    + " under '" + runDir + "'", e, true);
        }
        return runDir;
    }

    /**
     * The terminal stage's name, for attributing a failure that arrived unattributed.
     *
     * <p>Reads the <b>configured</b> stage name, the same value every other durable record uses — the
     * workspace directory, the checkpoint key and the {@code MFT_JOB_STAGE} row. An earlier version
     * read {@code op.name()} instead, which for a connector operation is a class constant: a stage
     * configured as {@code <file.streamWrite name="out"/>} recorded its per-stage row under
     * {@code out} and its failure under {@code file.streamWrite}, so the two could not be joined and
     * the fault sequence saw a name the deployer never wrote.
     *
     * <p>It also cannot throw. {@code stageNames} is fixed at parse time, so unlike {@code op.name()}
     * there is no call here to fail while a failure is already being reported.
     */
    private String sinkName() {
        if (operators.isEmpty()) {
            return name;
        }
        int last = operators.size() - 1;
        return stageNameAt(last, operators.get(last));
    }

    /**
     * An operator's name, or a {@link StreamException} naming it by position.
     *
     * <p>Called once per operator at the top of {@link #validate()}, which is what entitles the rest
     * of this class to call {@code name()} directly. A name that is absent or that throws is a broken
     * operator, and the only useful time to say so is deployment: the value ends up naming a workspace
     * directory and keying both the checkpoint store and the per-stage job rows, so substituting
     * something plausible at run time would put two stages of the same class into one directory —
     * exactly the collision an explicit name exists to prevent.
     *
     * @param index the operator's position, which is the one identifier that needs no call
     * @param op    the operator
     * @return its name, guaranteed non-null and non-blank
     * @throws StreamException if {@code name()} is absent, blank, or throws
     */
    private String requireName(int index, StreamOperator op) throws StreamException {
        String operatorName;
        try {
            operatorName = op.name();
        } catch (RuntimeException e) {
            throw new StreamException("operator " + index + " of stream pipeline '" + name + "' ("
                    + op.getClass().getName() + ") threw from name(); an operator's name is read to"
                    + " build workspace paths and checkpoint keys, so it must be a plain accessor", e);
        }
        if (operatorName == null || operatorName.isBlank()) {
            throw new StreamException("operator " + index + " of stream pipeline '" + name + "' ("
                    + op.getClass().getName() + ") has no name(); it is needed to name a workspace"
                    + " directory and to key checkpoints");
        }
        return operatorName;
    }

    /**
     * Refuses a string that is about to become a single directory name.
     *
     * <p>Every value checked here ends up in {@code Paths.get} or {@code Path.resolve}, and some of
     * what is built there is later handed to a recursive delete. The rejections are not cosmetic:
     *
     * <ul>
     *   <li>{@code ""} — {@code Paths.get("/root", "pipe", "")} collapses to {@code /root/pipe}, so a
     *       run would take the whole pipeline's directory as its own and reclaiming it would delete
     *       every other run, including live ones.</li>
     *   <li>{@code ".."} or any segment containing a separator — escapes the directory it was meant to
     *       name. {@code Files.walk} does not normalise {@code ..} before deleting.</li>
     *   <li>Leading or trailing whitespace — two names that look identical resolve to different
     *       directories, or the same one, depending on the filesystem.</li>
     * </ul>
     *
     * @param what  how to describe the offending value in the message
     * @param value the candidate segment
     * @throws StreamException if it cannot safely be one path segment
     */
    private static void requirePathSegment(String what, String value) throws StreamException {
        if (value == null || value.isEmpty()) {
            throw new StreamException(what + " that is empty, which cannot name a directory");
        }
        if (!value.equals(value.trim())) {
            throw new StreamException(what + " with leading or trailing whitespace: '" + value + "'");
        }
        if (".".equals(value) || "..".equals(value)) {
            throw new StreamException(what + " of '" + value + "', which does not name a directory");
        }
        if (value.indexOf('/') >= 0 || value.indexOf('\\') >= 0 || value.indexOf('\0') >= 0) {
            throw new StreamException(what + " that cannot be used as a single path segment: '"
                    + value + "'");
        }
    }

    /**
     * An operator's name for a <b>teardown log line</b>, never for anything durable.
     *
     * <p>One caller: {@link #destroy()}, which has no stage index to hand and is already reporting a
     * failure it must not replace. A connector-backed stage is resolved from its template per run and
     * never cached, so the instance being destroyed need not be the instance {@link #validate()}
     * proved well-behaved — hence the defensive read, which does not stay quiet about it.
     *
     * <p>Everything durable uses {@link #stageNameAt}, including failure attribution. See
     * {@link #sinkName()} for why.
     */
    private String nameForDiagnosis(StreamOperator op) {
        try {
            String operatorName = op.name();
            if (operatorName != null && !operatorName.isBlank()) {
                return operatorName;
            }
            log.warn("operator " + op.getClass().getName() + " of stream pipeline '" + name + "' has"
                    + " no name() while a failure is being attributed; using its class name");
        } catch (RuntimeException e) {
            log.warn("operator " + op.getClass().getName() + " of stream pipeline '" + name + "' threw"
                    + " from name() while a failure is being attributed; using its class name", e);
        }
        return op.getClass().getSimpleName();
    }

    /**
     * Reports every stage, sink included. Telemetry must never fail a transfer, on either path.
     *
     * <p>The sink needs its own arithmetic because it is the one stage with no {@link StageStream}
     * around it: it consumes a stream rather than producing one, so there is nothing to decorate. Its
     * wall time is the whole run — everything upstream executes inside {@code consume()} — so its own
     * cost is that elapsed time minus the time the stage below it accumulated. Exactly the subtraction
     * {@link StageStream#selfNanos()} performs, one level further out.
     *
     * @param sinkStage        the sink's stage name, or {@code null} when the pipeline has no sink
     * @param sinkElapsedNanos wall time inside {@code consume()}, or negative if it never started
     */
    private void reportStagesQuietly(JobContext job, List<StageStream> stages, String sinkStage,
                                     long sinkElapsedNanos) {
        for (StageStream s : stages) {
            try {
                job.stageFinished(s.stage(), s.bytesIn(), s.bytes(), s.selfNanos());
            } catch (Throwable t) {
                log.warn("Failed to report stage '" + s.stage() + "'; continuing", t);
            }
        }

        if (sinkStage == null || sinkElapsedNanos < 0L) {
            return;
        }
        try {
            // The stage the sink read from. Empty only when a resume released every upstream stage.
            StageStream top = stages.isEmpty() ? null : stages.get(stages.size() - 1);
            long consumed = top == null ? 0L : top.bytes();
            long upstreamNanos = top == null ? 0L : top.totalNanos();

            // bytesOut equals bytesIn by construction, not by measurement: a sink writes to somewhere
            // this class cannot see, so the only honest figure is what it was handed.
            job.stageFinished(sinkStage, consumed, consumed,
                    Math.max(0L, sinkElapsedNanos - upstreamNanos));
        } catch (Throwable t) {
            log.warn("Failed to report sink stage '" + sinkStage + "'; continuing", t);
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
     * Where materialised artifacts are written beneath.
     *
     * <p>Normally set by {@link #init} from {@code synapse.properties}. Exposed so a programmatically
     * assembled pipeline — a test, chiefly — can give itself one without a properties file.
     */
    public void setWorkspaceRoot(String root) {
        this.workspaceRoot = root;
    }

    /**
     * Where stage scratch directories go, overriding the default of a temporary directory inside the
     * workspace.
     *
     * <p>Normally set by {@link #init} from {@code synapse.properties}. Exposed for the same reason
     * {@link #setWorkspaceRoot} is: a programmatically assembled pipeline needs one without a
     * properties file.
     */
    public void setScratchRoot(String root) {
        this.scratchRoot = root;
    }

    /**
     * Records that the structural rules have been checked.
     *
     * <p>Refuses a pipeline whose stages are still deferred, because {@code validated} is also what
     * {@link #bindDeferredStages} tests to decide whether binding is still owed. Setting it early would
     * skip binding altogether, leaving the operator list empty and the first read of it throwing
     * {@code IndexOutOfBoundsException} from somewhere that says nothing about the cause.
     *
     * @throws IllegalStateException if any stage is still reached through an unbound connector template
     */
    public void markValidated() {
        if (hasDeferredStages()) {
            throw new IllegalStateException("stream pipeline '" + name + "' still has stages to bind"
                    + " through a connector template, so it cannot be marked validated yet; validation"
                    + " happens in init(), after the library deployer has run");
        }
        this.validated = true;
    }

    /**
     * Adds an operator, with an explicit bound on how much work a failure may cost.
     *
     * @param op             the operator
     * @param nameIsExplicit whether its name was written down rather than derived from position
     * @param maxReprocessed the deployer's bound in the operator's checkpoint unit, or {@code null}
     *                       to take the operator's default
     * @throws IllegalArgumentException if the operator is null, the bound is below 1, or
     *                                  {@code name()} throws
     */
    public void addOperator(StreamOperator op, boolean nameIsExplicit, Integer maxReprocessed) {
        if (op == null) {
            throw new IllegalArgumentException("cannot add a null operator to pipeline '" + name + "'");
        }
        if (maxReprocessed != null && maxReprocessed < 1) {
            throw new IllegalArgumentException("maxReprocessed must be at least 1: " + maxReprocessed);
        }
        // Read once. This runs before validate(), so it is the earliest point a broken name() can be
        // reported, and the message has to carry the class because there is no name to identify it by.
        String operatorName;
        try {
            operatorName = op.name();
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("operator " + op.getClass().getName() + " threw from"
                    + " name() while being added to stream pipeline '" + name + "'; a name is read to"
                    + " build workspace paths and checkpoint keys, so it must be a plain accessor", e);
        }
        operators.add(op);
        entries.add(OperatorEntry.ofOperator(op, operatorName));
        explicitNames.add(nameIsExplicit);
        stageNames.add(operatorName);
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
                             String elementName, String stageName, boolean nameIsExplicit,
                             Integer maxReprocessed) {
        if (invoke == null) {
            throw new IllegalArgumentException("cannot add a null operation to pipeline '" + name + "'");
        }
        entries.add(OperatorEntry.ofOperation(invoke, elementName));
        explicitNames.add(nameIsExplicit);
        stageNames.add(stageName == null || stageName.trim().isEmpty() ? elementName : stageName.trim());
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
        List<OperatorEntry.Resolution> resolutions = resolveForRun();
        for (OperatorEntry.Resolution resolution : resolutions) {
            operators.add(resolution.operator());
        }
        try {
            validate();
            checkStageConfiguration(resolutions);
        } catch (StreamException e) {
            throw new SynapseException("stream pipeline '" + name + "' is not valid: "
                    + e.getMessage(), e);
        }
        validated = true;
        auditInfo("Successfully deployed Stream Pipeline: " + name + " with " + operators.size()
                + " operator(s)");
    }

    /**
     * Asks every stage whether the configuration it was given could work, before any file is touched.
     *
     * <h2>Why at deployment, and why it matters more than it looks</h2>
     * A connector operation reads its parameters from the bound message, so nothing would otherwise
     * look at them until a run started — and a typo would then surface as a failed transfer over a
     * real customer file. The cost is not just the wasted run. Once bytes are moving, the framework
     * <b>cannot tell a bad declaration from a bad file</b>: "this column will not parse as an integer"
     * has two explanations and no way to choose, so any action it takes — delete the file, retry
     * forever — is wrong half the time.
     *
     * <p>Deciding it here removes the question instead of answering it badly. A structural mistake is
     * decidable from the configuration alone, with no file in hand, so the artifact is faulty and
     * nothing runs.
     *
     * <p>Only literal parameters are offered, since an expression needs a message to evaluate. What
     * remains genuinely undecidable — a column correctly declared for a different file — stays
     * undecidable, and is the deployer's {@code ActionAfterFailure} to dispose of.
     */
    private void checkStageConfiguration(List<OperatorEntry.Resolution> resolutions)
            throws StreamException {
        for (int i = 0; i < resolutions.size(); i++) {
            OperatorEntry.Resolution resolution = resolutions.get(i);
            StreamOperator op = resolution.operator();
            try {
                op.validateConfiguration(resolution.literalParameters());
            } catch (StreamException e) {
                throw new StreamException("operator " + i + " ('" + stageNameAt(i, op)
                        + "') of stream pipeline '" + name + "' is misconfigured: " + e.getMessage(),
                        e, false).withStage(stageNameAt(i, op));
            }
        }
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

    /** What to do when a retry finds a different source than the attempt it resumes. */
    public SourceIdentityPolicy getSourceIdentity() {
        return sourceIdentity;
    }

    /** Whether the deployer permits resume. See the field's javadoc on why there is no way to force it. */
    public boolean isResume() {
        return resume;
    }

    /** @param resume {@code false} to make every run of this pipeline scratch */
    public void setResume(boolean resume) {
        this.resume = resume;
    }

    public void setSourceIdentity(SourceIdentityPolicy policy) {
        this.sourceIdentity = policy == null ? SourceIdentityPolicy.WARN : policy;
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
