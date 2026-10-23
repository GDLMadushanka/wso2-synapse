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

import org.apache.synapse.MessageContext;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Everything one operator gets, for one invocation.
 * <p>
 * Fixed for a single run. {@link #stageName()} is this operator's own name, so two operators in
 * the same run do not see the same value.
 *
 * <h2>Capability gating</h2>
 * The {@code artifact(...)} accessors are legal only for an operator that
 * declared the matching flag on {@link StreamOperator}. Calling one without the declaration is a
 * programming error and throws.
 * <p>
 * Note there is no accessor for the raw {@link org.apache.synapse.stream.spi.CheckpointStore CheckpointStore}. Durable state reaches an operator
 * only through {@link StageArtifact}, which fuses the write, the {@code fsync} and the position
 * advance into one call so they cannot be reordered — the coupling that both comparable frameworks
 * had to rewrite. Handing out the store separately would put that ordering back in an author's hands
 * for no capability they do not already have.
 * <p>
 * That gate is enforced at runtime rather than by the compiler, and the reason is deliberate. The
 * alternative — capability marker interfaces detected with {@code instanceof} — cannot be
 * forwarded by a wrapping operator, and wrapping operators are something we expect to write. A
 * flag forwards in one line; an {@code instanceof} does not. The runtime check is the price of
 * that, and it is a price other frameworks pay for the same reason.
 */
public interface StreamContext {

    /**
     * The message this run was invoked for.
     * <p>
     * Present so an operator can read configuration a caller left on the message and so a
     * connector operation can resolve a tenant-qualified connection. It is <b>not</b> where the
     * bytes are: the payload is not built, and a sequence exists precisely to avoid building it.
     *
     * @return the invoking message context; never {@code null}
     */
    MessageContext message();

    /**
     * The channel back to whoever asked for the transfer.
     *
     * @return the job context, or {@link JobContext#NOOP} if the caller supplied none; never
     *         {@code null}
     */
    JobContext job();

    /**
     * The run's resource scope. Register every handle you open here — including the stream you
     * return — rather than closing it yourself, because it must outlive the method that created
     * it.
     *
     * @return the run's resource scope; never {@code null}
     */
    ResourceScope resources();

    /**
     * This operator's own name, as reported by {@link StreamOperator#name()}.
     *
     * @return this stage's name; never {@code null}
     */
    String stageName();

    /**
     * This stage's named working files matching {@code glob}, in no particular order.
     *
     * <p>The way a stage finds what a previous attempt left behind. A merge sort lists its {@code run-*}
     * files to decide whether to resume the merge or start a fresh split, and {@link StageArtifact#discard()}
     * is how it clears them.
     *
     * <p><b>The canonical artifact is never returned</b>, whatever the glob says, and neither are the
     * {@code .part} files behind any artifact. Both belong to the framework: one is the stage's output and
     * the run decides whether it is published, the other is a write in progress.
     *
     * <p>{@code glob} is a file-name pattern within this stage's directory and nothing more — a separator,
     * a {@code ..} or a null byte is refused rather than resolved. There is deliberately no way to name a
     * directory, this one included: a stage can reach its own working files and nothing else.
     *
     * @param glob a file-name pattern such as {@code "run-*"}
     * @return the matching named artifacts; empty when none match or the stage has no workspace
     * @throws IllegalStateException if the operator declared neither {@code materialises()} nor
     *                               {@code checkpointed()}
     * @throws IOException           if the stage's directory could not be read
     */
    List<StageArtifact> artifacts(String glob) throws IOException;

    /**
     * A private directory for this stage's working files, <b>removed when the run ends</b>.
     *
     * <p>Yours entirely: create as many files as you like, open them all at once, seek, delete,
     * rename. The framework guarantees only that the directory exists when this returns and is gone
     * when the run finishes — on every path, success, failure or cancellation. That guarantee is the
     * reason to use this rather than {@code Files.createTempDirectory} by hand.
     *
     * <h2>Scratch is not restart state</h2>
     * Nothing resumes from it and nothing publishes it. If a later attempt might want to read a file,
     * it is not scratch — use {@link #artifact(String)} and {@link StageArtifact#seal()}, which keep
     * it and make it readable. The rule is: <b>if a later attempt might read it, the framework owns
     * it; if only this run reads it, you do.</b>
     *
     * <p>The classic use is an external merge sort's spilled runs where phase 1 is <i>not</i> meant to
     * be resumable — otherwise those belong in sealed named artifacts.
     *
     * <p>Location comes from {@code mft.scratch.root} when set, and a temporary directory inside the
     * workspace otherwise. Configuring it separately is worth doing on a cluster: scratch is
     * node-local by definition, and the workspace is usually a network mount.
     *
     * <p>Ungated by capability flags, because scratch is not durable state — a pure decorator that
     * needs a working file should not have to claim {@code materialises()} to get one.
     *
     * @return this stage's scratch directory; created if it did not exist
     * @throws java.io.UncheckedIOException if the directory could not be created
     */
    Path scratch();

    /**
     * Says what this stage is doing, for a human watching a transfer.
     *
     * <pre>
     *   ctx.status("splitting: sealed run 47, 3.2 GB read");
     * </pre>
     *
     * <h2>This is not a checkpoint. Nothing resumes from it.</h2>
     * The distinction matters more than anything else on this method, because the two read alike.
     * {@link StageArtifact#unitDone} is a <b>durability contract</b> — do the work, confirm it
     * landed, then advance — and a call made at the wrong moment is silent data loss.
     * {@code status} is telemetry: not durable, never read back, and discarding every call would
     * cost nothing but a blank console. If you are about to call this to record how far you got,
     * you wanted {@code unitDone}.
     *
     * <h2>Call it whenever you have something new to say</h2>
     * There is no cadence to respect and no rate limit to honour. A call sets a field and writes one
     * line; the database write behind it is throttled by the framework, so how often you call and how
     * much the deployment pays are independent. That is the whole point — an operator should never
     * have to choose between being informative and being fast.
     *
     * <p>What it should <i>not</i> be is a running commentary on work that is already visible. Bytes
     * flowing through a streaming stage are measured for you. This is for what the framework cannot
     * see: a stage that is busy without emitting anything. {@code streaming.sort} is the case it
     * exists for — its split phase reads an entire file before producing a byte, and without this
     * there is nothing to distinguish it from a hang.
     *
     * <h2>Where it goes</h2>
     * To {@code mft-transfer.log} on every call, prefixed with the run id and this stage; and to
     * {@code MFT_JOB_STAGE.ACTIVITY}, where the management API reads it. The log is the history, the
     * column is the current value, and there is no third store (ADR-0069).
     *
     * <p>Never throws, whatever happens underneath. A transfer must not fail because telemetry did.
     *
     * @param activity what this stage is doing, in the operator's own words; {@code null} or blank
     *                 is ignored
     */
    default void status(String activity) {
    }

    /**
     * This stage's artifact, for an operator that only {@code materialises()}.
     *
     * <p>Obtaining it performs no I/O, so this is safe to call from {@code open()} or {@code wrap()}.
     * Calling it twice returns the same instance — two writers on one file would be a bug the
     * framework has no way to detect later.
     *
     * <p>The name is framework-defined, {@code <workspace>/artifact.out}, and deliberately not the
     * operator's to choose: restart decides what is already done by looking for that file, so a
     * name picked by the operator would leave the sequence guessing.
     *
     * @return this stage's artifact; never {@code null}
     * @throws IllegalStateException if this operator declared neither {@code materialises()} nor
     *                               {@code checkpointed()}
     * @see StageArtifact
     */
    StageArtifact artifact();

    /**
     * This stage's artifact, for an operator that {@code checkpointed()}s — with or without an
     * artifact of its own.
     *
     * <p>All three arguments exist because a bare number is not a checkpoint. The {@code unit} says
     * what the position counts, which is not optional: {@code Reader.skip()} skips characters while
     * {@code InputStream.skip()} skips bytes, so the same stored number resumes to a different place
     * depending on how the input was read. The {@code formatVersion} is the operator's own tag, so a
     * later version of it can refuse a checkpoint it cannot interpret rather than misreading one.
     * The {@code checkpointFrequency} is the deployer's cadence, in that same unit.
     *
     * <h2>Why the operator passes the cadence in</h2>
     * It used to be a framework-reserved stage attribute the framework read for itself, which meant
     * it could not appear in a connector's parameter form — the tooling builds that form from the
     * template's parameters, and an attribute is invisible to it (ADR-0067). It is now an ordinary
     * parameter of the operator that counts the units, read and passed in here.
     *
     * <p>It is a required argument rather than an overload precisely so an operator cannot forget
     * it: a checkpointed stage with no cadence silently gets one checkpoint per unit, which is
     * correct and slow, and nobody would notice until a throughput complaint. The operator supplies
     * its own default when the deployer left the parameter out — conventionally 1, the cautious
     * value (ADR-0025).
     *
     * <p>An operator that keeps a position but writes no bytes uses this and never calls
     * {@code append} — no file is created, and its checkpoint records an artifact length of zero.
     *
     * @param unit                what the positions passed to {@link StageArtifact#unitDone} count
     * @param formatVersion       the operator's own format tag; never {@code null} or blank
     * @param checkpointFrequency how many of this stage's units may pass between checkpoints, and so
     *                            how many a failure may reprocess; values below 1 are read as 1
     * @return this stage's artifact; never {@code null}
     * @throws IllegalStateException if this operator did not declare
     *                               {@link StreamOperator#checkpointed()}
     */
    StageArtifact artifact(String unit, String formatVersion, int checkpointFrequency);

    /**
     * An additional artifact for this stage, named.
     *
     * <p>Two quite different things use this, and they are told apart by whether the operator calls
     * {@link StageArtifact#seal()}:
     *
     * <ul>
     *   <li><b>Another output.</b> A routing sink writing per-destination files. Left unsealed, so the
     *       run's outcome publishes or discards it exactly like the canonical artifact.</li>
     *   <li><b>A durable working file.</b> An external merge sort's sorted runs. {@code seal()}ed as
     *       each one is finished, which publishes it immediately so the merge phase can read it back
     *       <i>within this run</i> — and leaves it in place if the run fails, which is how the next
     *       attempt resumes instead of re-splitting the input.</li>
     * </ul>
     *
     * <p>For a file nothing will ever want again, use {@link #scratch()} instead: it needs no naming
     * decision and is removed on every exit path.
     *
     * <p><b>Only the canonical artifact may checkpoint</b> in this version: a checkpoint carries one
     * output length, and a stage with several outputs needs one length per output. That
     * generalisation is anticipated and not built, so a named artifact supports {@code append} and
     * not {@link StageArtifact#unitDone}.
     *
     * @param name file name within this stage's directory; a single path segment
     * @return the named artifact; never {@code null}
     * @throws IllegalStateException if this operator declared neither {@code materialises()} nor
     *                               {@code checkpointed()}
     */
    StageArtifact artifact(String name);

    /**
     * The byte offset at which this stage's upstream stream begins. Zero unless a resume moved it.
     *
     * <p><b>One number, read by both ends</b>, which is what keeps them from disagreeing. A seekable
     * source starts here. A sink skips {@code itsOwnResumePosition - upstreamStartsAt()}, which is
     * naturally zero when the source honoured the offset and the full prefix when it did not — so the
     * same code is correct either way and no second flag has to be kept in step.
     *
     * <p>The framework only moves it when every stage between source and sink preserves byte offsets.
     * Today that means a chain of exactly two: a transform that changes volume makes the source's
     * offsets and the sink's unrelated, and pushing one then would corrupt the output silently.
     */
    default long upstreamStartsAt() {
        return 0L;
    }


}
