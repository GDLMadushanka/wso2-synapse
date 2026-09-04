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

import java.nio.file.Path;

/**
 * Everything one operator gets, for one invocation.
 * <p>
 * Fixed for a single run. {@link #stageName()} is this operator's own name, so two operators in
 * the same run do not see the same value.
 *
 * <h2>Capability gating</h2>
 * {@link #workspace()} and the {@code artifact(...)} accessors are legal only for an operator that
 * declared the matching flag on {@link StreamOperator}. Calling one without the declaration is a
 * programming error and throws.
 * <p>
 * Note there is no accessor for the raw {@link CheckpointStore}. Durable state reaches an operator
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
     * bytes are: the payload is not built, and a pipeline exists precisely to avoid building it.
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
     * This operator's private directory inside the run's workspace.
     * <p>
     * A stage owns a <b>directory</b>, not a file, so an operator producing several artifacts —
     * a routing sink writing per-destination outputs, say — needs no change to the layout.
     * <p>
     * Write only here. Writing to a configured or hardcoded absolute path instead bypasses the
     * deployment-time workspace check, breaks isolation between runs, and makes retention
     * unmanageable.
     * <p>
     * <b>Prefer {@link #artifact()}.</b> Anything written through this path directly is outside the
     * run's commit and abort handling, so it is neither published on success nor discarded on
     * failure — which is the defect invariant 5 exists to prevent. Use it for genuinely auxiliary
     * files, and the artifact accessors for anything a later attempt might read.
     *
     * @return this stage's workspace directory; never {@code null}
     * @throws IllegalStateException if this operator declared neither
     *                               {@link StreamOperator#materialises()} nor
     *                               {@link StreamOperator#checkpointed()}
     */
    Path workspace();

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
     * This stage's artifact, for an operator that only {@code materialises()}.
     *
     * <p>Obtaining it performs no I/O, so this is safe to call from {@code open()} or {@code wrap()}.
     * Calling it twice returns the same instance — two writers on one file would be a bug the
     * framework has no way to detect later.
     *
     * <p>The name is framework-defined, {@code <workspace>/artifact.out}, and deliberately not the
     * operator's to choose: restart decides what is already done by looking for that file, so a
     * name picked by the operator would leave the pipeline guessing.
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
     * <p>Both arguments exist because a bare number is not a checkpoint. The {@code unit} says what
     * the position counts, which is not optional: {@code Reader.skip()} skips characters while
     * {@code InputStream.skip()} skips bytes, so the same stored number resumes to a different place
     * depending on how the input was read. The {@code formatVersion} is the operator's own tag, so a
     * later version of it can refuse a checkpoint it cannot interpret rather than misreading one.
     *
     * <p>An operator that keeps a position but writes no bytes uses this and never calls
     * {@code append} — no file is created, and its checkpoint records an artifact length of zero.
     *
     * @param unit          what the positions passed to {@link StageArtifact#unitDone} count
     * @param formatVersion the operator's own format tag; never {@code null} or blank
     * @return this stage's artifact; never {@code null}
     * @throws IllegalStateException if this operator did not declare
     *                               {@link StreamOperator#checkpointed()}
     */
    StageArtifact artifact(CheckpointUnit unit, String formatVersion);

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
     * How many checkpoint units a failure at this stage may cost — the deployer's {@code maxReprocessed},
     * or 1 when they set none.
     *
     * <p>A checkpointed operator must honour this: record a checkpoint at least every N units. It is the
     * width of the duplicate window and the only lever on per-unit durability cost, and it belongs to the
     * deployer alone — no operator declares anything about it.
     *
     * <p>Higher is not free in the other direction either. Between checkpoints the artifact needs no
     * durable flush, because resume truncates back to the last recorded length, so a wider bound removes
     * most of the {@code fsync} traffic on a shared mount.
     *
     * @return at least 1
     */
    default int maxReprocessed() {
        return 1;
    }

}
