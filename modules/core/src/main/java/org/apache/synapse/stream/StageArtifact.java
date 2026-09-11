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

import java.io.IOException;
import java.io.InputStream;

/**
 * A stage's durable state: the artifact it writes, the position it resumes from, and the coupling
 * between them.
 *
 * <h2>Why this exists rather than a {@code Path} and a {@link CheckpointStore}</h2>
 * Those two primitives left <b>six</b> obligations with the operator author, every one of them
 * unenforced and every one of them silent when broken: write only under the stage directory, write to
 * a temporary name, keep the final name stable across runs, {@code fsync} before the position moves,
 * truncate to the recorded length on resume, and register with the scope so a failed run publishes
 * nothing. The first operator written against them — a byte-for-byte tee that parses nothing —
 * needed about 130 lines to satisfy them, and every line of it was identical for every other
 * operator that would ever materialise.
 * <p>
 * That is the shape of a defect other frameworks have shipped. Spring Batch expresses state-keeping
 * as an interface a component implements, auto-registers components that implement it directly, and
 * <b>silently loses restart state for delegates</b> that a composing writer forgot to register. The
 * mechanism worked; the second implementer did not wire it. Handing out a facade the framework
 * already registered removes the opportunity.
 *
 * <h2>The ordering contract is inside {@link #unitDone}, not around it</h2>
 * A checkpoint must never be ahead of the effect it describes:
 * <pre>
 *   1. do the work        (side effects happen)     ← the operator's, and only the operator's
 *   2. confirm it landed  (fsync, not flush)        ← here
 *   3. advance the position                         ← here, and only after 2
 * </pre>
 * Steps 2 and 3 happen inside one call, in that order, so <b>an operator cannot reorder them</b>. A
 * checkpoint ahead of its effect is silent data loss; behind it is at most a bounded replay. Both
 * frameworks that had to rewrite their resume machinery rewrote this coupling rather than their data
 * model, which is why it is worth taking out of an author's hands.
 * <p>
 * That leaves exactly one obligation the framework cannot discharge, and it is stated once here:
 * <b>do not call {@link #unitDone} until the work it describes has actually happened.</b> Only the
 * operator knows when its backend call returned or its record was accepted. This is the same line
 * Kafka Connect had to retrofit in KIP-89 — separate "my data is durable" from "commit my position"
 * at the boundary where the component has knowledge, and fuse them below it.
 *
 * <h2>Three shapes, matching the three flag combinations</h2>
 * <table border="1">
 *   <caption>What an operator uses, by what it declared</caption>
 *   <tr><th>Declares</th><th>Obtained with</th><th>Uses</th></tr>
 *   <tr><td>{@code materialises()} only</td><td>{@link StreamContext#artifact()}</td>
 *       <td>{@link #append}; the framework renames on success and discards on failure</td></tr>
 *   <tr><td>{@code checkpointed()} only</td>
 *       <td>{@link StreamContext#artifact(CheckpointUnit, String)}</td>
 *       <td>{@link #unitDone} alone. Nothing is appended, so no file is ever created</td></tr>
 *   <tr><td>both</td><td>{@link StreamContext#artifact(CheckpointUnit, String)}</td>
 *       <td>both, and {@link #resumePosition} to skip its input on a retry</td></tr>
 * </table>
 *
 * <h2>Lifecycle</h2>
 * Obtaining one performs <b>no I/O</b>, so it is safe to call from {@code open()} or {@code wrap()}
 * where invariant 1 applies. The filesystem is touched on the first call to {@link #isComplete},
 * {@link #resumePosition}, {@link #append} or {@link #unitDone} — see the note on invariant 1 in
 * {@link #isComplete()}.
 * <p>
 * Publishing is the run's decision, not the operator's. There is deliberately no {@code commit()}
 * here: the framework registers this with the run's {@link ResourceScope}, so a successful run
 * renames the artifact into place and a failed one deletes it. An operator that could publish could
 * publish a partial artifact, and the next attempt would read its presence as a finished segment.
 *
 * @see StreamContext#artifact()
 * @see CheckpointStore
 */
public interface StageArtifact {

    /**
     * Whether a previous run finished writing this artifact.
     * <p>
     * True only when the file exists under its <b>final</b> name, which — because the framework
     * publishes by rename — means it is whole. A partial write is never visible here.
     * <p>
     * <b>On invariant 1.</b> This stats the workspace, and invariant 1 otherwise forbids an operator
     * touching the filesystem during the build phase. The carve-out is deliberate and narrow: resume
     * has to be decided while the chain is being built, a local or mounted {@code stat} holds no
     * resource and costs microseconds, and the invariant exists to stop a build holding live remote
     * connections rather than to forbid metadata reads. Nothing else about invariant 1 changes.
     *
     * @return {@code true} if this stage's artifact is already complete
     * @throws IOException if the workspace could not be read
     */
    boolean isComplete() throws IOException;

    /**
     * Opens the completed artifact. The returned stream is registered with the run's resource scope.
     *
     * <h2>Only the canonical artifact reports a short-circuit</h2>
     * Opening {@link StreamContext#artifact() the canonical artifact} tells the pipeline this
     * <b>stage</b> produced its output from a previous run, which releases the upstream stages built
     * above it that will now never be read. Doing that here rather than asking the operator to
     * remember is the point: forgetting it holds a remote connection open and unread for the length of
     * the run, with nothing to indicate it.
     * <p>
     * Opening a {@link StreamContext#artifact(String) named} file reports nothing, because it means
     * nothing about the stage. An external merge sort reads its sealed runs back while phase 1 is
     * still consuming the input — treating that as a short-circuit would release the upstream it is
     * reading from.
     *
     * @return a stream over the finished artifact
     * @throws IOException           if it could not be opened
     * @throws IllegalStateException if the artifact is not complete; check {@link #isComplete()}
     */
    InputStream openComplete() throws IOException;

    /**
     * Where to resume this stage's <b>input</b>, in the unit this artifact was obtained with.
     * <p>
     * Reads the stored checkpoint, reconciles it against the artifact on disk, and truncates the
     * partial artifact back to the recorded length — all before returning, so the operator never sees
     * an inconsistent pair. What is returned is the input position the operator should skip to;
     * honouring it is the operator's job, because only the operator knows where its record boundaries
     * are.
     *
     * <h2>{@code L <= A}</h2>
     * The checkpoint records an artifact length {@code L}; the partial file has an actual length
     * {@code A}. The checkpoint is taken only when {@code L <= A}. A checkpoint ahead of the data
     * describes bytes that are not there, so it is discarded and the stage starts over rather than
     * resuming on top of a gap. With the mandatory {@code fsync} in {@link #unitDone} this should
     * never happen, so it is a safety net against storage that acknowledges writes it has not
     * committed — which mount options make a real possibility rather than a theoretical one.
     * <p>
     * Note its limit: it compares <b>lengths</b>, so it cannot detect a file of the right size with a
     * gap in it. Only a checksum could, and there is not one.
     *
     * @return the input position to resume from, or {@code 0} for a fresh start
     * @throws IOException if the stored position could not be read, or the artifact could not be
     *                     truncated — neither may be guessed past
     */
    long resumePosition() throws IOException;

    /**
     * Opens the part of this artifact a previous attempt already produced, so a resumed stage can
     * hand its downstream the <b>whole</b> output stream rather than only the tail.
     *
     * <h2>Why the framework calls this and an operator does not</h2>
     * A downstream stage counts records of its input from 1 and skips its own recorded position. If a
     * resumed stage emitted only what it re-derives, that count would land in the wrong place — past
     * the end of a short stream, or over different records — and rows would vanish with the run
     * reporting success. Every materialising transform needs this, forgetting it is silent data loss,
     * and by the argument in this interface's own javadoc that makes it the framework's obligation
     * rather than an author's. {@code StreamPipeline} concatenates it ahead of the operator's stream;
     * no operator calls it.
     *
     * <h2>It reconciles first</h2>
     * The reconciliation and truncation of {@link #resumePosition} happen here too, and happen
     * <b>before</b> any byte is served — the two are the same {@code start()}, so whichever is reached
     * first does the work and the other sees the result. That ordering is required: serving bytes past
     * the recorded length would hand a downstream stage output whose position was never recorded.
     *
     * <p>The returned stream is <b>bounded</b> to the reconciled length. The operator appends past it
     * through a separate handle while this is being read, and those bytes must not appear here — they
     * are the same bytes the operator is about to produce again.
     *
     * <p>Performs I/O, so unlike {@link #isComplete()} it must not be called during the build phase —
     * see invariant 1. The pipeline defers it to the first {@code read()}.
     *
     * @return a stream over the already-produced output; empty when there is none
     * @throws IOException if the artifact could not be reconciled or opened
     */
    InputStream openPrefix() throws IOException;

    /**
     * Appends one byte to the artifact. Buffered; not durable until {@link #unitDone} says so.
     *
     * @param b the byte
     * @throws IOException if the write failed
     */
    void append(int b) throws IOException;

    /**
     * Appends to the artifact. Buffered; not durable until {@link #unitDone} says so.
     *
     * @param b   source buffer
     * @param off offset into {@code b}
     * @param len number of bytes
     * @throws IOException if the write failed
     */
    void append(byte[] b, int off, int len) throws IOException;

    /**
     * Reports that the operator has finished the unit ending at {@code position}, and advances the
     * durable position if the deployer's bound says it is due.
     * <p>
     * {@code position} is <b>cumulative</b> in this artifact's {@link CheckpointUnit} — record 51,
     * byte 4096 — not a delta. It must never go backwards.
     *
     * <h2>What this does, in order, and why the order is not negotiable</h2>
     * When a checkpoint is due it flushes the artifact, {@code fsync}s it, and only then records
     * {@code (position, artifact length, unit, format version)}. When one is not due it returns
     * having done nothing durable, which is exactly what the deployer asked for: the bytes appended
     * since the last checkpoint are truncated away on resume, so they need no {@code fsync} of their
     * own. That is the whole of why {@code maxReprocessed} is the only lever on throughput here.
     *
     * <h2>Cadence, and why coarse reporting is safe</h2>
     * A checkpoint is due when {@code position} has advanced by at least the stage's
     * {@code maxReprocessed} since the last one. An operator that reports finely gets exactly the
     * bound the deployer asked for. An operator that reports coarsely — a byte copier calling once
     * per block — checkpoints on every call, which is as close to the bound as its own granularity
     * allows, and the framework logs once when it cannot honour the bound rather than silently
     * missing it.
     * <p>
     * This is also what stops the obvious pathology: counting a bound of 1 in bytes and calling per
     * byte is an {@code fsync} per byte. Report per unit you would actually be willing to redo.
     *
     * @param position cumulative input position now complete; must not decrease
     * @throws IOException              if the artifact or the position could not be made durable — in
     *                                  which case the caller must treat the position as not advanced
     * @throws IllegalStateException    if this artifact was obtained without a unit and format
     *                                  version, or the operator did not declare {@code checkpointed()}
     * @throws IllegalArgumentException if {@code position} is negative or moves backwards
     */
    void unitDone(long position) throws IOException;

    /**
     * As {@link #unitDone(long)}, carrying operator-owned state alongside the position.
     *
     * <p>For a resume position that is not a single number — an external merge sort's phase, a
     * routing sink's per-destination offsets. See {@link Checkpoint} for why a bare integer is not
     * enough and what the payload may hold.
     *
     * <p>The payload is <b>replaced</b> on every call, not merged. An operator that writes it on some
     * calls and not others ends up with whatever the last call said, so pass it every time or not at
     * all.
     *
     * @param position cumulative input position now complete; must not decrease
     * @param payload  operator-owned state, or {@code null}; at most
     *                 {@value Checkpoint#MAX_PAYLOAD_BYTES} bytes of UTF-8
     * @throws IOException              if the artifact or the position could not be made durable
     * @throws IllegalArgumentException if the payload exceeds the cap — it is refused, not truncated
     */
    void unitDone(long position, String payload) throws IOException;

    /**
     * The operator-owned state stored with the checkpoint this attempt is resuming from.
     *
     * <p>{@code null} means <b>no checkpoint</b>, which is also how an operator tells a fresh run
     * from a resumed one at position 0.
     *
     * @return the stored payload, or {@code null} if there is none
     * @throws IOException if the stored position could not be read
     */
    String resumePayload() throws IOException;

    /**
     * Bytes in this artifact — appended by this attempt plus whatever a previous one left after
     * truncation.
     *
     * <p>This is the figure the framework records as {@code outputLength}, exposed so an operator can
     * report progress without counting separately.
     *
     * @return the artifact's length in bytes; {@code 0} if nothing has been written
     * @throws IOException if the stored position could not be read
     */
    long length() throws IOException;

    /**
     * Finishes a <b>named</b> working file now, making it complete and readable within this run.
     *
     * <p>Flushes, {@code fsync}s and renames it into place, exactly as a successful run would — but
     * immediately, rather than at the end of the run. After this it answers {@link #isComplete()} and
     * can be reopened with {@link #openComplete()}; further {@link #append} is refused.
     *
     * <h2>Why this exists — two commit policies, not one</h2>
     * The canonical artifact publishes only if the <b>whole run</b> succeeded. That is invariant 5:
     * a partial output renamed into place is read by the next attempt as a finished segment.
     * <p>
     * A named working file is the opposite case. An external merge sort writes N sorted runs and then
     * merges them, so it must <b>read back within the same run</b> what it has just written — and if
     * the run fails, the completed runs surviving is precisely how the next attempt resumes instead of
     * re-splitting the input. Deferring their rename to the end of the run makes both impossible.
     * <p>
     * Invariant 5 is not weakened, because nothing downstream ever reads a named working file as the
     * stage's output. Only the operator that wrote it looks at it.
     *
     * <h2>Consequences an operator should know</h2>
     * A sealed file is <b>no longer discarded when the run fails</b> — that is the point of sealing,
     * and it means the operator owns deciding when a file is worth keeping. It also survives a
     * successful run, so the stage directory accumulates working files until workspace retention
     * removes the run.
     *
     * @throws IOException           if the file could not be made durable or renamed
     * @throws IllegalStateException if called on the canonical artifact rather than a named one
     */
    void seal() throws IOException;
}
