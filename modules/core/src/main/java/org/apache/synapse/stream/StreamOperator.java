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
 * Base contract for a link in a {@code <streamSequence>} byte chain.
 * <p>
 * An operator is <b>not a mediator</b>. It contributes a link to a lazy chain and does not run
 * when it is invoked; the sequence's sink is the only participant that reads, and all execution
 * happens inside that read loop.
 * <p>
 * Every operator implements <b>exactly one</b> of {@link StreamSource}, {@link StreamTransform}
 * or {@link StreamSink}. Position is one axis; the three flags below are a second, orthogonal
 * one describing <i>behaviour</i>. Behaviour classes are inferred combinations of the flags,
 * never declared types.
 *
 * <h2>Unenforced rules this contract depends on</h2>
 * <ol>
 *   <li><b>{@code open()} and {@code wrap()} must perform no I/O.</b> The build loop runs every
 *       operator before a byte moves, and on a resumed run the chain it built may be discarded
 *       entirely.</li>
 *   <li><b>The build loop never skips.</b> Do not assume that being constructed means your
 *       stream will be read.</li>
 *   <li><b>One class must not implement two role interfaces.</b> Dispatch tests
 *       {@code instanceof StreamSource} before position, so a multi-role operator mid-chain
 *       would silently discard its upstream. {@code validate()} rejects this.</li>
 *   <li><b>Operators must be stateless.</b> One instance serves concurrent runs. All per-run
 *       state belongs in the returned stream or in the {@link StreamContext}, never in fields.
 *       Fields hold immutable configuration only.</li>
 * </ol>
 *
 * @see StreamSource
 * @see StreamTransform
 * @see StreamSink
 */
public interface StreamOperator {

    /**
     * Stable identity for this operator, used for telemetry labels, fault attribution, the
     * operator's workspace directory name and its checkpoint key.
     * <p>
     * Must be non-null, non-empty, and contain no path separator, NUL, or leading/trailing
     * whitespace. Two calls on the same instance must return equal values.
     * <p>
     * An operator declaring {@link #checkpointed()} or {@link #materialises()} must be given an
     * <b>explicit</b> name in configuration rather than one derived from its position in the
     * chain: checkpoints are keyed by name, and a chain index stops being meaningful the moment
     * a stage can appear in more than one branch.
     *
     * @return this operator's name; never {@code null} or empty
     */
    String name();

    /**
     * Checks the configuration a deployer wrote, at <b>deployment</b>, before any file is touched.
     *
     * <h2>Why this exists</h2>
     * A connector operation reads its parameters from the bound message, so by default nothing looks
     * at them until a run starts — and a typo then surfaces as a failed transfer over a real customer
     * file. Worse, it surfaces as a failure the framework cannot attribute: once bytes are moving,
     * "this column will not parse as an integer" is indistinguishable from a bad file, and guessing
     * wrong either deletes data or retries forever.
     *
     * <p>The fix is to make that class of error impossible to reach. A structural mistake — an
     * unknown {@code format}, malformed {@code dataTypes} JSON, {@code chunk} mode without a
     * {@code chunkSize} — is decidable from the configuration alone, with no file in hand. Decided
     * here, the artifact is faulty and nothing runs; left to runtime, it becomes a question about the
     * customer's data that has no answer.
     *
     * <h2>What it is given, and what it is not</h2>
     * Only parameters whose value is <b>literal</b> in the artifact. A parameter written as an
     * expression cannot be evaluated without a message, so it is absent from the map rather than
     * present-and-wrong, and an implementation must tolerate that: treat a missing key as "cannot
     * check", never as "not configured". The same {@code from(...)} the operator uses at runtime
     * should do the work, so the two cannot drift.
     *
     * @param literalParameters the operation's literally-configured parameters, by name
     * @throws StreamException if the configuration cannot produce a working run
     */
    default void validateConfiguration(java.util.Map<String, String> literalParameters)
            throws StreamException {
    }

    /**
     * Whether this operator produces <b>byte-identical output for byte-identical input</b>.
     *
     * <p>Strictly that, and not "logically equivalent". The framework uses it for one thing: a stage
     * that {@link #checkpointed()} may only sit downstream of a {@link #materialises()} stage that is
     * deterministic, because a failure re-derives the materialising stage's unpersisted tail and, if
     * that tail may differ, the checkpointed stage's recorded position would skip records of different
     * bytes — silently, and undetectably afterwards (ADR-0034).
     *
     * <p>So a wrong {@code true} does not fail a transfer. It produces a plausible, complete, wrong
     * output file. {@code false} costs a re-run; there is no symmetric cost to being wrong the other
     * way, which is why the default is {@code false} and why an operator that cannot be sure must
     * leave it alone.
     */
    default boolean deterministic() {
        return false;
    }

    /**
     * Whether the answer to {@link #deterministic()} belongs to the <b>deployer</b> rather than to this
     * operator.
     *
     * <p>For most operators it does not: what they do is fixed in their own code, so they can answer,
     * and a deployer asserting otherwise would be overriding a fact with a guess. But an operator whose
     * output is produced by <i>deployer-supplied configuration</i> genuinely cannot answer — a
     * per-record loop that runs a named sequence emits whatever that sequence emits, and the sequence
     * may call a backend, read a clock or generate an id. The operator sees a name; the deployer wrote
     * the mediators.
     *
     * <p>Declaring this lets a {@code deterministic="true|false"} stage attribute settle it, and the
     * attribute is <b>refused at deployment on any operator that does not</b> — so the choice is
     * offered exactly where someone is in a position to make it. The same question ADR-0025 settled for
     * {@code checkpointFrequency}: not "is this configurable" but "who can possibly know".
     *
     * <p>Declaring it changes nothing on its own. With no attribute, {@link #deterministic()} still
     * decides.
     */
    default boolean defersDeterminismToDeployer() {
        return false;
    }

    /**
     * Whether this operator keeps durable position state and can resume part-way through.
     * <p>
     * Declaring this entitles the operator to {@link StreamContext#checkpointStore()} and
     * obliges it to honour the ordering contract described there: do the work, confirm it
     * landed, <i>then</i> advance the checkpoint. Never fused, never reordered.
     *
     * @return {@code true} if this operator checkpoints its position
     */
    default boolean checkpointed() {
        return false;
    }

    /**
     * Whether this operator writes a durable artifact under its workspace directory.
     * <p>
     * Declaring this entitles the operator to {@link StreamContext#workspace()} and makes it the
     * last stage of its restart segment. Return {@code true} even when the write is conditional
     * — on an input size threshold, say — because the sequence's check is static and cannot see
     * the condition. Transient in-memory buffering is not materialisation.
     *
     * @return {@code true} if this operator writes a durable artifact
     */
    default boolean materialises() {
        return false;
    }
}
