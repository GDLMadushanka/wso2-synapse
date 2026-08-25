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
 * Base contract for a link in a {@code <streamPipeline>} byte chain.
 * <p>
 * An operator is <b>not a mediator</b>. It contributes a link to a lazy chain and does not run
 * when it is invoked; the pipeline's sink is the only participant that reads, and all execution
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
     * Whether this operator produces <b>byte-identical output for byte-identical input</b>.
     * <p>
     * Strictly that, and not "logically equivalent" or "the same records". Two mechanisms depend
     * on this flag and only the strict reading serves both: reuse of a spilled artifact needs
     * byte-identity, and record-count skip-forward needs record boundaries to fall in the same
     * places. An operator that emitted the same records in a different order would satisfy a
     * loose reading and would silently break resume, because "record 51" would be a different
     * record.
     * <p>
     * <b>This is the one method in the contract whose misuse causes silent data corruption
     * rather than an error.</b> A wrong {@code false} costs re-derivation; a wrong {@code true}
     * produces a plausible, complete, wrong result. Return {@code false} when uncertain.
     * <p>
     * Not deterministic, despite appearances: reading a remote path whose object may change;
     * stamping a timestamp, hostname or UUID; anything depending on the iteration order of an
     * unordered collection; anything whose output a remote call decides.
     * <p>
     * Any implementation returning {@code true} should state in its class javadoc why it is
     * entitled to.
     *
     * @return {@code true} only if output is byte-identical for identical input, always
     */
    default boolean deterministic() {
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
     * — on an input size threshold, say — because the pipeline's check is static and cannot see
     * the condition. Transient in-memory buffering is not materialisation.
     *
     * @return {@code true} if this operator writes a durable artifact
     */
    default boolean materialises() {
        return false;
    }
}
