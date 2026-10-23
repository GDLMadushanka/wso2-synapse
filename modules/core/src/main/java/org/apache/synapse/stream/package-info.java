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

/**
 * Contracts for {@code <streamSequence>} — a pull-based byte chain.
 * <p>
 * A sequence is an ordered list of {@link org.apache.synapse.stream.StreamOperator}s. Building the
 * chain moves no bytes: a {@link org.apache.synapse.stream.StreamSource} returns a stream it has
 * not read, each {@link org.apache.synapse.stream.StreamTransform} decorates the stream it was
 * given, and the {@link org.apache.synapse.stream.StreamSink} is the only participant that calls
 * {@code read()}. All execution happens inside that read loop, which is what makes backpressure a
 * property of the shape rather than a subsystem.
 * <p>
 * <h2>What is in here, and what is not</h2>
 * This package is the <b>operator contract</b>: what a connector author imports and nothing else.
 * {@link org.apache.synapse.stream.StreamOperator} and the three role interfaces, {@link
 * org.apache.synapse.stream.StreamContext}, {@link org.apache.synapse.stream.StageArtifact},
 * {@link org.apache.synapse.stream.Checkpoint}, {@link org.apache.synapse.stream.CheckpointUnit},
 * {@link org.apache.synapse.stream.ResourceScope}, {@link org.apache.synapse.stream.StreamCommit},
 * {@link org.apache.synapse.stream.StreamSeed}, {@link org.apache.synapse.stream.StreamOrigin},
 * {@link org.apache.synapse.stream.JobContext}, {@link org.apache.synapse.stream.StreamException}.
 * <p>
 * Two sub-packages hold what an operator cannot reach, so that this one reads as the surface an
 * operator actually faces rather than as everything the feature contains:
 * <ul>
 *   <li>{@link org.apache.synapse.stream.spi} — the stores the <b>product</b> implements and
 *       registers, and the records they carry. {@code JobContext} stays here because an operator is
 *       handed one; only the store behind it is there.</li>
 *   <li>{@link org.apache.synapse.stream.transfer} — the MFT submission queue, which references
 *       nothing in the byte chain and which nothing in the byte chain references.</li>
 * </ul>
 * {@code CheckpointStore} was operator-facing until ADR-0031 removed it from {@code StreamContext};
 * durable state now reaches an operator only through {@code StageArtifact}, which is why the store
 * moved to {@code spi} and {@code Checkpoint} did not. ADR-0062 records the split.
 *
 * <p><b>This package depends on nothing above it.</b> That is deliberate and worth preserving: it is
 * what lets an operator be written and unit-tested against these interfaces alone, with no
 * Synapse configuration machinery in the test. Layering runs strictly one way —
 * adapters &rarr; {@code stream.sequence} &rarr; {@code stream}.
 */
package org.apache.synapse.stream;
