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
 * Contracts for {@code <streamPipeline>} — a pull-based byte chain.
 * <p>
 * A pipeline is an ordered list of {@link org.apache.synapse.stream.StreamOperator}s. Building the
 * chain moves no bytes: a {@link org.apache.synapse.stream.StreamSource} returns a stream it has
 * not read, each {@link org.apache.synapse.stream.StreamTransform} decorates the stream it was
 * given, and the {@link org.apache.synapse.stream.StreamSink} is the only participant that calls
 * {@code read()}. All execution happens inside that read loop, which is what makes backpressure a
 * property of the shape rather than a subsystem.
 * <p>
 * <h2>Two audiences in one package</h2>
 * Worth knowing which types are which, because the answer moved when {@code StageArtifact} landed:
 * <ul>
 *   <li><b>Operator-facing</b> — what a connector author imports. {@link
 *       org.apache.synapse.stream.StreamOperator} and the three role interfaces, {@link
 *       org.apache.synapse.stream.StreamContext}, {@link org.apache.synapse.stream.StageArtifact},
 *       {@link org.apache.synapse.stream.Checkpoint}, {@link
 *       org.apache.synapse.stream.CheckpointUnit}, {@link org.apache.synapse.stream.ResourceScope},
 *       {@link org.apache.synapse.stream.StreamCommit}, {@link
 *       org.apache.synapse.stream.StreamSeed}, {@link org.apache.synapse.stream.StreamOrigin},
 *       {@link org.apache.synapse.stream.JobContext}, {@link
 *       org.apache.synapse.stream.StreamException}.</li>
 *   <li><b>Product-implemented</b> — seams the runtime fills in, which no operator can reach.
 *       {@link org.apache.synapse.stream.JobStore}, {@link org.apache.synapse.stream.JobStores},
 *       {@link org.apache.synapse.stream.JobRun}, {@link org.apache.synapse.stream.JobRecord},
 *       {@link org.apache.synapse.stream.CheckpointStore}, {@link
 *       org.apache.synapse.stream.CheckpointStores}.</li>
 * </ul>
 * {@code CheckpointStore} belonged to the first group until ADR-0031 removed it from {@code
 * StreamContext}; durable state now reaches an operator only through {@code StageArtifact}. The two
 * groups are left in one package deliberately — see the note in docs/roadmap.md.
 *
 * <p><b>This package depends on nothing above it.</b> That is deliberate and worth preserving: it is
 * what lets an operator be written and unit-tested against these interfaces alone, with no
 * Synapse configuration machinery in the test. Layering runs strictly one way —
 * adapters &rarr; {@code stream.pipeline} &rarr; {@code stream}.
 */
package org.apache.synapse.stream;
