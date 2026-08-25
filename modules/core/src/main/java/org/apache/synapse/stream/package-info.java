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
 * <b>This package depends on nothing above it.</b> That is deliberate and worth preserving: it is
 * what lets an operator be written and unit-tested against these interfaces alone, with no
 * Synapse configuration machinery in the test. Layering runs strictly one way —
 * adapters &rarr; {@code stream.pipeline} &rarr; {@code stream}.
 */
package org.apache.synapse.stream;
