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
 * The MFT transfer queue: what was submitted, as opposed to what ran.
 * <p>
 * A {@code <store messageStore="…" type="MFT"/>} writes a row here and gets an id back; an MFT
 * message processor claims one and runs the sequence it names. The queue is a table, and the
 * {@code StreamMessageStore} artifact is a view over the rows carrying its name — which is why
 * both {@code getProducer()} and {@code getConsumer()} throw. See ADR-0061.
 * <p>
 * <h2>Why this is its own package</h2>
 * It is the one cluster in this feature that touches nothing else: no operator, no checkpoint, no
 * run record references a {@link org.apache.synapse.stream.transfer.Transfer}, and nothing here
 * references the byte chain beyond {@code StreamException}. A submission is a connection name and a
 * path; it knows nothing about streaming and streaming knows nothing about it.
 * <p>
 * <h2>A submission is not a run</h2>
 * They are separate rows with separate lifetimes. A submission may never become a run — cancelled,
 * or still queued — and most runs have no submission, because a synchronous transfer never passed
 * through here. {@code linkRun} is the join, written when the run starts. ADR-0044.
 */
package org.apache.synapse.stream.transfer;
