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
 * Seams the product fills in, which no operator can reach.
 * <p>
 * Everything here is implemented outside Synapse — by {@code MftStores} in the Micro Integrator,
 * against a JDBC datasource — and reached through a static holder that the product registers at
 * startup. Synapse declares the shape and never provides one: a sequence with nothing registered
 * runs scratch rather than broken, which is what makes the contracts testable with no database
 * anywhere.
 * <p>
 * <h2>Why these are not in the contracts package</h2>
 * An operator author imports {@code org.apache.synapse.stream} and should find only what they can
 * use. A {@link org.apache.synapse.stream.spi.JobStore} is not something an operator calls, writes
 * or sees; it is how the runtime records what an operator did. Mixing the two made the contracts
 * package read as a much larger surface than an operator actually faces.
 * <p>
 * The records here — {@link org.apache.synapse.stream.spi.JobRecord}, {@link
 * org.apache.synapse.stream.spi.JobRun}, {@link org.apache.synapse.stream.spi.Run}, {@link
 * org.apache.synapse.stream.spi.Stage}, {@link org.apache.synapse.stream.spi.Invoker} — are the
 * rows those stores read and write, and travel no further than the store that owns them and the
 * management API that reports them.
 * <p>
 * <h2>Direction</h2>
 * This package depends on {@code org.apache.synapse.stream} and nothing depends on it from there.
 * {@link org.apache.synapse.stream.JobContext} stays in the contracts package precisely because
 * {@code StreamContext} hands one to an operator; only the <i>store</i> behind it lives here.
 */
package org.apache.synapse.stream.spi;
