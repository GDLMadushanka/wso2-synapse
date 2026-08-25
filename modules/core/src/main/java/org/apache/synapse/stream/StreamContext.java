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
 * {@link #workspace()} and {@link #checkpointStore()} are legal only for an operator that
 * declared the matching flag on {@link StreamOperator}. Calling one without the declaration is a
 * programming error and throws.
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
     *
     * @return this stage's workspace directory; never {@code null}
     * @throws IllegalStateException if this operator declared neither
     *                               {@link StreamOperator#materialises()} nor
     *                               {@link StreamOperator#checkpointed()}
     */
    Path workspace();

    /**
     * This operator's checkpoint store.
     *
     * @return this stage's checkpoint store; never {@code null}
     * @throws IllegalStateException if this operator did not declare
     *                               {@link StreamOperator#checkpointed()}
     */
    CheckpointStore checkpointStore();
}
