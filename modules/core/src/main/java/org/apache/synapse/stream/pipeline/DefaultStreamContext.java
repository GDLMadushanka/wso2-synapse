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

package org.apache.synapse.stream.pipeline;

import org.apache.synapse.MessageContext;
import org.apache.synapse.stream.CheckpointStore;
import org.apache.synapse.stream.JobContext;
import org.apache.synapse.stream.ResourceScope;
import org.apache.synapse.stream.StreamContext;

import java.nio.file.Path;

/**
 * What one operator gets for one invocation.
 * <p>
 * Immutable, and one instance per operator per run — {@code stageName} differs between operators,
 * so they cannot share one. That is a deliberate change from an earlier design in which the
 * pipeline mutated a shared context as the chain was built: a shared mutable context is a race
 * waiting for the first operator that hands its context to another thread.
 *
 * <h2>Capability gating</h2>
 * {@link #workspace()} and {@link #checkpointStore()} are legal only for an operator that declared
 * the matching flag. The pipeline passes that entitlement in at construction, and an unentitled
 * call throws {@link IllegalStateException} naming the flag that is missing.
 */
public class DefaultStreamContext implements StreamContext {

    private final MessageContext message;
    private final JobContext job;
    private final ResourceScope resources;
    private final String stageName;

    private final boolean mayUseWorkspace;
    private final boolean mayCheckpoint;

    /** Null until workspace support lands; see {@link #workspace()}. */
    private final Path workspace;

    /** Null until checkpoint support lands; see {@link #checkpointStore()}. */
    private final CheckpointStore checkpointStore;

    /**
     * @param message         the invoking message context
     * @param job             the job context, already defaulted to {@code NOOP} by the pipeline
     * @param resources       the run's resource scope
     * @param stageName       this operator's name
     * @param mayUseWorkspace whether this operator declared {@code materialises()} or
     *                        {@code checkpointed()}
     * @param mayCheckpoint   whether this operator declared {@code checkpointed()}
     * @param workspace       this stage's workspace directory, or {@code null} if unsupported
     * @param checkpointStore this stage's checkpoint store, or {@code null} if unsupported
     */
    public DefaultStreamContext(MessageContext message, JobContext job, ResourceScope resources,
                                String stageName, boolean mayUseWorkspace, boolean mayCheckpoint,
                                Path workspace, CheckpointStore checkpointStore) {
        this.message = message;
        this.job = job == null ? JobContext.NOOP : job;
        this.resources = resources;
        this.stageName = stageName;
        this.mayUseWorkspace = mayUseWorkspace;
        this.mayCheckpoint = mayCheckpoint;
        this.workspace = workspace;
        this.checkpointStore = checkpointStore;
    }

    @Override
    public MessageContext message() {
        return message;
    }

    @Override
    public JobContext job() {
        return job;
    }

    @Override
    public ResourceScope resources() {
        return resources;
    }

    @Override
    public String stageName() {
        return stageName;
    }

    /** Set by {@link #resumedFromArtifact}; read by the pipeline straight after the wrap call. */
    private boolean resumed;

    @Override
    public void resumedFromArtifact() {
        this.resumed = true;
    }

    /** Whether this stage said it short-circuited to an existing artifact. */
    public boolean hasResumedFromArtifact() {
        return resumed;
    }

    @Override
    public Path workspace() {
        if (!mayUseWorkspace) {
            throw new IllegalStateException("operator '" + stageName + "' asked for a workspace but"
                    + " declared neither materialises() nor checkpointed(); declare one, or do not"
                    + " write durable files");
        }
        if (workspace == null) {
            throw new IllegalStateException("operator '" + stageName + "' needs a workspace but none"
                    + " is configured; set '" + org.apache.synapse.SynapseConstants.STREAM_WORKSPACE_ROOT
                    + "' in synapse.properties. A pipeline that materialises is rejected at deployment"
                    + " for this, so reaching here means the pipeline was assembled programmatically");
        }
        return workspace;
    }

    @Override
    public CheckpointStore checkpointStore() {
        if (!mayCheckpoint) {
            throw new IllegalStateException("operator '" + stageName + "' asked for a checkpoint store"
                    + " but did not declare checkpointed()");
        }
        if (checkpointStore == null) {
            throw new UnsupportedOperationException("operator '" + stageName + "' declared"
                    + " checkpointed(), but checkpoint support is not implemented yet");
        }
        return checkpointStore;
    }
}
