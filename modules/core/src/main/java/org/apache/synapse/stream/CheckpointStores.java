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
 * Where a stage's {@link CheckpointStore} comes from.
 *
 * <h2>Why this is a hole rather than an implementation</h2>
 * Checkpoints live in a database, and Synapse has no datasource of its own. The product does: MI resolves
 * a named datasource through {@code DataSourceService}, which is published only after the repository is
 * populated, and owns the JDBC and the DDL. So the storage belongs there and the <i>contract</i> belongs
 * here — the same direction as every other adapter in this feature.
 *
 * <p>A static holder is a blunt instrument, and it is used deliberately: a checkpoint store has to be
 * reachable from inside a running pipeline, which is several layers below anything that could be handed
 * a reference at construction. The alternative was threading a provider through
 * {@code SynapseEnvironment} and every context, for one value that never varies per pipeline.
 *
 * <h2>Absent is a legitimate state</h2>
 * With no provider registered, checkpointing is unavailable and a pipeline containing a
 * {@code checkpointed()} operator is <b>rejected at deployment</b> — the same shape as a missing
 * workspace root. That is deliberate: an operator that declared it keeps a durable position, silently
 * running without one, would report success on a transfer that could never resume.
 */
public final class CheckpointStores {

    /**
     * Supplies the store for one stage of one run.
     *
     * <p>Per stage rather than per pipeline because a checkpoint is keyed by
     * {@code (runId, stageName)} — two stages of a pipeline resume independently, and two runs of the
     * same pipeline must never see each other's positions.
     */
    public interface Provider {

        /**
         * @param pipelineName the owning pipeline
         * @param runId        the run, as named by the framework
         * @param stageName    the stage's configured identity
         * @return a store scoped to exactly that stage of that run; never {@code null}
         */
        CheckpointStore storeFor(String pipelineName, String runId, String stageName);
    }

    private static volatile Provider provider;

    private CheckpointStores() {
    }

    /**
     * Installs the provider. Called once by the product at startup, after its datasource is available.
     *
     * @param p the provider, or {@code null} to withdraw it
     */
    public static void register(Provider p) {
        provider = p;
    }

    /** Whether checkpointing is available at all. Read by {@code validate()} at deployment. */
    public static boolean isAvailable() {
        return provider != null;
    }

    /**
     * @return the store for this stage
     * @throws IllegalStateException if no provider is registered — which deployment should already have
     *                               refused, so reaching this means a pipeline was assembled
     *                               programmatically or a provider was withdrawn mid-run
     */
    public static CheckpointStore storeFor(String pipelineName, String runId, String stageName) {
        Provider p = provider;
        if (p == null) {
            throw new IllegalStateException("stage '" + stageName + "' of pipeline '" + pipelineName
                    + "' needs a checkpoint store, but none is registered. Checkpointing requires the"
                    + " MFT datasource; a pipeline that declares checkpointed() is normally refused at"
                    + " deployment for this");
        }
        return p.storeFor(pipelineName, runId, stageName);
    }
}
