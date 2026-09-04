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
 * Where a pipeline's {@link JobStore} comes from.
 *
 * <p>The same hole, for the same reason, as {@link CheckpointStores}: job records live in a database
 * and Synapse has no datasource of its own, so the storage belongs to the product and the contract
 * belongs here.
 *
 * <h2>Where it differs from {@link CheckpointStores}</h2>
 * A missing checkpoint store is grounds for <b>refusing a pipeline at deployment</b>, because an
 * operator that declared it keeps a durable position would otherwise report success on a transfer that
 * could never resume. A missing job store is not, so this hands back {@link JobStore#NOOP} rather than
 * throwing and callers need no null check — refusing every pipeline in a Synapse without a datasource
 * would be absurd.
 *
 * <h2>What is lost when no provider is registered</h2>
 * More than observability, and worth stating because the honest list is longer than it looks:
 * <ul>
 *   <li>Runs cannot be looked up, listed, or reported on.</li>
 *   <li><b>The source-identity guard is off.</b> It compares this attempt's source against what the
 *       previous attempt recorded, and with no store there is no previous record — so a retry over a
 *       replaced file resumes into the earlier attempt's artifacts. This holds even under
 *       {@code sourceIdentity="strict"}, whose whole promise is to refuse that run.</li>
 *   <li>Nothing can find an interrupted run afterwards, so crash recovery has nothing to sweep.</li>
 * </ul>
 */
public final class JobStores {

    /**
     * Supplies the store for one pipeline.
     *
     * <p>Per pipeline rather than per run, because a job row is keyed by run id alone and one store
     * serves every run of that pipeline. That is the opposite of the checkpoint provider, which is per
     * stage of per run — a difference that follows from the keys, not from taste.
     */
    public interface Provider {

        /**
         * @param pipelineName the owning pipeline
         * @return a store for that pipeline's runs; never {@code null}
         */
        JobStore storeFor(String pipelineName);
    }

    private static volatile Provider provider;

    private JobStores() {
    }

    /**
     * Installs the provider. Called once by the product at startup, after its datasource is available.
     *
     * @param p the provider, or {@code null} to withdraw it
     */
    public static void register(Provider p) {
        provider = p;
    }

    /** Whether runs are being recorded at all. */
    public static boolean isAvailable() {
        return provider != null;
    }

    /**
     * @param pipelineName the owning pipeline
     * @return that pipeline's store, or {@link JobStore#NOOP} when none is registered
     */
    public static JobStore storeFor(String pipelineName) {
        Provider p = provider;
        if (p == null) {
            return JobStore.NOOP;
        }
        JobStore store = p.storeFor(pipelineName);
        return store == null ? JobStore.NOOP : store;
    }
}
