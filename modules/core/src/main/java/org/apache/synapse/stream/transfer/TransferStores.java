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

package org.apache.synapse.stream.transfer;


/**
 * Where the {@link TransferStore} comes from.
 *
 * <p>The same hole as {@link org.apache.synapse.stream.spi.JobStores JobStores} and {@link org.apache.synapse.stream.spi.CheckpointStores CheckpointStores}: transfers are database rows and
 * Synapse has no datasource, so the storage belongs to the product and the contract belongs here. The
 * product registers one store at startup over the same datasource its job records already use.
 *
 * <h2>Absence is fatal here, unlike a job store</h2>
 * {@link org.apache.synapse.stream.spi.JobStores#storeFor JobStores#storeFor} hands back a NOOP when nothing is registered, because refusing every
 * sequence in a Synapse with no datasource would be absurd — a transfer still runs, it simply goes
 * unrecorded. A transfer store cannot degrade that way: a NOOP would accept a submission, hand back an
 * id and lose it, and the caller holding that id has been lied to. So this returns {@code null} when
 * nothing is registered, and the store artifact that needs one fails at deployment rather than at the
 * first submission.
 *
 * <p>Unbound, unlike the others: a job id is unique across every store, so looking one up needs no store
 * name, which is what lets the management API answer {@code GET /mft/jobs/1423} without being told where
 * to look.
 */
public final class TransferStores {

    private static volatile TransferStore store;

    private TransferStores() {
    }

    /**
     * Installs the store. Called once by the product at startup, after its datasource is available.
     *
     * @param s the store, or {@code null} to withdraw it
     */
    public static void register(TransferStore s) {
        store = s;
    }

    /** Whether transfers can be submitted at all. */
    public static boolean isAvailable() {
        return store != null;
    }

    /**
     * @return the store, or {@code null} when the product registered none. Callers must check: there is
     *         deliberately no silent fallback, because losing a submitted transfer is worse than
     *         refusing one
     */
    public static TransferStore store() {
        return store;
    }
}
