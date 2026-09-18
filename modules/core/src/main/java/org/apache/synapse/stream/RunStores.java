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
 * Where the {@link RunStore} comes from.
 *
 * <p>The same hole as {@link JobStores} and {@link TransferStores}: runs are database rows and Synapse
 * has no datasource, so the storage belongs to the product and the contract belongs here. The product
 * registers one store at startup, over the same datasource the job records are written through.
 *
 * <h2>Null rather than a NOOP, and for the opposite reason to the write side</h2>
 * {@link JobStores#storeFor} degrades to a NOOP because a transfer that runs unrecorded is still a
 * transfer that ran. A reader cannot degrade the same way: a NOOP would answer "no runs" to a question
 * about a server that has run thousands, and an empty answer that means "I cannot see" is worse than an
 * error, because it looks like news. So this returns {@code null} when nothing is registered, and a
 * caller says so rather than reporting emptiness.
 *
 * <p>Unbound by pipeline, unlike {@link JobStores}: a run id is unique across every pipeline, and a
 * listing's whole purpose is to cross them.
 */
public final class RunStores {

    private static volatile RunStore store;

    private RunStores() {
    }

    /**
     * Installs the store. Called once by the product at startup, after its datasource is available.
     *
     * @param s the store, or {@code null} to withdraw it
     */
    public static void register(RunStore s) {
        store = s;
    }

    /** Whether runs can be read at all. */
    public static boolean isAvailable() {
        return store != null;
    }

    /**
     * @return the store, or {@code null} when the product registered none. Callers must check: an
     *         empty listing and an unreadable one must not look the same
     */
    public static RunStore store() {
        return store;
    }
}
