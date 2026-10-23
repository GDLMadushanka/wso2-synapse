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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A submitted transfer: one row of a {@link TransferStore}.
 *
 * <h2>It does not say which sequence runs it</h2>
 * The processor draining the store is configured with that, exactly as a forwarding processor is
 * configured with its endpoint. A row that named one would let two processors on the same store
 * disagree about what they are draining.
 *
 * <h2>It does not describe a connection, only name one</h2>
 * {@link #parameters()} holds what the submitter supplied — typically a connection <i>name</i> and a
 * path. Never a URL with a credential in it: this is persisted, listed over the management API and
 * logged. A name is resolved against what is deployed now; an inlined URL resolves successfully to the
 * wrong host after someone repoints the connection.
 *
 * @param jobId          the readable id handed back at submission, and the name the run takes
 * @param storeName      which store this was submitted to; every queue operation is scoped to it
 * @param parameters     the values the sequence's operators read, unmodifiable
 * @param status         {@link TransferStatus}
 * @param attempt        how many times a processor has started this, zero before the first
 * @param nextAttemptAt  epoch millis before which no processor may claim it, or {@code 0}
 * @param runId          the run that is executing or executed this, or {@code null} before the first
 * @param errorCode      the last failure's code, or {@code null}
 * @param errorMessage   the last failure's message, or {@code null}
 * @param createdAt      epoch millis at submission
 * @param updatedAt      epoch millis of the last change
 * @param endedAt        epoch millis when it reached a terminal status, or {@code 0}
 * <h2>The submitter is captured here because nowhere else can</h2>
 * The artifact that submitted a transfer -- an API, a proxy, an inbound endpoint -- has returned by the
 * time anything runs it, on a thread the run never sees. Recording it at submission is what lets the
 * processor later report the whole path a transfer came through, rather than only the half it can see
 * for itself. See {@link org.apache.synapse.stream.spi.Invoker Invoker}.
 *
 */
public record Transfer(long jobId, String storeName, Map<String, String> parameters, String status,
                       int attempt, long nextAttemptAt, String runId, String submitter,
                       String errorCode, String errorMessage, long createdAt, long updatedAt,
                       long endedAt) {

    public Transfer {
        parameters = parameters == null || parameters.isEmpty()
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(parameters));
    }

    /**
     * @param name the parameter to read
     * @return its value, or {@code null} if the submitter did not supply one
     */
    public String parameter(String name) {
        return parameters.get(name);
    }

    /**
     * The name this transfer's run takes.
     *
     * <p>Prefixed rather than bare, because it becomes a workspace directory and a checkpoint key, and
     * a directory called {@code 1423} says nothing about what it holds. The job id the caller quotes
     * stays the number.
     *
     * @return the run id
     */
    public String runName() {
        // mft-job-, matching the framework's own mft-src- and mft-run-: one recognisable prefix, then a
        // segment naming the rule that produced the id. "job" because this one is named after the
        // submission, which is also what makes it resumable -- every attempt asks for the same id.
        return "mft-job-" + jobId;
    }
}
