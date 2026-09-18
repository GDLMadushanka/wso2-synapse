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

package org.apache.synapse.mediators.stream;

import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseException;
import org.apache.synapse.mediators.AbstractMediator;
import org.apache.synapse.mediators.Value;
import org.apache.synapse.stream.Invoker;
import org.apache.synapse.stream.StreamException;
import org.apache.synapse.stream.Submission;
import org.apache.synapse.stream.TransferStore;
import org.apache.synapse.stream.TransferStores;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Submits a transfer to an MFT queue and puts its job id on the message.
 *
 * <pre>
 *   &lt;mftSubmit store="mft-queue" idempotencyKey="{json-eval($.requestId)}"&gt;
 *       &lt;property name="connection" value="conn1"/&gt;
 *       &lt;property name="path" expression="json-eval($.source)"/&gt;
 *   &lt;/mftSubmit&gt;
 *
 *   &lt;!-- mft.jobId is now set; mft.submitted says whether this call created it --&gt;
 * </pre>
 *
 * <h2>Why this exists rather than {@code <store>}</h2>
 * Because a submitter needs the id back. The store mediator hands its producer a <b>clone</b> of the
 * message and gets a boolean in return, and a Synapse message context's properties are private to it —
 * so nothing a producer computes can reach the caller. An asynchronous transfer whose id is never
 * returned cannot be polled, which is the whole point of queuing it.
 *
 * <h2>Fields, not a payload</h2>
 * Every other message store accepts any message, so a store that demanded a particular body would be a
 * message store in name only. The values a transfer needs are therefore named here, in the artifact
 * nearest whoever is submitting, and each is a {@link Value} — so a resource decides what a caller may
 * supply and what is fixed, using the expressions it already uses everywhere else.
 *
 * <p>Nothing is validated against the pipeline that will run it. The store does not know which
 * processor drains it, so it cannot know which parameters a source needs; a missing one fails when the
 * run builds, and the failure is recorded on the transfer.
 */
public class MftSubmitMediator extends AbstractMediator {

    /** Where the answer is left, for the resource that has to respond with it. */
    public static final String JOB_ID = "mft.jobId";

    /** Whether this call created the transfer, or found one already in flight. */
    public static final String SUBMITTED = "mft.submitted";

    /**
     * Why a submission failed, for a resource that answers differently depending.
     *
     * <p>{@code CONFLICT} is the caller's to fix — today that means an idempotency key reused for
     * different values — and maps naturally to {@code 409}. {@code UNAVAILABLE} is ours: the store
     * could not be reached, and the same request will work once it can.
     */
    public static final String ERROR = "mft.submitError";

    public static final String CONFLICT = "CONFLICT";
    public static final String UNAVAILABLE = "UNAVAILABLE";

    private String store;
    private Value idempotencyKey;
    private final List<Parameter> parameters = new ArrayList<>();

    @Override
    public boolean mediate(MessageContext synCtx) {
        if (store == null || store.isBlank()) {
            throw new SynapseException("<mftSubmit/> needs a 'store' naming the message store to submit"
                    + " to");
        }
        TransferStore transfers = TransferStores.store();
        if (transfers == null) {
            throw new SynapseException("cannot submit a transfer to '" + store + "': MFT has no"
                    + " transfer store. Configure an MFT datasource and deploy the message store");
        }

        Map<String, String> values = new LinkedHashMap<>();
        for (Parameter parameter : parameters) {
            String value = parameter.value().evaluateValue(synCtx);
            if (value != null) {
                // A parameter that evaluated to nothing is left out rather than stored empty: the
                // difference between "not supplied" and "supplied as blank" is one an operator may act
                // on, and an expression that matched nothing means the former.
                values.put(parameter.name(), value);
            }
        }

        String key = idempotencyKey == null ? null : idempotencyKey.evaluateValue(synCtx);

        // Who is submitting, read off the message the way every other part of Synapse reports the
        // invoking artifact. Captured here because this is the only moment it exists: the API or proxy
        // mediating right now will have answered its caller long before a processor claims this row,
        // and no thread that runs the transfer can see back to it.
        Invoker submitter = Invoker.of(synCtx);

        Submission submission;
        try {
            submission = transfers.submit(store, values, key == null || key.isBlank() ? null : key,
                    submitter == null ? null : submitter.name());
        } catch (StreamException e) {
            // Never swallowed. A submission that was not stored must not look accepted: the caller is
            // about to be handed a job id for a transfer nothing will ever run.
            //
            // Retryability is the only thing that separates "you contradicted yourself" from "we could
            // not reach the database", and it is set by whoever threw: a reused key is not retryable,
            // an unreachable store is. Left on the message so the fault sequence can answer
            // accordingly without parsing the text of an error.
            synCtx.setProperty(ERROR, e.isRetryable() ? UNAVAILABLE : CONFLICT);
            throw new SynapseException(e.isRetryable()
                    ? "could not submit a transfer to '" + store + "'"
                    : e.getMessage(), e);
        }

        synCtx.setProperty(JOB_ID, String.valueOf(submission.jobId()));
        synCtx.setProperty(SUBMITTED, String.valueOf(submission.created()));
        if (log.isDebugEnabled()) {
            log.debug((submission.created() ? "submitted transfer " : "joined transfer ")
                    + submission.jobId() + " in '" + store + "'");
        }
        return true;
    }

    public String getStore() {
        return store;
    }

    public void setStore(String store) {
        this.store = store;
    }

    public Value getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(Value idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public void addParameter(String name, Value value) {
        parameters.add(new Parameter(name, value));
    }

    /**
     * One value the transfer carries.
     *
     * @param name  what the operator will read it as, under the {@code mft.} prefix
     * @param value literal or expression, evaluated once at submission — never at run, when the message
     *              that carried it is long gone
     */
    public record Parameter(String name, Value value) {
    }
}
