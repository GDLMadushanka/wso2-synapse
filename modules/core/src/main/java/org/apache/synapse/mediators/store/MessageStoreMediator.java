/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.synapse.mediators.store;

import org.apache.axis2.AxisFault;
import org.apache.synapse.Mediator;
import org.apache.synapse.MessageContext;
import org.apache.synapse.config.xml.SynapsePath;
import org.apache.synapse.continuation.ContinuationStackManager;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.mediators.AbstractMediator;
import org.apache.synapse.message.store.MessageStore;
import org.apache.synapse.message.store.impl.jms.JmsStore;
import org.apache.synapse.transport.nhttp.NhttpConstants;
import org.apache.synapse.util.MessageHelper;
import org.apache.synapse.SynapseException;
import org.apache.synapse.mediators.Value;
import org.apache.synapse.message.store.impl.mft.StreamMessageStore;
import org.apache.synapse.stream.spi.Invoker;
import org.apache.synapse.stream.StreamException;
import org.apache.synapse.stream.transfer.Submission;
import org.apache.synapse.stream.transfer.TransferStore;
import org.apache.synapse.stream.transfer.TransferStores;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * <code>MessageStoreMediator</code> will store the incoming Messages in associated MessageStore
 *
 * <pre>
 *   &lt;store messageStore="jms-queue" [sequence="onStore"]/&gt;
 *
 *   &lt;store messageStore="bulk-queue" type="MFT" idempotencyKey="{json-eval($.requestId)}"&gt;
 *       &lt;property name="connection" value="connSmb"/&gt;
 *       &lt;property name="path" expression="json-eval($.source)"/&gt;
 *   &lt;/store&gt;
 * </pre>
 *
 * <h2>Two things, told apart by an attribute on the element</h2>
 * Without {@code type}, or with {@code type="DEFAULT"}, this stores a message: the behaviour it has
 * always had, reached by code this attribute does not touch. With {@code type="MFT"} it submits a
 * <b>transfer</b> to an MFT queue and leaves the job id on the message.
 *
 * <p>The discriminator is on the element rather than inferred from the store it names, so that
 * {@code <property/>} children and {@code idempotencyKey} can be refused at <i>parse</i> time instead
 * of resolving an artifact the parser cannot see. It also means a reader of the sequence can tell
 * which of the two things this element does without looking the store up.
 *
 * <h2>Why submitting is a separate method and not a condition</h2>
 * A submission must not build the envelope, must not clone the message, must not clear the
 * continuation stack and must not suppress the response — the caller is about to answer with
 * {@link #JOB_ID}. Rather than thread five conditions through the storing path, {@link #mediate}
 * forks once at the top and the two share nothing but the element.
 */
public class MessageStoreMediator extends AbstractMediator{

    /** {@code type} for an ordinary message store. The default when the attribute is absent. */
    public static final String DEFAULT_TYPE = "DEFAULT";

    /** {@code type} for an MFT transfer queue. */
    public static final String MFT_TYPE = "MFT";

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

    /**
     * {@link #MFT_TYPE}, or {@code null} for the storing behaviour. Never read on the storing path, so
     * an existing configuration reaches exactly the code it did before.
     */
    private String type;

    /** The caller's assertion of sameness, evaluated per submission. Only set when type is MFT. */
    private Value idempotencyKey;

    /** The values a submitted transfer carries. Only populated when type is MFT. */
    private final List<Parameter> parameters = new ArrayList<>();

    /**
     * Name of the Mediator
     */
    private String name;

    /**
     *MessageStore Name
     */
    private String messageStoreName;

    /**
     *MessageStore Name Expression
     */
    private SynapsePath messageStoreExp;

    /**
     * Sequence name to be invoked when the message is stored
     */
    private String  onStoreSequence;

    /**
     * Status of the guaranteed delivery
     */
    private boolean isGuaranteedDeliveryEnabled = false;

    /**
     * Failover message store name
     */
    private String failoverMessageStoreName = null;

    private static final String PRODUCER_GUARANTEED_DELIVERY = "store.producer.guaranteed.delivery.enable";
    private static final String FAILOVER_MESSAGE_STORE_NAME = "store.failover.message.store.name";

    /**
     * Lock for store message operation
     */
    private final Object storeMessageLock = new Object();

    public boolean mediate(MessageContext synCtx) {
        boolean produceStatus;
        if (synCtx.getEnvironment().isDebuggerEnabled()) {
            if (super.divertMediationRoute(synCtx)) {
                return true;
            }
        }

        if(synCtx != null) {
            if (MFT_TYPE.equals(type)) {
                return submitTransfer(synCtx);
            }
            MessageStore messageStore;
            if (messageStoreExp != null) {
                messageStore = synCtx.getConfiguration().getMessageStore(messageStoreExp.stringValueOf(synCtx));
            } else {
                messageStore = synCtx.getConfiguration().getMessageStore(messageStoreName);
            }

            if(messageStore != null) {

                if (messageStore.getParameters().get(PRODUCER_GUARANTEED_DELIVERY) != null) {
                    isGuaranteedDeliveryEnabled = Boolean.parseBoolean(messageStore.getParameters().get
                            (PRODUCER_GUARANTEED_DELIVERY).toString());
                }

                if (messageStore.getParameters().get(FAILOVER_MESSAGE_STORE_NAME) != null) {
                    failoverMessageStoreName = (String) messageStore.getParameters().get(FAILOVER_MESSAGE_STORE_NAME);
                }

                if(onStoreSequence != null) {
                    Mediator sequence = synCtx.getSequence(onStoreSequence);
                    if(sequence != null) {
                        sequence.mediate(synCtx);
                    }
                }

                if(log.isDebugEnabled()) {
                    log.debug("Message Store mediator storing the message : \n " + synCtx.getEnvelope());
                }

                // Ensure that the message is fully read
                synCtx.getEnvelope().buildWithAttachments();
                //Clone the message before sending to the producer
                //Fix ESBJAVA-3650
                MessageContext newCtx = null;
                try {
                    newCtx = MessageHelper.cloneMessageContext(synCtx);
                    ContinuationStackManager.clearStack(newCtx);
                } catch (AxisFault af) {
                    handleException("Error when cloning the message context", af, synCtx);
                }

                synchronized (storeMessageLock) {
                   produceStatus = messageStore.getProducer().storeMessage(newCtx);
                }

                if (!produceStatus) {

                    //Fix ESBJAVA-5011, since connection is already null need to nullify producer also
                    if (messageStore instanceof JmsStore) {
                        ((JmsStore) messageStore).setProducer(null);
                    }

                    if (isGuaranteedDeliveryEnabled && failoverMessageStoreName != null && !failoverMessageStoreName
                            .isEmpty()) {

                        MessageStore failoverMessageStore = synCtx.getConfiguration().getMessageStore(failoverMessageStoreName);
                        boolean failoverProduceStatus;
                        synchronized (storeMessageLock) {
                            failoverProduceStatus = failoverMessageStore.getProducer().storeMessage(newCtx);
                        }
                        if (!failoverProduceStatus) {
                            synCtx.setProperty(NhttpConstants.HTTP_SC, 500);
                            synCtx.setProperty(NhttpConstants.ERROR_DETAIL, "Failed to store message.");
                            synCtx.setProperty(NhttpConstants.ERROR_MESSAGE, "Failed to store message [" + synCtx.getMessageID()
                                                                             + "] in store [" + messageStore.getName() + "].");
                            handleException("Failed to store message [" + synCtx.getMessageID()
                                            + "] in failover store [" + failoverMessageStoreName + "].", synCtx);
                        }

                        if (shouldTrace(synCtx)) {
                            trace.error("Message [" + synCtx.getMessageID() + "] store in the failover message store [" + failoverMessageStoreName + "]");
                        }


                    } else {

                        synCtx.setProperty(NhttpConstants.HTTP_SC, 500);
                        synCtx.setProperty(NhttpConstants.ERROR_DETAIL, "Failed to store message.");
                        synCtx.setProperty(NhttpConstants.ERROR_MESSAGE, "Failed to store message [" + synCtx.getMessageID()
                                                                         + "] in store [" + messageStore.getName() + "].");
                        handleException("Failed to store message [" + synCtx.getMessageID()
                                        + "] in store [" + messageStore.getName() + "].", synCtx);

                    }

                }
                // with the nio transport, this causes the listener not to write a 202
                // Accepted response, as this implies that Synapse does not yet know if
                // a 202 or 200 response would be written back.
                Axis2MessageContext msgCtx = (Axis2MessageContext) synCtx;
                if (null != msgCtx.getAxis2MessageContext() && null != msgCtx.getAxis2MessageContext().getOperationContext()) {
                    msgCtx.getAxis2MessageContext().getOperationContext().setProperty(
                            org.apache.axis2.Constants.RESPONSE_WRITTEN, "SKIP");
                }

                return true;
            } else {
                handleException("Message Store does not exist.", synCtx);
            }
        }
        return false;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMessageStoreName() {
        return messageStoreName;
    }

    public void setMessageStoreName(String messageStoreName) {
        this.messageStoreName = messageStoreName;
    }

    public String  getOnStoreSequence() {
        return onStoreSequence;
    }

    public void setOnStoreSequence(String  onStoreSequence) {
        this.onStoreSequence = onStoreSequence;
    }

    public void setMessageStoreExp(SynapsePath messageStoreExp) {
        this.messageStoreExp = messageStoreExp;
    }

    public SynapsePath getMessageStoreExp() {
        return messageStoreExp;
    }

    // ------------------------------------------------------------------ MFT submission

    /**
     * Queues a transfer and leaves its id on the message.
     *
     * <p>Nothing here touches the storing path's machinery: no envelope build, no clone, no
     * continuation-stack clear, and no suppressed response — the caller answers with {@link #JOB_ID}.
     *
     * <p>Nothing is validated against the sequence that will run it. The queue does not know which
     * processor drains it, so it cannot know which parameters a source operator needs; a missing one
     * fails when the run builds, naming the stage, and is recorded on the transfer.
     *
     * @param synCtx the submitting message, whose properties the values are evaluated against
     * @return always {@code true}; a failure throws rather than returning
     */
    private boolean submitTransfer(MessageContext synCtx) {

        String storeName = messageStoreExp != null
                ? messageStoreExp.stringValueOf(synCtx)
                : messageStoreName;

        // Resolved before anything is written, because the store's name is what tags the row and a name
        // nothing drains produces a transfer that waits for ever with no error anywhere. The storing
        // path gets this for free by dereferencing the store; a submission has to ask.
        MessageStore store = synCtx.getConfiguration() == null
                ? null
                : synCtx.getConfiguration().getMessageStore(storeName);
        if (store == null) {
            throw new SynapseException("<store messageStore=\"" + storeName + "\" type=\"" + MFT_TYPE
                    + "\"/> names a message store that is not deployed, so the transfer would be queued"
                    + " under a name no message processor drains");
        }
        if (!(store instanceof StreamMessageStore)) {
            throw new SynapseException("<store messageStore=\"" + storeName + "\" type=\"" + MFT_TYPE
                    + "\"/> names a store that is not an MFT transfer queue. Drop the type attribute to"
                    + " store a message in it, or name a store declared with "
                    + StreamMessageStore.class.getName());
        }

        TransferStore transfers = TransferStores.store();
        if (transfers == null) {
            throw new SynapseException("cannot submit a transfer to '" + storeName + "': MFT has no"
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
            submission = transfers.submit(storeName, values, key == null || key.isBlank() ? null : key,
                    submitter == null ? null : submitter.name());
        } catch (StreamException e) {
            // Never swallowed. A submission that was not stored must not look accepted: the caller is
            // about to be handed a job id for a transfer nothing will ever run.
            //
            // Retryability is the only thing that separates "you contradicted yourself" from "we could
            // not reach the database", and it is set by whoever threw: a reused key is not retryable,
            // an unreachable store is. Left on the message so the fault sequence can answer accordingly
            // without parsing the text of an error.
            synCtx.setProperty(ERROR, e.isRetryable() ? UNAVAILABLE : CONFLICT);
            throw new SynapseException(e.isRetryable()
                    ? "could not submit a transfer to '" + storeName + "'"
                    : e.getMessage(), e);
        }

        synCtx.setProperty(JOB_ID, String.valueOf(submission.jobId()));
        synCtx.setProperty(SUBMITTED, String.valueOf(submission.created()));
        if (log.isDebugEnabled()) {
            log.debug((submission.created() ? "submitted transfer " : "joined transfer ")
                    + submission.jobId() + " in '" + storeName + "'");
        }
        return true;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
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
     * One value a submitted transfer carries.
     *
     * @param name  what the operator will read it as, under the {@code mft.} prefix
     * @param value literal or expression, evaluated once at submission — never at run, when the message
     *              that carried it is long gone
     */
    public record Parameter(String name, Value value) {
    }
}
