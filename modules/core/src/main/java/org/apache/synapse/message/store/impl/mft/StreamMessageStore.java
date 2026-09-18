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

package org.apache.synapse.message.store.impl.mft;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseException;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.message.MessageConsumer;
import org.apache.synapse.message.MessageProducer;
import org.apache.synapse.message.store.AbstractMessageStore;
import org.apache.synapse.stream.StreamException;
import org.apache.synapse.stream.Transfer;
import org.apache.synapse.stream.TransferStore;
import org.apache.synapse.stream.TransferStores;

import java.util.Collections;
import java.util.List;

/**
 * The queue of transfers an {@code MFTMessageProcessor} runs.
 *
 * <pre>
 *   &lt;messageStore name="mft-queue"
 *                 class="org.apache.synapse.message.store.impl.mft.StreamMessageStore"/&gt;
 * </pre>
 *
 * <h2>It owns no storage</h2>
 * A submitted transfer is a row in {@code MFT_TRANSFER} carrying this store's name, so this artifact is
 * a view over that table rather than a table of its own. Two of these are two independent queues — a
 * bulk one and an interactive one, each drained by its own processor at its own concurrency — and each
 * sees only its own transfers. One row then covers a transfer's whole life —
 * submitted, claimed, running, finished, retried — and a claim and a job-row transition are one commit
 * instead of two writes that have to be kept in step.
 *
 * <p>The artifact still exists because {@code MessageProcessorFactory} refuses a {@code
 * <messageProcessor>} that names no store (ADR-0027). Keeping it also keeps deployment, the management
 * API and pause/resume/terminate working exactly as an operator already expects.
 *
 * <h2>It takes no parameters</h2>
 * Deliberately unlike {@code JDBCMessageStore}, which takes {@code store.jdbc.connection.url} and
 * friends inline. MFT already requires a datasource, gated loudly at deployment (ADR-0010); asking for a
 * second one — or for the same one twice — would be configuration for its own sake. This resolves the
 * queue the product registered, and refuses to deploy if there is none, because a store that accepts a
 * submission it cannot persist has already lied to the caller that got a job id back.
 *
 * <h2>Why neither a producer nor a consumer</h2>
 * Both throw, and the asymmetry with every other message store is the point: <b>a transfer is not a
 * message</b>. A submission carries a connection name and a path — values an operator will read — and no
 * payload at all; a claimed transfer is a descriptor and an id, not something to mediate. Submitting goes
 * through {@code <mftSubmit>}, which can hand the job id back to its caller, and claiming goes through
 * the processor's own task. Accepting an arbitrary {@code MessageContext} here would queue something
 * nothing can execute.
 */
public class StreamMessageStore extends AbstractMessageStore {

    private static final Log log = LogFactory.getLog(StreamMessageStore.class);

    /** Resolved once at deployment; see {@link #init}. */
    private TransferStore transfers;

    @Override
    public void init(SynapseEnvironment se) {
        super.init(se);

        transfers = TransferStores.store();
        if (transfers == null) {
            // At deployment rather than at the first submission, and fatal rather than degraded: the
            // alternative is an API that returns a job id for a transfer that was never stored.
            throw new SynapseException("message store '" + getName() + "' is an MFT transfer queue, but"
                    + " no MFT datasource is configured. Set [mft] data_source in deployment.toml and"
                    + " run the dbscripts for your database, or remove this message store");
        }
        if (!getParameters().isEmpty()) {
            // Not fatal: a stray parameter is a misunderstanding, not a corruption. But silence would
            // let someone believe they had repointed the queue at another database.
            log.warn("message store '" + getName() + "' ignores the parameter(s) " + getParameters().keySet()
                    + ". An MFT transfer queue takes none: it uses the MFT datasource, which is"
                    + " configured once under [mft] in deployment.toml");
        }
        log.info("MFT transfer queue '" + getName() + "' is ready");
    }

    /**
     * @return how many transfers are waiting, or {@code -1} if the queue could not be reached — the
     *         value {@code AbstractMessageStore} already uses for "unknown". Reporting must not throw:
     *         this is read by the management API, and a broken datasource is not a reason to fail a
     *         listing of every store on the node
     */
    @Override
    public int size() {
        try {
            return transfers == null ? -1 : transfers.queued(getName());
        } catch (StreamException e) {
            log.warn("could not count the transfers waiting in message store '" + getName() + "'", e);
            return -1;
        }
    }

    /**
     * @return a producer that turns a stored message into a queued transfer
     *
     * <p>This is how a transfer is submitted: {@code <store messageStore="mft-queue"/>}, with the body
     * saying what to move. MI already has a mechanism for putting something into a message store, and
     * inventing a second one for this store alone would be a parallel artifact for no gain (ADR-0043).
     */
    @Override
    public MessageProducer getProducer() {
        throw new SynapseException("message store '" + getName() + "' is an MFT transfer queue and does"
                + " not accept messages. Submit a transfer with <mftSubmit store=\"" + getName()
                + "\">, which records the values the pipeline needs and hands back a job id; a message"
                + " carries neither");
    }

    @Override
    public MessageConsumer getConsumer() {
        throw new SynapseException("message store '" + getName() + "' is an MFT transfer queue and does"
                + " not hand out messages. Its transfers are claimed by an MFT message processor, which"
                + " reads descriptors rather than message contexts");
    }

    /**
     * Gives up every waiting transfer.
     *
     * <p>Their job rows survive — this is the queue, not the history — so what was already submitted
     * remains visible and reportable, simply no longer scheduled.
     */
    @Override
    public void clear() {
        if (transfers == null) {
            return;
        }
        int cancelled = 0;
        try {
            for (Transfer waiting : transfers.list(getName(), "QUEUED", Integer.MAX_VALUE)) {
                if (transfers.cancel(waiting.jobId())) {
                    cancelled++;
                }
            }
        } catch (StreamException e) {
            throw new SynapseException("could not clear the MFT transfer queue '" + getName() + "'", e);
        }
        log.info("MFT transfer queue '" + getName() + "' cleared: " + cancelled + " transfer(s) will not"
                + " run. Their records are kept");
    }

    /**
     * Removing "the first message" is how a queue is drained by a caller that has nowhere else to look.
     * Here it would hand back a synthesised message and silently un-queue a transfer, so it refuses: an
     * MFT message processor claims through the queue, which returns a descriptor and records the claim.
     *
     * @throws SynapseException always
     */
    @Override
    public MessageContext remove() {
        throw new SynapseException("message store '" + getName() + "' is an MFT transfer queue; a"
                + " transfer is claimed by an MFT message processor, which records who claimed it."
                + " Taking one from the front of the queue with no owner would leave a transfer"
                + " nothing is running and nothing will retry");
    }

    /**
     * @param messageID the transfer to give up, by job id
     * @return always {@code null} — there is no message to hand back, and synthesising one would invite
     *         a caller to treat it as the transfer
     */
    @Override
    public MessageContext remove(String messageID) {
        if (transfers == null) {
            return null;
        }
        try {
            transfers.cancel(Long.parseLong(messageID.trim()));
        } catch (NumberFormatException e) {
            throw new SynapseException("'" + messageID + "' is not an MFT job id; they are numbers");
        } catch (StreamException e) {
            throw new SynapseException("could not cancel transfer " + messageID + " in the MFT transfer"
                    + " queue '" + getName() + "'", e);
        }
        return null;
    }

    // ---------------------------------------------------------------- browsing
    //
    // A queued transfer is a descriptor, not a message, and there is no honest MessageContext to build
    // from one. The management API reads size() only; anything that wants the transfers themselves
    // should ask GET /management/mft/jobs, which describes them as what they are.

    @Override
    public MessageContext get(int index) {
        return null;
    }

    @Override
    public MessageContext get(String messageId) {
        return null;
    }

    @Override
    public List<MessageContext> getAll() {
        return Collections.emptyList();
    }
}
