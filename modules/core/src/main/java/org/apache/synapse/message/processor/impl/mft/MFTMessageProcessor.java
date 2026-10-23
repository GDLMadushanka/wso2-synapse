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

package org.apache.synapse.message.processor.impl.mft;

import org.apache.synapse.message.processor.impl.ScheduledMessageProcessor;
import org.apache.synapse.task.Task;

/**
 * Runs the transfers waiting in a {@code StreamMessageStore}.
 *
 * <pre>
 *   &lt;messageProcessor name="mft-runner"
 *                     class="org.apache.synapse.message.processor.impl.mft.MFTMessageProcessor"
 *                     messageStore="mft-queue"&gt;
 *       &lt;parameter name="interval"&gt;2000&lt;/parameter&gt;
 *       &lt;parameter name="concurrency"&gt;4&lt;/parameter&gt;
 *       &lt;parameter name="max.delivery.attempts"&gt;4&lt;/parameter&gt;
 *       &lt;parameter name="client.retry.interval"&gt;30000&lt;/parameter&gt;
 *       &lt;parameter name="sequence"&gt;transferDone&lt;/parameter&gt;
 *       &lt;parameter name="fault.sequence"&gt;transferGivenUp&lt;/parameter&gt;
 *       &lt;parameter name="deactivate.after"&gt;10&lt;/parameter&gt;
 *   &lt;/messageProcessor&gt;
 * </pre>
 *
 * <h2>Why this is an ordinary message processor</h2>
 * Because the alternative was inventing a scheduled-task artifact with its own deployment, management
 * API and pause/resume/terminate surface (ADR-0027). Extending {@link ScheduledMessageProcessor} means an
 * operator configures, pauses and resumes an MFT processor exactly as they already do a forwarding one.
 *
 * <h2>What it does differently</h2>
 * It claims <b>descriptors</b>, not messages, and runs the sequence each one names (ADR-0041). Nothing is
 * dispatched into a sequence: the descriptor is the unit of work, and putting a sequence in between would
 * make the sequence it names decorative.
 *
 * <h2>The parameters, and what they cost to get wrong</h2>
 * <ul>
 *   <li>{@code interval} — how often to look for work. Inherited; a transfer runs for far longer than any
 *       sensible interval, so this is a latency knob, not a throughput one.</li>
 *   <li>{@code concurrency} — how many transfers this node runs at once. The pool is bounded by it and so
 *       is the claim loop, so it is also the answer to "how many source connections can this node hold
 *       open".</li>
 *   <li>{@code max.delivery.attempts} — how many times a <i>retryable</i> failure is retried before the
 *       transfer is left failed. Default 4, matching the forwarding processor. A non-retryable failure is
 *       never retried, whatever this says.</li>
 *   <li>{@code client.retry.interval} — how long a failed transfer waits before it can be claimed again.
 *       Recorded on the row, so another node cannot pick it up early either.</li>
 *   <li>{@code max.delivery.drop} — {@code Enabled} (the default) gives up a transfer that exhausted its
 *       attempts and keeps going; {@code Disabled} deactivates the processor instead, which is what the
 *       forwarding processor does.</li>
 *   <li>{@code deactivate.after} — how many transfers may be given up <b>consecutively</b> before this
 *       processor stops itself. Default 10; zero turns it off. One bad file must not stop a queue, but
 *       ten in a row is a destination that is down.</li>
 * </ul>
 *
 * <h2>Three hooks, all optional</h2>
 * {@code sequence} runs after a transfer completes, {@code fault.sequence} when one is given up, and
 * {@code message.processor.deactivate.sequence} when this processor stops itself. They are where a
 * confirmation email, a ledger row or an alert belongs — none of it is expressible in a sequence, which
 * moves bytes and knows nothing about what the transfer meant. A hook never changes a transfer's
 * outcome. ADR-0042.
 */
public class MFTMessageProcessor extends ScheduledMessageProcessor {

    private static final String CONCURRENCY = "concurrency";
    private static final String MAX_DELIVERY_ATTEMPTS = "max.delivery.attempts";
    private static final String RETRY_INTERVAL = "client.retry.interval";

    @Override
    protected Task getTask() {
        return new MFTTransferService(this, synapseEnvironment, CONCURRENCY, MAX_DELIVERY_ATTEMPTS,
                RETRY_INTERVAL);
    }
}
