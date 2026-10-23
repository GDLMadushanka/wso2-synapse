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

package org.apache.synapse.mediators.store;

import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.util.AXIOMUtil;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseException;
import org.apache.synapse.TestMessageContext;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.config.xml.MessageStoreMediatorFactory;
import org.apache.synapse.config.xml.MessageStoreMediatorSerializer;
import org.apache.synapse.mediators.Value;
import org.apache.synapse.message.store.impl.jms.JmsStore;
import org.apache.synapse.message.store.impl.mft.StreamMessageStore;
import org.apache.synapse.stream.StreamException;
import org.apache.synapse.stream.transfer.Submission;
import org.apache.synapse.stream.transfer.Transfer;
import org.apache.synapse.stream.transfer.TransferStatus;
import org.apache.synapse.stream.transfer.TransferStore;
import org.apache.synapse.stream.transfer.TransferStores;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.mockito.Mockito.mock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@code <store type="MFT">}: what a submission puts in the queue, and what it puts back on the message.
 *
 * <p>The second half is why this is a mediator at all rather than the store's producer: a producer is
 * handed a clone and returns a boolean, so an id computed there could never reach the caller.
 *
 * <p>The storing path is deliberately not exercised here. It is reached by a comparison this class
 * never satisfies, and the point of the fork is that it was not touched.
 */
public class MessageStoreMediatorMftTest {

    private static final String QUEUE = "mft-queue";

    private FakeStore store;

    @Before
    public void setUp() {
        store = new FakeStore();
        TransferStores.register(store);
    }

    @After
    public void tearDown() {
        TransferStores.register(null);
    }

    /** A mediator pointed at a deployed MFT queue, the way a parsed one would be. */
    private static MessageStoreMediator submitting() {
        MessageStoreMediator mediator = new MessageStoreMediator();
        mediator.setType(MessageStoreMediator.MFT_TYPE);
        mediator.setMessageStoreName(QUEUE);
        return mediator;
    }

    /** A message whose configuration has the queue deployed in it, as a running node's would. */
    private static MessageContext messageWith(String storeName,
                                              org.apache.synapse.message.store.MessageStore deployed) {
        SynapseConfiguration configuration = new SynapseConfiguration();
        if (deployed != null) {
            deployed.setName(storeName);
            configuration.addMessageStore(storeName, deployed);
        }
        TestMessageContext msg = new TestMessageContext();
        msg.setConfiguration(configuration);
        // mediate() asks the environment whether the debugger is on before it does anything else; a
        // mock answers false, which is the only answer this test cares about.
        msg.setEnvironment(mock(SynapseEnvironment.class));
        return msg;
    }

    private static MessageContext message() {
        return messageWith(QUEUE, new StreamMessageStore());
    }

    @Test
    public void itSubmitsItsValuesAndLeavesTheJobIdOnTheMessage() {
        MessageStoreMediator mediator = submitting();
        mediator.addParameter("connection", new Value("conn1"));
        mediator.addParameter("path", new Value("in/orders.csv"));
        MessageContext msg = message();

        assertTrue(mediator.mediate(msg));

        assertEquals(1, store.submitted.size());
        assertEquals(QUEUE, store.submitted.get(0).storeName());
        assertEquals("conn1", store.submitted.get(0).parameters().get("connection"));
        assertEquals("in/orders.csv", store.submitted.get(0).parameters().get("path"));
        assertEquals("1", msg.getProperty(MessageStoreMediator.JOB_ID));
        assertEquals("true", msg.getProperty(MessageStoreMediator.SUBMITTED));
    }

    /**
     * The id is what the caller polls with, so a submission that joined an existing transfer must still
     * hand one back — with {@code mft.submitted} saying which happened, for a resource that wants to
     * answer differently.
     */
    @Test
    public void joiningAnExistingTransferStillReturnsItsId() {
        store.nextIsExisting = 42L;
        MessageStoreMediator mediator = submitting();
        mediator.addParameter("path", new Value("in/orders.csv"));
        MessageContext msg = message();

        mediator.mediate(msg);

        assertEquals("42", msg.getProperty(MessageStoreMediator.JOB_ID));
        assertEquals("false", msg.getProperty(MessageStoreMediator.SUBMITTED));
    }

    /**
     * The idempotency key reaches the store as the caller wrote it, which is the whole of what it is
     * for: the same key is the same transfer.
     */
    @Test
    public void theIdempotencyKeyReachesTheStore() {
        MessageStoreMediator mediator = submitting();
        mediator.setIdempotencyKey(new Value("abc-123"));
        mediator.addParameter("path", new Value("in/orders.csv"));

        mediator.mediate(message());

        assertEquals("abc-123", store.lastKey);
    }

    /** A blank key is no key: it must not be stored as one, or it collides with the next blank. */
    @Test
    public void aBlankIdempotencyKeyIsTreatedAsAbsent() {
        MessageStoreMediator mediator = submitting();
        mediator.setIdempotencyKey(new Value("   "));

        mediator.mediate(message());

        assertNull(store.lastKey);
    }

    /**
     * The XML contract, which is where a typo is cheap to catch. A misspelt {@code <propery>} that
     * deployed quietly would submit a transfer missing the one value its source needed, and the failure
     * would surface minutes later inside a run, naming an operator rather than the typo.
     */
    @Test
    public void theFactoryRefusesWhatWouldSubmitSomethingIncomplete() throws Exception {
        assertRefused(store("type=\"MFT\"", "<property value=\"in/a.csv\"/>"), "has no 'name'");
        assertRefused(store("type=\"MFT\"", "<property name=\"path\"/>"), "carries nothing");
        assertRefused(store("type=\"MFT\"",
                "<property name=\"path\" value=\"a\" expression=\"$ctx:b\"/>"), "it takes one");
    }

    /**
     * The attribute is the discriminator, so what it gates is refused where it is written rather than
     * against the store it names — which the parser cannot see.
     */
    @Test
    public void whatBelongsToMftIsRefusedWithoutTheType() throws Exception {
        assertRefused(store("", "<property name=\"path\" value=\"in/a.csv\"/>"),
                "no type=\"MFT\"");
        assertRefused(store("idempotencyKey=\"abc\"", ""), "no type=\"MFT\"");
        assertRefused(store("type=\"DEFAULT\"", "<property name=\"path\" value=\"a\"/>"),
                "no type=\"MFT\"");
        assertRefused(store("type=\"JMS\"", ""), "is not a known type");
    }

    /**
     * A child element that is not a {@code <property/>} stays ignored, exactly as it was before this
     * attribute existed. Only what is newly meaningful is newly refused.
     */
    @Test
    public void otherChildrenAreStillIgnored() throws Exception {
        MessageStoreMediator built = parse(store("", "<somethingElse/>"));
        assertNull(built.getType());
        assertTrue(built.getParameters().isEmpty());
    }

    /** And what it accepts, round-tripped through the serializer so the two cannot drift apart. */
    @Test
    public void aSubmissionRoundTripsThroughItsSerializer() throws Exception {
        MessageStoreMediator built = parse(store("type=\"MFT\" idempotencyKey=\"{json-eval($.id)}\"",
                "<property name=\"connection\" value=\"conn1\"/>"
                        + "<property name=\"path\" value=\"in/orders.csv\"/>"));

        assertEquals(MessageStoreMediator.MFT_TYPE, built.getType());
        assertEquals(QUEUE, built.getMessageStoreName());
        assertEquals(2, built.getParameters().size());

        OMElement written = new MessageStoreMediatorSerializer().serializeMediator(null, built);
        MessageStoreMediator again = (MessageStoreMediator) new MessageStoreMediatorFactory()
                .createMediator(written, new Properties());

        assertEquals(built.getType(), again.getType());
        assertEquals(built.getMessageStoreName(), again.getMessageStoreName());
        assertEquals(built.getParameters().size(), again.getParameters().size());
        assertEquals("conn1", again.getParameters().get(0).value().getKeyValue());
        assertNotNull("the key must round-trip as an expression, not become a literal",
                again.getIdempotencyKey().getExpression());
    }

    /** An ordinary store element must come back with no type attribute, not with DEFAULT written in. */
    @Test
    public void anOrdinaryStoreRoundTripsUnchanged() throws Exception {
        MessageStoreMediator built = parse(store("", ""));
        assertNull(built.getType());

        OMElement written = new MessageStoreMediatorSerializer().serializeMediator(null, built);
        assertNull(written.getAttributeValue(new javax.xml.namespace.QName("type")));
    }

    /**
     * The two expression forms the submit API actually uses: a braced attribute and a bare
     * {@code expression=}. They take different factories — braces are how Synapse tells an expression
     * from a literal, so a bare path in a braced-attribute reader parses as text, and a braced one in a
     * path reader does not parse at all.
     */
    @Test
    public void bothExpressionFormsParse() throws Exception {
        MessageStoreMediator built = parse(store(
                "type=\"MFT\" idempotencyKey=\"{json-eval($.requestId)}\"",
                "<property name=\"connection\" value=\"conn1\"/>"
                        + "<property name=\"source\" expression=\"json-eval($.source)\"/>"));

        assertNotNull("a braced attribute is an expression, not a literal",
                built.getIdempotencyKey().getExpression());
        assertNull(built.getIdempotencyKey().getKeyValue());
        assertEquals("conn1", built.getParameters().get(0).value().getKeyValue());
        assertNotNull("a bare expression= is a path",
                built.getParameters().get(1).value().getExpression());
    }

    /**
     * The queue's name is what tags the row, so a name nothing drains must fail at the submission
     * rather than producing a transfer that waits for ever with no error anywhere.
     */
    @Test
    public void submittingToAStoreThatIsNotDeployedIsRefused() {
        MessageStoreMediator mediator = submitting();
        try {
            mediator.mediate(messageWith(QUEUE, null));
            fail("expected the submission to be refused");
        } catch (SynapseException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("not deployed"));
        }
        assertTrue(store.submitted.isEmpty());
    }

    /** type="MFT" and the store it names can disagree, and the disagreement is the caller's mistake. */
    @Test
    public void submittingToAStoreThatIsNotAnMftQueueIsRefused() {
        MessageStoreMediator mediator = submitting();
        try {
            mediator.mediate(messageWith(QUEUE, new JmsStore()));
            fail("expected the submission to be refused");
        } catch (SynapseException expected) {
            assertTrue(expected.getMessage(),
                    expected.getMessage().contains("not an MFT transfer queue"));
        }
        assertTrue(store.submitted.isEmpty());
    }

    /** A store that cannot be reached must fail the submission, not quietly hand back an id. */
    @Test
    public void aStoreThatCannotBeReachedFailsTheSubmission() {
        store.failing = true;
        MessageStoreMediator mediator = submitting();
        mediator.addParameter("path", new Value("in/orders.csv"));
        MessageContext msg = message();

        try {
            mediator.mediate(msg);
            fail("expected the submission to fail");
        } catch (SynapseException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("could not submit"));
        }
        assertNull(msg.getProperty(MessageStoreMediator.JOB_ID));
    }

    /**
     * The two failures a resource answers differently: a contradiction the caller must fix, and a store
     * that will work again shortly. Left on the message rather than parsed out of an error's text.
     */
    @Test
    public void aRefusedSubmissionSaysWhichKindOfFailureItWas() {
        store.conflicting = true;
        MessageStoreMediator mediator = submitting();
        mediator.addParameter("path", new Value("in/orders.csv"));
        MessageContext conflicted = message();

        try {
            mediator.mediate(conflicted);
            fail("expected the submission to be refused");
        } catch (SynapseException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("different values"));
        }
        assertEquals(MessageStoreMediator.CONFLICT,
                conflicted.getProperty(MessageStoreMediator.ERROR));

        store.conflicting = false;
        store.failing = true;
        MessageContext unavailable = message();
        try {
            mediator.mediate(unavailable);
            fail("expected the submission to fail");
        } catch (SynapseException expected) {
            // the point is the property, not the message
        }
        assertEquals(MessageStoreMediator.UNAVAILABLE,
                unavailable.getProperty(MessageStoreMediator.ERROR));
    }

    /** No MFT datasource means no queue; accepting the submission would lose it. */
    @Test
    public void submittingWithNoTransferStoreRegisteredIsRefused() {
        TransferStores.register(null);
        MessageStoreMediator mediator = submitting();

        try {
            mediator.mediate(message());
            fail("expected the submission to be refused");
        } catch (SynapseException expected) {
            assertTrue(expected.getMessage(),
                    expected.getMessage().contains("no transfer store"));
        }
    }

    // ------------------------------------------------------------------ helpers

    private static String store(String attributes, String children) {
        return "<store xmlns=\"http://ws.apache.org/ns/synapse\" messageStore=\"" + QUEUE + "\" "
                + attributes + ">" + children + "</store>";
    }

    private static MessageStoreMediator parse(String xml) throws Exception {
        return (MessageStoreMediator) new MessageStoreMediatorFactory()
                .createMediator(AXIOMUtil.stringToOM(xml), new Properties());
    }

    private static void assertRefused(String xml, String expectedInMessage) throws Exception {
        try {
            parse(xml);
            fail("expected this to be refused: " + xml);
        } catch (SynapseException expected) {
            assertTrue("message should mention '" + expectedInMessage + "' but was: "
                    + expected.getMessage(), expected.getMessage().contains(expectedInMessage));
        }
    }

    private static final class FakeStore implements TransferStore {
        private final List<Long> stoppedCalls = new ArrayList<>();

        private final List<Transfer> submitted = new ArrayList<>();
        private String lastSubmitter;
        private String lastKey;
        private boolean failing;
        private boolean conflicting;
        private long nextIsExisting;

        @Override
        public Submission submit(String storeName, Map<String, String> parameters,
                                 String idempotencyKey, String submitter) throws StreamException {
            lastSubmitter = submitter;
            lastKey = idempotencyKey;
            if (failing) {
                throw new StreamException("the datasource is unreachable", true);
            }
            if (conflicting) {
                throw new StreamException("idempotency key 'k' already belongs to transfer 1, which"
                        + " was submitted with different values", false);
            }
            if (nextIsExisting > 0L) {
                return new Submission(nextIsExisting, false);
            }
            long jobId = submitted.size() + 1L;
            submitted.add(new Transfer(jobId, storeName, new LinkedHashMap<>(parameters),
                    TransferStatus.QUEUED.name(), 0, 0L, null, submitter, null, null, 0L, 0L, 0L));
            return new Submission(jobId, true);
        }

        @Override
        public Transfer claim(String storeName) {
            return null;
        }

        @Override
        public org.apache.synapse.stream.transfer.TransferStatus requestPause(long jobId) {
            return null;
        }

        @Override
        public org.apache.synapse.stream.transfer.TransferStatus requestStop(long jobId) {
            return null;
        }

        @Override
        public boolean start(long jobId) {
            return false;
        }

        @Override
        public org.apache.synapse.stream.transfer.TransferStatus stopped(long jobId) {
            stoppedCalls.add(jobId);
            return null;
        }

        @Override
        public void linkRun(long jobId, String runId) {
        }

        @Override
        public boolean requeue(long jobId, long notBefore) {
            return false;
        }

        @Override
        public void finish(long jobId, TransferStatus status, String errorCode, String errorMessage) {
        }

        @Override
        public boolean cancel(long jobId) {
            return false;
        }

        @Override
        public Transfer find(long jobId) {
            return null;
        }

        @Override
        public List<Transfer> list(String storeName, String status, int limit) {
            return new ArrayList<>(submitted);
        }

        @Override
        public int queued(String storeName) {
            return submitted.size();
        }
    }
}
