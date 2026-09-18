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

import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.util.AXIOMUtil;
import org.apache.synapse.MessageContext;
import org.apache.synapse.config.xml.stream.MftSubmitMediatorFactory;
import org.apache.synapse.config.xml.stream.MftSubmitMediatorSerializer;
import org.apache.synapse.SynapseException;
import org.apache.synapse.TestMessageContext;
import org.apache.synapse.mediators.Value;
import org.apache.synapse.stream.StreamException;
import org.apache.synapse.stream.Submission;
import org.apache.synapse.stream.Transfer;
import org.apache.synapse.stream.TransferStatus;
import org.apache.synapse.stream.TransferStore;
import org.apache.synapse.stream.TransferStores;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * What a submission puts in the store, and what it puts back on the message.
 *
 * <p>The second half is the reason this mediator exists at all: a store mediator hands its producer a
 * clone and gets a boolean, so an id computed there could never reach the caller.
 */
public class MftSubmitMediatorTest {

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

    @Test
    public void itSubmitsItsValuesAndLeavesTheJobIdOnTheMessage() {
        MftSubmitMediator mediator = new MftSubmitMediator();
        mediator.setStore("mft-queue");
        mediator.addParameter("connection", new Value("conn1"));
        mediator.addParameter("path", new Value("in/orders.csv"));
        MessageContext msg = new TestMessageContext();

        assertTrue(mediator.mediate(msg));

        assertEquals(1, store.submitted.size());
        assertEquals("mft-queue", store.submitted.get(0).storeName());
        assertEquals("conn1", store.submitted.get(0).parameters().get("connection"));
        assertEquals("in/orders.csv", store.submitted.get(0).parameters().get("path"));
        assertEquals("1", msg.getProperty(MftSubmitMediator.JOB_ID));
        assertEquals("true", msg.getProperty(MftSubmitMediator.SUBMITTED));
    }

    /**
     * The id is what the caller polls with, so a submission that joined an existing transfer must still
     * hand one back — with {@code mft.submitted} saying which happened, for a resource that wants to
     * answer differently.
     */
    @Test
    public void joiningAnExistingTransferStillReturnsItsId() {
        store.nextIsExisting = 42L;
        MftSubmitMediator mediator = new MftSubmitMediator();
        mediator.setStore("mft-queue");
        mediator.addParameter("path", new Value("in/orders.csv"));
        MessageContext msg = new TestMessageContext();

        mediator.mediate(msg);

        assertEquals("42", msg.getProperty(MftSubmitMediator.JOB_ID));
        assertEquals("false", msg.getProperty(MftSubmitMediator.SUBMITTED));
    }

    /**
     * The XML contract, which is where a typo is cheap to catch. A misspelt {@code <propery>} that
     * deployed quietly would submit a transfer missing the one value its source needed, and the failure
     * would surface minutes later inside a run, naming an operator rather than the typo.
     */
    @Test
    public void theFactoryRefusesWhatWouldSubmitSomethingIncomplete() throws Exception {
        assertRefused("<mftSubmit xmlns=\"http://ws.apache.org/ns/synapse\"/>", "requires a 'store'");
        assertRefused("<mftSubmit xmlns=\"http://ws.apache.org/ns/synapse\" store=\"q\">"
                + "<propery name=\"path\" value=\"in/a.csv\"/></mftSubmit>", "<property/> children only");
        assertRefused("<mftSubmit xmlns=\"http://ws.apache.org/ns/synapse\" store=\"q\">"
                + "<property value=\"in/a.csv\"/></mftSubmit>", "has no 'name'");
        assertRefused("<mftSubmit xmlns=\"http://ws.apache.org/ns/synapse\" store=\"q\">"
                + "<property name=\"path\"/></mftSubmit>", "carries nothing");
        assertRefused("<mftSubmit xmlns=\"http://ws.apache.org/ns/synapse\" store=\"q\">"
                + "<property name=\"path\" value=\"a\" expression=\"$ctx:b\"/></mftSubmit>", "it takes one");
    }

    /** And what it accepts, round-tripped through the serializer so the two cannot drift apart. */
    @Test
    public void aSubmissionRoundTripsThroughItsSerializer() throws Exception {
        String xml = "<mftSubmit xmlns=\"http://ws.apache.org/ns/synapse\" store=\"mft-queue\">"
                + "<property name=\"connection\" value=\"conn1\"/>"
                + "<property name=\"path\" value=\"in/orders.csv\"/></mftSubmit>";

        MftSubmitMediator built = (MftSubmitMediator) new MftSubmitMediatorFactory()
                .createMediator(AXIOMUtil.stringToOM(xml), new Properties());

        assertEquals("mft-queue", built.getStore());
        assertEquals(2, built.getParameters().size());

        OMElement written = new MftSubmitMediatorSerializer().serializeMediator(null, built);
        MftSubmitMediator again = (MftSubmitMediator) new MftSubmitMediatorFactory()
                .createMediator(written, new Properties());

        assertEquals(built.getStore(), again.getStore());
        assertEquals(built.getParameters().size(), again.getParameters().size());
        assertEquals("conn1", again.getParameters().get(0).value().getKeyValue());
    }

    /**
     * The two expression forms the submit API actually uses, which is how this was found: a braced
     * attribute and a bare {@code expression=}. They take different factories — braces are how Synapse
     * tells an expression from a literal, so a bare path in a braced-attribute reader parses as text,
     * and a braced one in a path reader does not parse at all.
     */
    @Test
    public void bothExpressionFormsParse() throws Exception {
        String xml = "<mftSubmit xmlns=\"http://ws.apache.org/ns/synapse\" store=\"mft-queue\""
                + " idempotencyKey=\"{json-eval($.requestId)}\">"
                + "<property name=\"connection\" value=\"conn1\"/>"
                + "<property name=\"source\" expression=\"json-eval($.source)\"/></mftSubmit>";

        MftSubmitMediator built = (MftSubmitMediator) new MftSubmitMediatorFactory()
                .createMediator(AXIOMUtil.stringToOM(xml), new Properties());

        assertNotNull("a braced attribute is an expression, not a literal",
                built.getIdempotencyKey().getExpression());
        assertNull(built.getIdempotencyKey().getKeyValue());
        assertEquals("conn1", built.getParameters().get(0).value().getKeyValue());
        assertNotNull("a bare expression= is a path",
                built.getParameters().get(1).value().getExpression());
    }

    private static void assertRefused(String xml, String expectedInMessage) throws Exception {
        try {
            new MftSubmitMediatorFactory().createMediator(AXIOMUtil.stringToOM(xml), new Properties());
            fail("expected this to be refused: " + xml);
        } catch (SynapseException expected) {
            assertTrue("message should mention '" + expectedInMessage + "' but was: "
                    + expected.getMessage(), expected.getMessage().contains(expectedInMessage));
        }
    }

    /** A store that cannot be reached must fail the submission, not quietly hand back an id. */
    @Test
    public void aStoreThatCannotBeReachedFailsTheSubmission() {
        store.failing = true;
        MftSubmitMediator mediator = new MftSubmitMediator();
        mediator.setStore("mft-queue");
        mediator.addParameter("path", new Value("in/orders.csv"));
        MessageContext msg = new TestMessageContext();

        try {
            mediator.mediate(msg);
            fail("expected the submission to fail");
        } catch (SynapseException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("could not submit"));
        }
        assertNull(msg.getProperty(MftSubmitMediator.JOB_ID));
    }

    /**
     * The two failures a resource answers differently: a contradiction the caller must fix, and a store
     * that will work again shortly. Left on the message rather than parsed out of an error's text.
     */
    @Test
    public void aRefusedSubmissionSaysWhichKindOfFailureItWas() {
        store.conflicting = true;
        MftSubmitMediator mediator = new MftSubmitMediator();
        mediator.setStore("mft-queue");
        mediator.addParameter("path", new Value("in/orders.csv"));
        MessageContext conflicted = new TestMessageContext();

        try {
            mediator.mediate(conflicted);
            fail("expected the submission to be refused");
        } catch (SynapseException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("different values"));
        }
        assertEquals(MftSubmitMediator.CONFLICT, conflicted.getProperty(MftSubmitMediator.ERROR));

        store.conflicting = false;
        store.failing = true;
        MessageContext unavailable = new TestMessageContext();
        try {
            mediator.mediate(unavailable);
            fail("expected the submission to fail");
        } catch (SynapseException expected) {
            // the point is the property, not the message
        }
        assertEquals(MftSubmitMediator.UNAVAILABLE, unavailable.getProperty(MftSubmitMediator.ERROR));
    }

    /** No MFT datasource means no queue; accepting the submission would lose it. */
    @Test
    public void submittingWithNoStoreRegisteredIsRefused() {
        TransferStores.register(null);
        MftSubmitMediator mediator = new MftSubmitMediator();
        mediator.setStore("mft-queue");

        try {
            mediator.mediate(new TestMessageContext());
            fail("expected the submission to be refused");
        } catch (SynapseException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("no"));
        }
    }

    private static final class FakeStore implements TransferStore {
        private final List<Long> stoppedCalls = new ArrayList<>();

        private final List<Transfer> submitted = new ArrayList<>();
        private String lastSubmitter;
        private boolean failing;
        private boolean conflicting;
        private long nextIsExisting;

        @Override
        public Submission submit(String storeName, Map<String, String> parameters,
                                 String idempotencyKey, String submitter) throws StreamException {
            lastSubmitter = submitter;
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
        public org.apache.synapse.stream.TransferStatus requestPause(long jobId) {
            return null;
        }

        @Override
        public org.apache.synapse.stream.TransferStatus requestStop(long jobId) {
            return null;
        }

        @Override
        public boolean start(long jobId) {
            return false;
        }

        @Override
        public org.apache.synapse.stream.TransferStatus stopped(long jobId) {
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
