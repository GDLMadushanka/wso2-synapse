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

package org.apache.synapse.config.xml.stream;

import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.util.AXIOMUtil;
import org.apache.synapse.SynapseException;
import org.apache.synapse.stream.SourceIdentityPolicy;
import org.apache.synapse.stream.sequence.StreamSequence;
import org.apache.synapse.config.xml.MediatorFactoryFinder;
import org.apache.synapse.libraries.imports.SynapseImport;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link StreamSequenceFactory} and {@link StreamSequenceSerializer}.
 * <p>
 * The round trip is the acceptance criterion for the pair: a serialized artifact must re-read into a
 * sequence that validates identically, because restore and the management API both depend on it.
 */
public class StreamSequenceFactoryTest {

    private static final String NS = "xmlns=\"http://ws.apache.org/ns/synapse\"";

    private static StreamSequence build(String xml) throws Exception {
        OMElement elem = AXIOMUtil.stringToOM(xml);
        return StreamSequenceFactory.createStreamSequence(elem, new Properties());
    }

    // ---------------------------------------------------------------- happy path

    @Test
    public void buildsASequenceFromItsArtifactElement() throws Exception {
        StreamSequence p = build("<streamSequence name=\"ingest\" " + NS + ">"
                + "<description>reads and drains</description>"
                + "<source><test.classpathSource/></source>"
                + "<sink><test.classpathSink/></sink>"
                + "</streamSequence>");

        assertEquals("ingest", p.getName());
        assertEquals("reads and drains", p.getDescription());
        assertEquals(2, p.getOperators().size());
        assertEquals("test.classpathSource", p.getOperators().get(0).name());
        assertEquals("test.classpathSink", p.getOperators().get(1).name());
    }

    /**
     * An absent {@code <source>} is the whole of what {@code sourceProvided="true"} used to say.
     *
     * <p>ADR-0068 removed the attribute rather than keeping both, because the two could disagree —
     * and {@code validate()} had to enforce the agreement by hand. The structure is now the only
     * statement, so there is nothing left to disagree with.
     */
    @Test
    public void anAbsentSourceSectionMeansTheCallerSuppliesTheBytes() throws Exception {
        StreamSequence p = build("<streamSequence name=\"ingest\" " + NS + ">"
                + "<sink><test.classpathSink/></sink></streamSequence>");
        assertTrue(p.isSourceProvided());
    }

    /** And a sequence that writes one opens its own, which is the other half of the same fact. */
    @Test
    public void aSourceSectionMeansTheSequenceOpensItsOwn() throws Exception {
        StreamSequence p = build("<streamSequence name=\"ingest\" " + NS + ">"
                + "<source><test.classpathSource/></source>"
                + "<sink><test.classpathSink/></sink></streamSequence>");
        assertFalse(p.isSourceProvided());
    }

    // ---------------------------------------------------------------- strictness

    @Test
    public void rejectsAnUnknownAttributeRatherThanDeployingADefault() {
        try {
            build("<streamSequence name=\"ingest\" sourceProvidd=\"true\" " + NS + ">"
                    + "<sink><test.classpathSink/></sink></streamSequence>");
            fail("expected a typo'd attribute to be rejected");
        } catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("unknown attribute 'sourceProvidd'"));
        }
    }

    /**
     * An element that is neither a registered operator nor a connector operation is refused at parse
     * time. Nothing declares it, so there is nothing to wait for — this is a typo, and it should fail
     * where someone is looking.
     */
    @Test
    public void rejectsAnElementThatIsNeitherAnOperatorNorAConnectorOperation() {
        try {
            build("<streamSequence name=\"ingest\" " + NS + ">"
                    + "<transforms><notAnOperator/></transforms></streamSequence>");
            fail("expected an element nothing provides to be refused at parse time");
        } catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("notAnOperator"));
        }
    }

    // ---------------------------------------------------------------- sections (ADR-0068)

    /**
     * A stage written where a section belongs gets the longest message in the factory, because it is
     * the commonest mistake once the grammar is sectioned — and the one an editor will have caught
     * already, so reaching here means someone hand-wrote the file.
     */
    @Test
    public void refusesAStageDirectlyInsideTheSequence() {
        try {
            build("<streamSequence name=\"ingest\" " + NS + ">"
                    + "<test.classpathSink/></streamSequence>");
            fail("expected a stage outside a section to be refused");
        } catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("<source>, <transforms>, <sink>"));
            assertTrue("the message must name the stage, not just the rule: " + e.getMessage(),
                    e.getMessage().contains("test.classpathSink"));
        }
    }

    /** Cardinality is the half of this a schema can check, so it is also checked here. */
    @Test
    public void refusesASecondSectionOfTheSameKind() {
        try {
            build("<streamSequence name=\"ingest\" " + NS + ">"
                    + "<source><test.classpathSource/></source>"
                    + "<source><test.classpathSource/></source>"
                    + "<sink><test.classpathSink/></sink></streamSequence>");
            fail("expected two <source> sections to be refused");
        } catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("more than one <source>"));
        }
    }

    /** One operator each, because a transfer has one source and one sink. */
    @Test
    public void refusesTwoOperatorsInAnOuterSection() {
        try {
            build("<streamSequence name=\"ingest\" " + NS + ">"
                    + "<source><test.classpathSource/><test.classpathSource/></source>"
                    + "<sink><test.classpathSink/></sink></streamSequence>");
            fail("expected two operators in <source> to be refused");
        } catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("more than one"));
        }
    }

    /**
     * An empty outer section is refused rather than read as absent. The two mean different things —
     * absent says the caller supplies the bytes — and a deployer who opened the tag meant to put
     * something in it.
     */
    @Test
    public void refusesAnEmptyOuterSection() {
        try {
            build("<streamSequence name=\"ingest\" " + NS + ">"
                    + "<source/><sink><test.classpathSink/></sink></streamSequence>");
            fail("expected an empty <source> to be refused");
        } catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("<source> is empty"));
        }
    }

    /**
     * Document order is chain order, and the XSD will say the same with {@code xs:sequence}. Being
     * lenient here would mean an artifact that deploys and then fails to validate in the editor that
     * wrote it.
     */
    @Test
    public void refusesSectionsOutOfChainOrder() {
        try {
            build("<streamSequence name=\"ingest\" " + NS + ">"
                    + "<sink><test.classpathSink/></sink>"
                    + "<source><test.classpathSource/></source></streamSequence>");
            fail("expected sections out of order to be refused");
        } catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("chain order"));
        }
    }

    /**
     * The claim is checked against the class. For an SPI operator that is at parse time; for a
     * connector operation it is at {@code init()}, which is the first moment the class exists.
     */
    @Test
    public void refusesAnOperatorInTheWrongSection() {
        try {
            build("<streamSequence name=\"ingest\" " + NS + ">"
                    + "<source><test.classpathSource/></source>"
                    + "<transforms><test.classpathSink/></transforms></streamSequence>");
            fail("expected a sink inside <transforms> to be refused");
        } catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("<transforms>"));
            assertTrue("the message must say where it should go: " + e.getMessage(),
                    e.getMessage().contains("Move it to <sink>"));
        }
    }

    /**
     * Both outer sections absent is legal, and already was: an inbound-fed sequence ending in a
     * materialising transform is {@code <transforms>} alone. Here the transform is a connector
     * operation, so the sequence is deferred and the capability check waits for {@code init()}.
     */
    @Test
    public void transformsAloneIsALegalSequence() throws Exception {
        MediatorFactoryFinder finder = MediatorFactoryFinder.getInstance();
        Map<String, SynapseImport> saved = finder.getSynapseImportMap();
        try {
            SynapseImport imp = new SynapseImport();
            imp.setLibName("streaming");
            imp.setLibPackage("org.wso2.carbon.connector");
            imp.setStatus(true);
            Map<String, SynapseImport> imports = new HashMap<>();
            imports.put("{org.wso2.carbon.connector}streaming", imp);
            finder.setSynapseImportMap(imports);

            StreamSequence p = build("<streamSequence name=\"ingest\" " + NS + ">"
                    + "<transforms><streaming.forEach name=\"rows\"/></transforms>"
                    + "</streamSequence>");

            assertTrue(p.isSourceProvided());
            assertTrue(p.hasDeferredStages());
        } finally {
            finder.setSynapseImportMap(saved);
        }
    }

    /** A sequence whose operators all resolve through the SPI is built and validated immediately. */
    @Test
    public void aSequenceOfRegisteredOperatorsIsNotDeferred() throws Exception {
        StreamSequence p = build("<streamSequence name=\"ingest\" " + NS + ">"
                + "<source><test.classpathSource/></source><sink><test.classpathSink/></sink></streamSequence>");

        assertFalse("nothing to defer", p.hasDeferredStages());
        assertEquals(2, p.getOperators().size());
    }

    @Test
    public void rejectsAMissingName() {
        try {
            build("<streamSequence " + NS + "><sink><test.classpathSink/></sink></streamSequence>");
            fail("expected a nameless artifact to be rejected");
        } catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("must have a name"));
        }
    }

    @Test
    public void rejectsAnArtifactWithNoOperators() {
        try {
            build("<streamSequence name=\"empty\" " + NS + "/>");
            fail("expected an empty artifact to be rejected");
        } catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("no operators"));
        }
    }

    /**
     * {@code sourceProvided} is refused by name, not swept up with the unknown attributes.
     *
     * <p>An artifact carrying it is not a typo — it is one written against the grammar before
     * ADR-0068 — so the message says what replaced it rather than listing the known attributes.
     */
    @Test
    public void refusesTheWithdrawnSourceProvidedByName() {
        try {
            build("<streamSequence name=\"ingest\" sourceProvided=\"true\" " + NS + ">"
                    + "<sink><test.classpathSink/></sink></streamSequence>");
            fail("expected the withdrawn attribute to be refused");
        } catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("no <source> section"));
        }
    }

    /** The artifact and reference grammars share a local name; each must reject the other's shape. */
    @Test
    public void rejectsAReferenceAttributeInArtifactPosition() {
        try {
            build("<streamSequence name=\"ingest\" key=\"somewhere\" " + NS + ">"
                    + "<source><test.classpathSource/></source><sink><test.classpathSink/></sink></streamSequence>");
            fail("expected 'key' in artifact position to be rejected");
        } catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("belongs on a <streamSequence key"));
        }
    }

    // ---------------------------------------------------------------- round trip

    @Test
    public void serializesBackToSomethingThatReReadsIdentically() throws Exception {
        String xml = "<streamSequence name=\"ingest\" " + NS + ">"
                + "<description>round trip</description>"
                + "<sink><test.classpathSink/></sink>"
                + "</streamSequence>";

        StreamSequence original = build(xml);
        OMElement serialized = StreamSequenceSerializer.serializeStreamSequence(null, original);
        assertNotNull(serialized);

        StreamSequence reread = StreamSequenceFactory.createStreamSequence(serialized,
                new Properties());

        assertEquals(original.getName(), reread.getName());
        assertEquals(original.getDescription(), reread.getDescription());
        assertEquals(original.isSourceProvided(), reread.isSourceProvided());
        assertEquals(original.getOperators().size(), reread.getOperators().size());
        assertEquals(original.getOperators().get(0).name(), reread.getOperators().get(0).name());
    }

    @Test
    public void serializationNeverWritesTheWithdrawnAttribute() throws Exception {
        StreamSequence p = build("<streamSequence name=\"ingest\" " + NS + ">"
                + "<source><test.classpathSource/></source><sink><test.classpathSink/></sink></streamSequence>");

        OMElement out = StreamSequenceSerializer.serializeStreamSequence(null, p);

        assertFalse("sourceProvided is gone; emitting it would produce an artifact that will not"
                + " parse back", out.toString().contains("sourceProvided"));
    }

    @Test
    public void refusesToSerializeAProgrammaticallyAssembledSequence() {
        StreamSequence p = new StreamSequence();
        p.setName("assembled-in-code");
        try {
            StreamSequenceSerializer.serializeStreamSequence(null, p);
            fail("expected serialization to refuse a sequence with no source element");
        } catch (SynapseException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("no source element"));
        }
    }

    // ---------------------------------------------------------------- reserved stage attributes

    /**
     * test.classpathSink throws on any attribute other than 'name'. So if the framework's own
     * attribute reached its factory, this build would fail with "unexpected attribute" — that it
     * builds at all is the proof it was stripped.
     */
    @Test
    public void stripsFrameworkAttributesBeforeTheOperatorFactorySeesThem() throws Exception {
        StreamSequence p = build("<streamSequence name=\"ingest\" " + NS + ">"
                + "<source><test.classpathSource/></source>"
                + "<sink><test.classpathSink materialize=\"false\"/></sink>"
                + "</streamSequence>");

        assertEquals(2, p.getOperators().size());
    }

    /**
     * The spill materialize="true" asks for is not implemented, so accepting it silently gave the
     * deployer a sequence that still re-runs from stage 1 on failure while looking configured
     * otherwise. It is refused until the spill exists.
     */
    @Test
    public void refusesMaterializeTrueWhileSpillsAreNotImplemented() throws Exception {
        try {
            build("<streamSequence name=\"ingest\" " + NS + ">"
                    + "<source><test.classpathSource/></source>"
                    + "<sink><test.classpathSink materialize=\"true\"/></sink>"
                    + "</streamSequence>");
            fail("expected materialize=\"true\" to be refused");
        } catch (SynapseException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("not implemented yet"));
        }
    }

    /** A reserved attribute is still an attribute: a typo in its value must not deploy. */
    @Test
    public void refusesAGarbageMaterializeValue() throws Exception {
        try {
            build("<streamSequence name=\"ingest\" " + NS + ">"
                    + "<source><test.classpathSource/></source>"
                    + "<sink><test.classpathSink materialize=\"banana\"/></sink>"
                    + "</streamSequence>");
            fail("expected a non-boolean materialize value to be refused");
        } catch (SynapseException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("expected \"true\" or \"false\""));
        }
    }

    /**
     * sourceIdentity was parsed by this factory and rejected by its own unknown-attribute whitelist,
     * so the feature could not be configured at all — and the serializer emitted it, producing an
     * artifact file that would not deploy on the next restart.
     */
    @Test
    public void acceptsSourceIdentity() throws Exception {
        // strict needs a caller-supplied seed to compare against, hence no <source>.
        StreamSequence p = build("<streamSequence name=\"ingest\""
                + " sourceIdentity=\"strict\" " + NS + ">"
                + "<sink><test.classpathSink/></sink>"
                + "</streamSequence>");

        assertEquals(SourceIdentityPolicy.STRICT, p.getSourceIdentity());
    }

    @Test
    public void acceptsSourceIdentityOffOnASelfSourcingSequence() throws Exception {
        StreamSequence p = build("<streamSequence name=\"ingest\" sourceIdentity=\"off\" " + NS + ">"
                + "<source><test.classpathSource/></source><sink><test.classpathSink/></sink></streamSequence>");

        assertEquals(SourceIdentityPolicy.OFF, p.getSourceIdentity());
    }

    /**
     * This used to be refused. A sequence that opened its own source recorded no identity, so strict
     * promised to refuse a changed source and could not — and a promise that cannot be kept is worse
     * than no promise.
     *
     * <p>A source can now identify itself before the run (ADR-0046), so the comparison is possible on
     * both paths and the refusal is gone. A source that declines to identify itself still records
     * nothing, which leaves strict inert rather than wrong: there are simply never two identities to
     * compare.
     */
    @Test
    public void strictIsAllowedOnASequenceThatOpensItsOwnSource() throws Exception {
        StreamSequence p = build("<streamSequence name=\"ingest\" sourceIdentity=\"strict\" " + NS + ">"
                + "<source><test.classpathSource/></source><sink><test.classpathSink/></sink></streamSequence>");

        assertEquals(SourceIdentityPolicy.STRICT, p.getSourceIdentity());
    }

    @Test
    public void refusesAnUnknownSourceIdentity() throws Exception {
        try {
            build("<streamSequence name=\"ingest\" sourceIdentity=\"maybe\" " + NS + ">"
                    + "<source><test.classpathSource/></source><sink><test.classpathSink/></sink></streamSequence>");
            fail("expected an unknown policy to be refused");
        } catch (SynapseException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("strict"));
        }
    }


    /**
     * The reserved attributes are stripped off the live element, so the stored source element has to be
     * cloned BEFORE that happens. Cloning after lost the attribute, and the deployer's
     * restoreSynapseArtifact writes that lossy copy back to disk — silently turning a materialising
     * stage back into a pass-through one on the next restart.
     */
    @Test
    public void keepsReservedStageAttributesInTheSerializedForm() throws Exception {
        StreamSequence p = build("<streamSequence name=\"ingest\" " + NS + ">"
                + "<source><test.classpathSource/></source>"
                + "<sink><test.classpathSink materialize=\"false\"/></sink>"
                + "</streamSequence>");

        String serialized = StreamSequenceSerializer.serializeStreamSequence(null, p).toString();
        assertTrue("materialize must survive a round trip: " + serialized,
                serialized.contains("materialize=\"false\""));
    }

    /**
     * {@code replaySafe} was withdrawn: asking a deployer to answer a duplicate-flavoured question
     * implies the answer governs duplicates, and side effects are at-least-once regardless. It is no
     * longer reserved, so it now reaches the operator's own factory and is refused there as the
     * unknown attribute it is — a loud failure rather than a silently ignored one.
     */
    @Test
    public void replaySafeIsNoLongerRecognisedAndIsRefusedAsUnknown() {
        try {
            build("<streamSequence name=\"ingest\" " + NS + ">"
                    + "<source><test.classpathSource/></source>"
                    + "<sink><test.classpathSink replaySafe=\"true\"/></sink>"
                    + "</streamSequence>");
            fail("expected a withdrawn attribute to be rejected, not ignored");
        } catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().toLowerCase().contains("attribute"));
        }
    }

    // ---------------------------------------------------------------- checkpointFrequency

    /**
     * {@code checkpointFrequency} is <b>not</b> framework-reserved any more (ADR-0067).
     *
     * <p>It used to be: the framework read it off the stage, stripped it, and handed it to the
     * artifact. That made it unconfigurable from the tooling, which builds a connector's form out of
     * the template's parameters and cannot see an attribute. It is now the operator's own parameter,
     * so the framework must pass it through untouched like any other.
     *
     * <p>{@code test.classpathSink} throws on every attribute but {@code name}, which is what makes
     * this a test rather than a comment: if the framework were still stripping it, the build would
     * succeed.
     */
    @Test
    public void checkpointFrequencyReachesTheOperatorsFactory() {
        try {
            build("<streamSequence name=\"ingest\" " + NS + ">"
                    + "<source><test.classpathSource/></source>"
                    + "<sink><test.classpathSink checkpointFrequency=\"500\"/></sink>"
                    + "</streamSequence>");
            fail("the framework must no longer strip checkpointFrequency");
        } catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("checkpointFrequency"));
        }
    }

    /**
     * A connector operation carries its cadence as an ordinary <b>parameter</b>, which is the shape
     * ADR-0067 moved it to.
     *
     * <p>The old attribute form had a bug worth remembering: {@code parseMaxReprocessed} logged the
     * widened bound using {@code operator.name()}, and the connector branch has <b>no operator</b> to
     * ask — that is the whole point of the deferred route. So a sequence with a bound above 1 died
     * with a {@code NullPointerException} surfacing only as <i>"Stream sequence configuration cannot
     * be built - Continue in fail-safe mode"</i>, the sequence silently absent afterwards. A value of
     * 1, or no attribute, went straight past the guard the dereference sat in, and every SPI-route
     * test passed a real operator — which is why nothing caught it.
     *
     * <p>As a parameter it cannot recur: the framework does not read the value at all, so there is no
     * branch to dereference an operator from.
     */
    @Test
    public void aConnectorOperationCarriesItsCadenceAsAParameter() throws Exception {
        MediatorFactoryFinder finder = MediatorFactoryFinder.getInstance();
        Map<String, SynapseImport> saved = finder.getSynapseImportMap();
        try {
            SynapseImport imp = new SynapseImport();
            imp.setLibName("streaming");
            imp.setLibPackage("org.wso2.carbon.connector");
            imp.setStatus(true);
            Map<String, SynapseImport> imports = new HashMap<>();
            imports.put("{org.wso2.carbon.connector}streaming", imp);
            finder.setSynapseImportMap(imports);

            StreamSequence p = build("<streamSequence name=\"ingest\" " + NS + ">"
                    + "<source><test.classpathSource/></source>"
                    + "<transforms><streaming.forEach name=\"rows\">"
                    + "<checkpointFrequency>1000</checkpointFrequency>"
                    + "</streaming.forEach></transforms>"
                    + "</streamSequence>");

            assertTrue(p.hasDeferredStages());
            String serialized = StreamSequenceSerializer.serializeStreamSequence(null, p).toString();
            assertTrue("the parameter must survive a round trip: " + serialized,
                    serialized.contains("1000"));
        } finally {
            finder.setSynapseImportMap(saved);
        }
    }

    /**
     * A connector operation parses even though its class is nowhere — an {@code InvokeMediator} holds a
     * template <i>name</i>, so nothing has to be loaded. That is what lets a sequence naming
     * {@code <file.streamWrite>} survive being read before the library deployer has run.
     *
     * <p>The structural checks therefore move to {@code init()}, and a connector that is genuinely
     * absent fails <b>there</b> — at startup, where someone is looking — rather than on the first
     * message to reach the sequence.
     */
    @Test
    public void aConnectorOperationDefersToInitAndFailsThereWhenTheTemplateIsAbsent() throws Exception {
        MediatorFactoryFinder finder = MediatorFactoryFinder.getInstance();
        Map<String, SynapseImport> saved = finder.getSynapseImportMap();
        try {
            SynapseImport imp = new SynapseImport();
            imp.setLibName("file");
            imp.setLibPackage("org.wso2.carbon.connector");
            imp.setStatus(true);
            Map<String, SynapseImport> imports = new HashMap<>();
            imports.put("{org.wso2.carbon.connector}file", imp);
            finder.setSynapseImportMap(imports);

            StreamSequence p = build("<streamSequence name=\"ingest\" " + NS + ">"
                    + "<source><test.classpathSource/></source>"
                    + "<sink><file.streamWrite/></sink></streamSequence>");

            assertTrue("an unloadable connector operation must still parse", p.hasDeferredStages());
            // Only the SPI-resolved stage is built at parse; the connector one is still just a
            // template name, so the chain is incomplete until init() rebuilds it in order.
            assertEquals("only the SPI stage is built so far", 1, p.getOperators().size());

            try {
                p.init(null);
                fail("expected an absent connector template to fail at init, not at first use");
            } catch (Exception e) {
                // Names the operation, which is the part that matters. The exact reason differs by
                // what is missing: with no environment it is the configuration, and in a running server
                // it is the template — "Deploy the connector that provides it."
                assertTrue(e.getMessage(), e.getMessage().contains("file.streamWrite"));
            }
        } finally {
            finder.setSynapseImportMap(saved);
        }
    }

    // ------------------------------------------------------------------ resume

    /** The deployer may decline resume. Absent, a qualifying run still resumes. */
    @Test
    public void resumeFalseIsAccepted() throws Exception {
        StreamSequence p = build("<streamSequence xmlns=\"http://ws.apache.org/ns/synapse\""
                + " name=\"decline\" resume=\"false\"><source><test.classpathSource/></source><sink><test.classpathSink/></sink></streamSequence>");
        assertFalse(p.isResume());

        StreamSequence dflt = build("<streamSequence xmlns=\"http://ws.apache.org/ns/synapse\""
                + " name=\"default\"><source><test.classpathSource/></source><sink><test.classpathSink/></sink></streamSequence>");
        assertTrue("resume is allowed unless declined", dflt.isResume());
    }

    /**
     * {@code resume="true"} is refused rather than accepted as a no-op.
     *
     * <p>It reads as "make this sequence resumable", which no attribute can do — durability is a
     * property of the run, decided by the source's origin and whether the provider gave it an identity.
     * Accepting it silently would let a deployer believe they had configured resume for an API body.
     * ADR-0018 made this mistake, ADR-0019 removed it, and ADR-0033 must not bring it back.
     */
    @Test
    public void resumeTrueIsRefusedBecauseItCannotBeHonoured() throws Exception {
        try {
            build("<streamSequence xmlns=\"http://ws.apache.org/ns/synapse\" name=\"claim\""
                    + " resume=\"true\"><source><test.classpathSource/></source><sink><test.classpathSink/></sink></streamSequence>");
            fail("expected resume=\"true\" to be refused");
        } catch (SynapseException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("cannot be switched on"));
            assertTrue("the message must name the way to decline",
                    e.getMessage().contains("resume=\"false\""));
        }
    }

    /** And it is a known attribute, so the unknown-attribute rule does not reject it. */
    @Test
    public void resumeIsAKnownAttribute() throws Exception {
        try {
            build("<streamSequence xmlns=\"http://ws.apache.org/ns/synapse\" name=\"typo\""
                    + " resumee=\"false\"><source><test.classpathSource/></source><sink><test.classpathSink/></sink></streamSequence>");
            fail("expected a typo to be refused");
        } catch (SynapseException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("resumee"));
            assertTrue("the list of known attributes must mention resume",
                    e.getMessage().contains("'resume'"));
        }
    }
}
