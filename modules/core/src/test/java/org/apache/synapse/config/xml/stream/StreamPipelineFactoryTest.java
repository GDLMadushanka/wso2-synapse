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
import org.apache.synapse.stream.pipeline.StreamPipeline;
import org.apache.synapse.config.xml.MediatorFactoryFinder;
import org.apache.synapse.libraries.imports.SynapseImport;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link StreamPipelineFactory} and {@link StreamPipelineSerializer}.
 * <p>
 * The round trip is the acceptance criterion for the pair: a serialized artifact must re-read into a
 * pipeline that validates identically, because restore and the management API both depend on it.
 */
public class StreamPipelineFactoryTest {

    private static final String NS = "xmlns=\"http://ws.apache.org/ns/synapse\"";

    private static StreamPipeline build(String xml) throws Exception {
        OMElement elem = AXIOMUtil.stringToOM(xml);
        return StreamPipelineFactory.createStreamPipeline(elem, new Properties());
    }

    // ---------------------------------------------------------------- happy path

    @Test
    public void buildsAPipelineFromItsArtifactElement() throws Exception {
        StreamPipeline p = build("<streamPipeline name=\"ingest\" " + NS + ">"
                + "<description>reads and drains</description>"
                + "<test.classpathSource/>"
                + "<test.classpathSink/>"
                + "</streamPipeline>");

        assertEquals("ingest", p.getName());
        assertEquals("reads and drains", p.getDescription());
        assertEquals(2, p.getOperators().size());
        assertEquals("test.classpathSource", p.getOperators().get(0).name());
        assertEquals("test.classpathSink", p.getOperators().get(1).name());
    }

    @Test
    public void readsSourceProvided() throws Exception {
        StreamPipeline p = build("<streamPipeline name=\"ingest\" sourceProvided=\"true\" " + NS + ">"
                + "<test.classpathSink/></streamPipeline>");
        assertTrue(p.isSourceProvided());
    }

    @Test
    public void defaultsSourceProvidedToFalse() throws Exception {
        // A sink-only pipeline without sourceProvided must be rejected, which proves the default.
        try {
            build("<streamPipeline name=\"ingest\" " + NS + "><test.classpathSink/></streamPipeline>");
            fail("expected a sink-only pipeline without sourceProvided to be rejected");
        } catch (SynapseException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("at least a source and a sink"));
        }
    }

    // ---------------------------------------------------------------- strictness

    @Test
    public void rejectsAnUnknownAttributeRatherThanDeployingADefault() {
        try {
            build("<streamPipeline name=\"ingest\" sourceProvidd=\"true\" " + NS + ">"
                    + "<test.classpathSink/></streamPipeline>");
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
            build("<streamPipeline name=\"ingest\" " + NS + "><notAnOperator/></streamPipeline>");
            fail("expected an element nothing provides to be refused at parse time");
        } catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("notAnOperator"));
        }
    }

    /** A pipeline whose operators all resolve through the SPI is built and validated immediately. */
    @Test
    public void aPipelineOfRegisteredOperatorsIsNotDeferred() throws Exception {
        StreamPipeline p = build("<streamPipeline name=\"ingest\" " + NS + ">"
                + "<test.classpathSource/><test.classpathSink/></streamPipeline>");

        assertFalse("nothing to defer", p.hasDeferredStages());
        assertEquals(2, p.getOperators().size());
    }

    @Test
    public void rejectsAMissingName() {
        try {
            build("<streamPipeline " + NS + "><test.classpathSink/></streamPipeline>");
            fail("expected a nameless artifact to be rejected");
        } catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("must have a name"));
        }
    }

    @Test
    public void rejectsAnArtifactWithNoOperators() {
        try {
            build("<streamPipeline name=\"empty\" " + NS + "/>");
            fail("expected an empty artifact to be rejected");
        } catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("no operators"));
        }
    }

    @Test
    public void rejectsANonBooleanSourceProvided() {
        try {
            build("<streamPipeline name=\"ingest\" sourceProvided=\"yes\" " + NS + ">"
                    + "<test.classpathSink/></streamPipeline>");
            fail("expected a non-boolean to be rejected");
        } catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("expected \"true\" or \"false\""));
        }
    }

    /** The artifact and reference grammars share a local name; each must reject the other's shape. */
    @Test
    public void rejectsAReferenceAttributeInArtifactPosition() {
        try {
            build("<streamPipeline name=\"ingest\" key=\"somewhere\" " + NS + ">"
                    + "<test.classpathSource/><test.classpathSink/></streamPipeline>");
            fail("expected 'key' in artifact position to be rejected");
        } catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("belongs on a <streamPipeline key"));
        }
    }

    // ---------------------------------------------------------------- round trip

    @Test
    public void serializesBackToSomethingThatReReadsIdentically() throws Exception {
        String xml = "<streamPipeline name=\"ingest\" sourceProvided=\"true\" " + NS + ">"
                + "<description>round trip</description>"
                + "<test.classpathSink/>"
                + "</streamPipeline>";

        StreamPipeline original = build(xml);
        OMElement serialized = StreamPipelineSerializer.serializeStreamPipeline(null, original);
        assertNotNull(serialized);

        StreamPipeline reread = StreamPipelineFactory.createStreamPipeline(serialized,
                new Properties());

        assertEquals(original.getName(), reread.getName());
        assertEquals(original.getDescription(), reread.getDescription());
        assertEquals(original.isSourceProvided(), reread.isSourceProvided());
        assertEquals(original.getOperators().size(), reread.getOperators().size());
        assertEquals(original.getOperators().get(0).name(), reread.getOperators().get(0).name());
    }

    @Test
    public void serializationOmitsSourceProvidedWhenItIsFalse() throws Exception {
        StreamPipeline p = build("<streamPipeline name=\"ingest\" " + NS + ">"
                + "<test.classpathSource/><test.classpathSink/></streamPipeline>");

        OMElement out = StreamPipelineSerializer.serializeStreamPipeline(null, p);

        assertFalse("a default should not be written back out as though it were chosen",
                out.toString().contains("sourceProvided"));
    }

    @Test
    public void refusesToSerializeAProgrammaticallyAssembledPipeline() {
        StreamPipeline p = new StreamPipeline();
        p.setName("assembled-in-code");
        try {
            StreamPipelineSerializer.serializeStreamPipeline(null, p);
            fail("expected serialization to refuse a pipeline with no source element");
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
        StreamPipeline p = build("<streamPipeline name=\"ingest\" " + NS + ">"
                + "<test.classpathSource/>"
                + "<test.classpathSink materialize=\"true\"/>"
                + "</streamPipeline>");

        assertEquals(2, p.getOperators().size());
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
            build("<streamPipeline name=\"ingest\" " + NS + ">"
                    + "<test.classpathSource/>"
                    + "<test.classpathSink replaySafe=\"true\"/>"
                    + "</streamPipeline>");
            fail("expected a withdrawn attribute to be rejected, not ignored");
        } catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().toLowerCase().contains("attribute"));
        }
    }

    // ---------------------------------------------------------------- maxReprocessed

    /**
     * The deployer is in control of granularity, in either direction, and against any operator. The
     * bound names its own consequence — at most N units reprocessed — so it cannot be read as a safety
     * switch, which is why it needs no refusal. test.classpathSink claims nothing about replay, and
     * throws on any attribute but 'name', so a successful build also proves the attribute was stripped.
     */
    @Test
    public void aDeployerMayLoosenTheBoundOnAnOperatorThatClaimsNothing() throws Exception {
        StreamPipeline p = build("<streamPipeline name=\"ingest\" " + NS + ">"
                + "<test.classpathSource/>"
                + "<test.classpathSink maxReprocessed=\"500\"/>"
                + "</streamPipeline>");

        assertEquals(500, p.maxReprocessed(1));
    }

    /** Saying nothing gives the cautious default: a failure costs one unit. */
    @Test
    public void theDefaultBoundIsOneUnitWhenTheOperatorClaimsNothing() throws Exception {
        StreamPipeline p = build("<streamPipeline name=\"ingest\" " + NS + ">"
                + "<test.classpathSource/>"
                + "<test.classpathSink/>"
                + "</streamPipeline>");

        assertEquals(1, p.maxReprocessed(1));
    }

    @Test
    public void rejectsANonNumericBound() {
        try {
            build("<streamPipeline name=\"ingest\" " + NS + ">"
                    + "<test.classpathSource/>"
                    + "<test.emailSink name=\"send\" maxReprocessed=\"loose\"/>"
                    + "</streamPipeline>");
            fail("expected a non-numeric bound to be rejected");
        } catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("whole number"));
        }
    }

    /** Zero would claim a failure costs nothing, which no protocol can deliver. */
    @Test
    public void rejectsABoundBelowOne() {
        try {
            build("<streamPipeline name=\"ingest\" " + NS + ">"
                    + "<test.classpathSource/>"
                    + "<test.emailSink name=\"send\" maxReprocessed=\"0\"/>"
                    + "</streamPipeline>");
            fail("expected zero to be rejected");
        } catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("at least 1"));
        }
    }

    /**
     * A connector operation parses even though its class is nowhere — an {@code InvokeMediator} holds a
     * template <i>name</i>, so nothing has to be loaded. That is what lets a pipeline naming
     * {@code <file.streamWrite>} survive being read before the library deployer has run.
     *
     * <p>The structural checks therefore move to {@code init()}, and a connector that is genuinely
     * absent fails <b>there</b> — at startup, where someone is looking — rather than on the first
     * message to reach the pipeline.
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

            StreamPipeline p = build("<streamPipeline name=\"ingest\" " + NS + ">"
                    + "<test.classpathSource/><file.streamWrite/></streamPipeline>");

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
}
