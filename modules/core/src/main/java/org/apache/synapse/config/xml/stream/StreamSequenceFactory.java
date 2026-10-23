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

import org.apache.axiom.om.OMAttribute;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.SynapseException;
import org.apache.synapse.config.xml.XMLConfigConstants;
import org.apache.synapse.Mediator;
import org.apache.synapse.config.xml.MediatorFactoryFinder;
import org.apache.synapse.mediators.template.InvokeMediator;
import org.apache.synapse.stream.SourceIdentityPolicy;
import org.apache.synapse.stream.StreamException;
import org.apache.synapse.stream.StreamOperator;
import org.apache.synapse.stream.sequence.StageSection;
import org.apache.synapse.stream.sequence.StreamSequence;

import javax.xml.namespace.QName;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Properties;

/**
 * Builds a {@link StreamSequence} from a {@code <streamSequence>} artifact element.
 *
 * <pre>
 *   &lt;streamSequence name="archive-ingest"&gt;
 *       &lt;description&gt;optional&lt;/description&gt;
 *       &lt;source&gt;
 *           &lt;file.streamRead configKey="conn"&gt;…&lt;/file.streamRead&gt;
 *       &lt;/source&gt;
 *       &lt;transforms&gt;
 *           &lt;streaming.forEach name="rows"&gt;…&lt;/streaming.forEach&gt;
 *       &lt;/transforms&gt;
 *       &lt;sink&gt;
 *           &lt;file.streamWrite configKey="conn"&gt;…&lt;/file.streamWrite&gt;
 *       &lt;/sink&gt;
 *   &lt;/streamSequence&gt;
 * </pre>
 *
 * <h2>Sections (ADR-0068)</h2>
 * All three are optional and the outer two hold exactly one operator each. An absent
 * {@code <source>} means the caller supplies the bytes — which is what {@code sourceProvided="true"}
 * used to say, and the attribute is gone because the structure now says it and the two could
 * disagree. An absent {@code <sink>} means the framework drains the chain itself, which it has
 * always done for a sequence ending in a materialising transform.
 *
 * <p>The sections exist for the <b>editor</b>, not for this class: cardinality is not expressible in
 * the type system, and a schema can check it at edit time without loading a class. What reaches here
 * is the deployer's claim, recorded as a {@link StageSection} against each stage and checked against
 * the class in {@code validate()} — at {@code init()} for a connector operation, whose class is not
 * loaded at parse time.
 *
 * <h2>Two rules that are stricter than Synapse's habit</h2>
 * <b>A child that does not resolve to an operator is a hard failure.</b> There is no
 * ignore-unknown-element path, because a silently dropped operator is a silently wrong transfer.
 * <p>
 * <b>An unknown attribute is a hard failure.</b> A typo'd {@code sourceIdentity} that deploys as its
 * default would be a sequence behaving unlike the one that was written.
 */
public class StreamSequenceFactory {

    private static final Log log = LogFactory.getLog(StreamSequenceFactory.class);

    private static final QName ATT_NAME = new QName("name");
    private static final QName ATT_SOURCE_IDENTITY = new QName("sourceIdentity");
    private static final QName ATT_RESUME = new QName("resume");
    /**
     * Attributes the <b>framework</b> reads off a stage, which the operator never sees.
     * <p>
     * These are stripped from the element before it reaches the operator's factory, so that "an
     * operator factory rejects unknown attributes" stays correct without every factory having to
     * know this list.
     */
    private static final Set<String> RESERVED_STAGE_ATTRIBUTES =
            Set.of("materialize", "deterministic");

    private static final QName DESCRIPTION_ELT =
            new QName(XMLConfigConstants.SYNAPSE_NAMESPACE, "description");

    private StreamSequenceFactory() {
    }

    /**
     * @param elem       the {@code <streamSequence>} element
     * @param properties deployment properties
     * @return a validated sequence, ready for {@code init()}
     * @throws SynapseException if the artifact is not usable
     */
    public static StreamSequence createStreamSequence(OMElement elem, Properties properties) {
        if (elem == null) {
            throw new SynapseException("cannot build a stream sequence from a null element");
        }

        rejectUnknownAttributes(elem);

        StreamSequence sequence = new StreamSequence();

        // Checked before 'name', because the two grammars share a local name and "you used the
        // reference grammar here" is a far more useful diagnosis than "this has no name".
        if (elem.getAttribute(new QName("key")) != null) {
            throw new SynapseException("a top-level <streamSequence> has a 'key' attribute. 'key'"
                    + " belongs on a <streamSequence key=\"...\"/> reference inside a sequence; an"
                    + " artifact declares 'name' and its operators instead");
        }

        OMAttribute name = elem.getAttribute(ATT_NAME);
        if (name == null || name.getAttributeValue() == null || name.getAttributeValue().isBlank()) {
            throw new SynapseException("a <streamSequence> must have a name");
        }
        sequence.setName(name.getAttributeValue().trim());

        OMAttribute identity = elem.getAttribute(ATT_SOURCE_IDENTITY);
        if (identity != null) {
            SourceIdentityPolicy policy = SourceIdentityPolicy.parse(identity.getAttributeValue());
            if (policy == null) {
                throw new SynapseException("stream sequence '" + sequence.getName() + "' has"
                        + " 'sourceIdentity=\"" + identity.getAttributeValue() + "\"'; expected"
                        + " \"strict\", \"warn\" or \"off\"");
            }
            sequence.setSourceIdentity(policy);
        }

        OMAttribute resume = elem.getAttribute(ATT_RESUME);
        if (resume != null) {
            boolean value = parseBoolean(sequence.getName(), "resume", resume.getAttributeValue());
            if (value) {
                // Refused rather than accepted-as-a-no-op. resume="true" reads as "make this sequence
                // resumable", which no attribute can do: durability is a property of the run, decided
                // by the source's origin and whether the provider gave it an identity. Accepting it
                // silently would let a deployer believe they had configured resume for an API body.
                // ADR-0018 made exactly this mistake and ADR-0019 removed it -- see ADR-0033.
                throw new SynapseException("stream sequence '" + sequence.getName() + "' has"
                        + " 'resume=\"true\"'. Resume cannot be switched on: it happens when the source"
                        + " can be re-read and the caller gave it an identity, which only the caller"
                        + " knows. The attribute exists to decline resume with resume=\"false\";"
                        + " remove it to allow resume where the run qualifies");
            }
            sequence.setResume(false);
        }

        // Cloned BEFORE buildOperators, which strips the framework-reserved stage attributes off the
        // live element it is handed. Cloning afterwards would store a copy with 'materialize' and
        // 'deterministic' already removed, so a serialize-and-write-back — which
        // StreamSequenceDeployer.restoreSynapseArtifact does — would silently drop a deployer's
        // determinism assertion and with it the refusal that depends on it.
        sequence.setSourceElement(elem.cloneOMElement());

        try {
            buildOperators(sequence, elem, properties);
        } catch (StreamException e) {
            throw new SynapseException("stream sequence '" + sequence.getName() + "': "
                    + e.getMessage(), e);
        }

        // A sequence with a connector operation in it cannot be validated yet: the operator lives in a
        // template the library deployer has not created, so we do not know its role. init() validates
        // instead, which runs after libraries are deployed.
        if (!sequence.hasDeferredStages()) {
            try {
                sequence.validate();
            } catch (StreamException e) {
                throw new SynapseException("stream sequence '" + sequence.getName()
                        + "' is not valid: " + e.getMessage(), e);
            }
            sequence.markValidated();
        } else if (log.isDebugEnabled()) {
            log.debug("Stream sequence '" + sequence.getName() + "' reaches at least one operator"
                    + " through a connector template, so its structure is checked at initialisation.");
        }

        if (log.isDebugEnabled()) {
            log.debug("Built stream sequence '" + sequence.getName() + "' with "
                    + sequence.getOperators().size() + " operator(s)");
        }
        return sequence;
    }

    /**
     * Reads {@code deterministic="true|false"}.
     *
     * <p>Only the two words, and not Java's {@code Boolean.parseBoolean}, which answers {@code false}
     * to everything it does not recognise. A deployer writing {@code deterministic="yes"} would then
     * have asserted the opposite of what they meant, on an attribute whose whole purpose is to relax a
     * safety rule.
     *
     * @return the deployer's answer, or {@code null} when the attribute was absent
     */
    private static Boolean parseDeterministic(String sequenceName, int index, String stage,
                                              String value) throws StreamException {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if ("true".equalsIgnoreCase(trimmed)) {
            return Boolean.TRUE;
        }
        if ("false".equalsIgnoreCase(trimmed)) {
            return Boolean.FALSE;
        }
        throw new StreamException("stream sequence '" + sequenceName + "' operator " + index + " ('"
                + stage + "') has 'deterministic=\"" + value + "\"'; expected \"true\" or"
                + " \"false\"");
    }

     /**
     * Refuses {@code materialize="true"} while the spill it asks for is not implemented.
     *
     * <p>The attribute is reserved and stripped, so it used to be read and thrown away. A deployer who
     * set it to bound restart cost got no segment boundary, no spill and no warning — a transfer that
     * still re-ran from stage 1 on failure, configured to do otherwise. Silence there is worse than a
     * refusal, because the configuration looks honoured.
     *
     * <p>{@code "false"} is accepted: it asks for the behaviour that already happens. A value that is
     * neither is a typo and is refused as one.
     */
    private static void rejectUnhonouredMaterialize(String sequenceName, int index, String value) {
        if (value == null) {
            return;
        }
        if (parseBoolean(sequenceName, "materialize", value)) {
            throw new SynapseException("stream sequence '" + sequenceName + "' operator " + index
                    + " sets materialize=\"true\", but framework-inserted spills are not implemented"
                    + " yet, so nothing would happen. Remove it, or declare materialises() on the"
                    + " operator to get a real segment boundary");
        }
    }

    /**
     * How to name a stage in a parse-time message, before any operator necessarily exists.
     *
     * <p>Deliberately the <b>configuration</b>, not the operator. A connector operation is resolved
     * from its template long after parsing — that is the whole point of the deferred route — so there
     * is no instance to ask, and asking one anyway is how this method used to throw a
     * {@code NullPointerException} that surfaced as "Stream sequence configuration cannot be built"
     * with nothing pointing at the cause.
     *
     * <p>It is also the better label even where an operator does exist: a connector operation's
     * {@code name()} is a class constant shared by every stage using it, while the configured name is
     * what distinguishes them and what the deployer actually wrote.
     */
    private static String stageLabel(OMElement childElem) {
        OMAttribute nameAttr = childElem.getAttribute(ATT_NAME);
        String configured = nameAttr == null ? null : nameAttr.getAttributeValue();
        return (configured == null || configured.trim().isEmpty())
                ? childElem.getLocalName() : configured.trim();
    }

 
    /**
     * Builds and adds every operator, in chain order.
     *
     * <p>Runs either at parse time or, for a sequence whose operators come from a connector, from
     * {@code StreamSequence.init()} through the late binder. Both paths are the same code, which is why
     * a deferred sequence gets identical validation to an eager one.
     */
    private static void buildOperators(StreamSequence sequence, OMElement elem, Properties properties)
            throws StreamException {
        int index = 0;
        StageSection previous = null;
        EnumSet<StageSection> seen = EnumSet.noneOf(StageSection.class);

        for (Iterator<?> it = elem.getChildElements(); it.hasNext(); ) {
            Object child = it.next();
            if (!(child instanceof OMElement childElem)) {
                continue;
            }
            if (DESCRIPTION_ELT.equals(childElem.getQName())) {
                sequence.setDescription(childElem.getText());
                continue;
            }

            StageSection section = StageSection.forElement(childElem.getLocalName());
            if (section == null) {
                // The commonest mistake once the grammar is sectioned, and the one worth the longest
                // message: a stage written where a section belongs. Naming the stage and the section
                // it probably wants beats "unexpected element".
                throw new StreamException("<" + childElem.getLocalName() + "> is directly inside"
                        + " <streamSequence>, which holds only " + StageSection.names() + " (and an"
                        + " optional <description>). An operator goes inside one of those — put"
                        + " <" + childElem.getLocalName() + "> in the section matching its role");
            }
            if (!seen.add(section)) {
                throw new StreamException("it has more than one <" + section.elementName() + ">");
            }
            if (previous != null && section.ordinal() < previous.ordinal()) {
                // Document order is chain order, and the schema says the same with xs:sequence. Being
                // lenient here and strict there would mean an artifact that deploys and then fails to
                // validate in the editor that wrote it.
                throw new StreamException("<" + section.elementName() + "> comes after <"
                        + previous.elementName() + ">; the sections are written in chain order, "
                        + StageSection.names());
            }
            previous = section;

            int before = index;
            for (Iterator<?> stages = childElem.getChildElements(); stages.hasNext(); ) {
                Object stage = stages.next();
                if (!(stage instanceof OMElement stageElem)) {
                    continue;
                }
                if (!section.allowsMany() && index > before) {
                    throw new StreamException("<" + section.elementName() + "> holds more than one"
                            + " operator; it carries exactly one. A transfer has one "
                            + section.role().getSimpleName() + ", and anything between belongs in"
                            + " <transforms>");
                }
                buildStage(sequence, stageElem, index, section, properties);
                index++;
            }
            if (index == before && !section.allowsMany()) {
                throw new StreamException("<" + section.elementName() + "> is empty; it carries one"
                        + " operator. Remove the section if this sequence has no "
                        + section.elementName());
            }
        }

        // An absent <source> is what sourceProvided="true" used to say, and now the only thing that
        // says it -- see ADR-0068. Set here rather than read from an attribute so the structure and
        // the flag cannot disagree.
        sequence.setSourceProvided(!seen.contains(StageSection.SOURCE));

        if (index == 0) {
            throw new StreamException("it has no operators");
        }
    }

    /**
     * Builds one stage and appends it, from an element inside a section.
     *
     * <p>Split out of {@link #buildOperators} when the grammar gained sections: the per-stage work is
     * identical in all three, and the only thing that differs is which section is recorded against it.
     */
    private static void buildStage(StreamSequence sequence, OMElement childElem, int index,
                                   StageSection section, Properties properties)
            throws StreamException {

        // Read and remove the framework's own attributes before the operator factory sees the
        // element.
        rejectUnhonouredMaterialize(sequence.getName(), index,
                takeReservedAttribute(childElem, "materialize"));
        Boolean deterministicAttr = parseDeterministic(sequence.getName(), index,
                stageLabel(childElem), takeReservedAttribute(childElem, "deterministic"));

        // Whether the name was written down, as opposed to derived from position. Durable state
        // is keyed by name, so an operator that keeps any must have been named explicitly.
        boolean nameIsExplicit = childElem.getAttribute(ATT_NAME) != null;

        StreamOperatorFactoryFinder finder = StreamOperatorFactoryFinder.getInstance();
        if (finder.isKnown(childElem.getQName())) {
            StreamOperator operator;
            try {
                operator = finder.getOperator(childElem, properties);
            } catch (StreamException e) {
                throw new StreamException("could not build operator " + index + " from <"
                        + childElem.getLocalName() + ">: " + e.getMessage(), e);
            }
            sequence.addOperator(operator, nameIsExplicit, deterministicAttr, section);
            return;
        }

        // Not a registered operator, so try the ordinary connector route: MediatorFactoryFinder
        // turns <file.streamRead/> into an InvokeMediator aimed at the connector's template, using
        // nothing but the SynapseImport declaration. That is what lets a connector expose a stream
        // operator with no change to how connectors are built — and it holds a template name rather
        // than a class, so it parses before the library deployer has run.
        Mediator mediator;
        try {
            mediator = MediatorFactoryFinder.getInstance().getMediator(childElem, properties);
        } catch (Exception e) {
            throw new StreamException("could not resolve <" + childElem.getLocalName()
                    + "> at position " + index + ": " + e.getMessage(), e);
        }
        if (mediator instanceof InvokeMediator invoke) {
            OMAttribute nameAttr = childElem.getAttribute(ATT_NAME);
            sequence.addOperation(invoke, childElem.getLocalName(),
                    nameAttr == null ? null : nameAttr.getAttributeValue(), nameIsExplicit,
                    deterministicAttr, section);
        } else if (mediator instanceof StreamOperator operator) {
            sequence.addOperator(operator, nameIsExplicit, deterministicAttr, section);
        } else {
            throw new StreamException("<" + childElem.getLocalName() + "> at position " + index
                    + " is not a stream operator. A sequence stage must be a registered stream"
                    + " operator or a connector operation whose implementation implements"
                    + " StreamSource, StreamTransform or StreamSink");
        }
    }

    /** Reads a framework attribute off a stage and removes it, so the operator never sees it. */
    private static String takeReservedAttribute(OMElement stage, String localName) {
        OMAttribute a = stage.getAttribute(new QName(localName));
        if (a == null) {
            return null;
        }
        String value = a.getAttributeValue();
        stage.removeAttribute(a);
        return value;
    }


    private static void rejectUnknownAttributes(OMElement elem) {
        for (Iterator<?> it = elem.getAllAttributes(); it.hasNext(); ) {
            OMAttribute a = (OMAttribute) it.next();
            String local = a.getQName().getLocalPart();
            if ("sourceProvided".equals(local)) {
                // Named rather than lumped in with the unknowns, because it was the grammar until
                // ADR-0068 and an artifact carrying it is not a typo -- it is one written against
                // the old shape, and the fix is structural rather than a rename.
                throw new SynapseException("<streamSequence> has 'sourceProvided'. That attribute is"
                        + " gone: a sequence whose caller supplies the bytes simply has no <source>"
                        + " section. Remove the attribute, and remove <source> if it was true");
            }
            if (!"name".equals(local) && !"sourceIdentity".equals(local)
                    && !"resume".equals(local) && !"key".equals(local)) {
                throw new SynapseException("<streamSequence> has an unknown attribute '" + local
                        + "'. Known attributes are 'name', 'sourceIdentity' and 'resume'");
            }
        }
    }

    private static boolean parseBoolean(String sequenceName, String attribute, String value) {
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new SynapseException("stream sequence '" + sequenceName + "' has '" + attribute
                + "=\"" + value + "\"'; expected \"true\" or \"false\"");
    }
}
