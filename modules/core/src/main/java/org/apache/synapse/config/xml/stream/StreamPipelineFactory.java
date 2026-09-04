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
import org.apache.synapse.stream.pipeline.StreamPipeline;

import javax.xml.namespace.QName;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Properties;

/**
 * Builds a {@link StreamPipeline} from a {@code <streamPipeline>} artifact element.
 *
 * <pre>
 *   &lt;streamPipeline name="archive-ingest" sourceProvided="false"&gt;
 *       &lt;description&gt;optional&lt;/description&gt;
 *       &lt;file.streamRead configKey="conn" path="/in/data.csv"/&gt;
 *       &lt;file.forEach name="rows" .../&gt;
 *       &lt;file.streamWrite configKey="conn" path="/out/data.json"/&gt;
 *   &lt;/streamPipeline&gt;
 * </pre>
 *
 * <h2>Two rules that are stricter than Synapse's habit</h2>
 * <b>A child that does not resolve to an operator is a hard failure.</b> There is no
 * ignore-unknown-element path, because a silently dropped operator is a silently wrong transfer.
 * <p>
 * <b>An unknown attribute is a hard failure.</b> A typo'd {@code sourceProvided} that deploys as its
 * default would be a pipeline behaving unlike the one that was written.
 */
public class StreamPipelineFactory {

    private static final Log log = LogFactory.getLog(StreamPipelineFactory.class);

    private static final QName ATT_NAME = new QName("name");
    private static final QName ATT_SOURCE_PROVIDED = new QName("sourceProvided");
    private static final QName ATT_SOURCE_IDENTITY = new QName("sourceIdentity");
    /**
     * Attributes the <b>framework</b> reads off a stage, which the operator never sees.
     * <p>
     * These are stripped from the element before it reaches the operator's factory, so that "an
     * operator factory rejects unknown attributes" stays correct without every factory having to
     * know this list.
     */
    private static final Set<String> RESERVED_STAGE_ATTRIBUTES = Set.of("materialize", "maxReprocessed");

    private static final QName DESCRIPTION_ELT =
            new QName(XMLConfigConstants.SYNAPSE_NAMESPACE, "description");

    private StreamPipelineFactory() {
    }

    /**
     * @param elem       the {@code <streamPipeline>} element
     * @param properties deployment properties
     * @return a validated pipeline, ready for {@code init()}
     * @throws SynapseException if the artifact is not usable
     */
    public static StreamPipeline createStreamPipeline(OMElement elem, Properties properties) {
        if (elem == null) {
            throw new SynapseException("cannot build a stream pipeline from a null element");
        }

        rejectUnknownAttributes(elem);

        StreamPipeline pipeline = new StreamPipeline();

        // Checked before 'name', because the two grammars share a local name and "you used the
        // reference grammar here" is a far more useful diagnosis than "this has no name".
        if (elem.getAttribute(new QName("key")) != null) {
            throw new SynapseException("a top-level <streamPipeline> has a 'key' attribute. 'key'"
                    + " belongs on a <streamPipeline key=\"...\"/> reference inside a sequence; an"
                    + " artifact declares 'name' and its operators instead");
        }

        OMAttribute name = elem.getAttribute(ATT_NAME);
        if (name == null || name.getAttributeValue() == null || name.getAttributeValue().isBlank()) {
            throw new SynapseException("a <streamPipeline> must have a name");
        }
        pipeline.setName(name.getAttributeValue().trim());

        OMAttribute provided = elem.getAttribute(ATT_SOURCE_PROVIDED);
        if (provided != null) {
            pipeline.setSourceProvided(parseBoolean(pipeline.getName(), "sourceProvided",
                    provided.getAttributeValue()));
        }

        OMAttribute identity = elem.getAttribute(ATT_SOURCE_IDENTITY);
        if (identity != null) {
            SourceIdentityPolicy policy = SourceIdentityPolicy.parse(identity.getAttributeValue());
            if (policy == null) {
                throw new SynapseException("stream pipeline '" + pipeline.getName() + "' has"
                        + " 'sourceIdentity=\"" + identity.getAttributeValue() + "\"'; expected"
                        + " \"strict\", \"warn\" or \"off\"");
            }
            pipeline.setSourceIdentity(policy);
        }

        // Cloned BEFORE buildOperators, which strips the framework-reserved stage attributes off the
        // live element it is handed. Cloning afterwards stored a copy with 'materialize' and
        // 'maxReprocessed' already removed, so a serialize-and-write-back — which
        // StreamPipelineDeployer.restoreSynapseArtifact does — silently reverted a stage configured
        // maxReprocessed="1000" to the default of 1, changing one fsync per thousand records into one
        // per record with nothing logged.
        pipeline.setSourceElement(elem.cloneOMElement());

        try {
            buildOperators(pipeline, elem, properties);
        } catch (StreamException e) {
            throw new SynapseException("stream pipeline '" + pipeline.getName() + "': "
                    + e.getMessage(), e);
        }

        // A pipeline with a connector operation in it cannot be validated yet: the operator lives in a
        // template the library deployer has not created, so we do not know its role. init() validates
        // instead, which runs after libraries are deployed.
        if (!pipeline.hasDeferredStages()) {
            try {
                pipeline.validate();
            } catch (StreamException e) {
                throw new SynapseException("stream pipeline '" + pipeline.getName()
                        + "' is not valid: " + e.getMessage(), e);
            }
            pipeline.markValidated();
        } else if (log.isDebugEnabled()) {
            log.debug("Stream pipeline '" + pipeline.getName() + "' reaches at least one operator"
                    + " through a connector template, so its structure is checked at initialisation.");
        }

        if (log.isDebugEnabled()) {
            log.debug("Built stream pipeline '" + pipeline.getName() + "' with "
                    + pipeline.getOperators().size() + " operator(s)");
        }
        return pipeline;
    }

    /** Reads a framework attribute off a stage and removes it, so the operator never sees it. */
    /**
     * Reads the deployer's bound on how much work a failure at this stage may cost.
     *
     * <p>There is nothing to validate it against: no operator declares anything about replay, and the
     * default is always the cautious 1. An earlier design refused configuration that contradicted an
     * operator's declaration, which was wrong twice over — it defended an exactly-once property that was
     * never on offer, and a boolean phrased as safety implied the answer governed duplicates. This
     * attribute names its own consequence instead, so it needs no guard.
     *
     * <p>What we do owe the deployer is visibility, so any widening is logged at deployment.
     */
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
    private static void rejectUnhonouredMaterialize(String pipelineName, int index, String value) {
        if (value == null) {
            return;
        }
        if (parseBoolean(pipelineName, "materialize", value)) {
            throw new SynapseException("stream pipeline '" + pipelineName + "' operator " + index
                    + " sets materialize=\"true\", but framework-inserted spills are not implemented"
                    + " yet, so nothing would happen. Remove it, or declare materialises() on the"
                    + " operator to get a real segment boundary");
        }
    }

    private static Integer parseMaxReprocessed(String pipelineName, int index,
                                               StreamOperator operator, String value) {
        if (value == null) {
            return null;
        }
        int parsed;
        try {
            parsed = Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new SynapseException("stream pipeline '" + pipelineName + "' operator " + index
                    + " has 'maxReprocessed=\"" + value + "\"'; expected a whole number of checkpoint"
                    + " units, at least 1");
        }
        if (parsed < 1) {
            throw new SynapseException("stream pipeline '" + pipelineName + "' operator " + index
                    + " has 'maxReprocessed=\"" + value + "\"'; it must be at least 1, since a failure"
                    + " always costs at least the unit in flight");
        }
        if (parsed > 1 && log.isInfoEnabled()) {
            log.info("Stream pipeline '" + pipelineName + "' operator " + index + " ('"
                    + operator.name() + "') is configured with maxReprocessed=" + parsed
                    + ", so up to " + parsed + " units may be reprocessed after a failure and any side"
                    + " effects in them repeated. Configured deliberately; recorded here so it is"
                    + " visible.");
        }
        return parsed;
    }

    /**
     * Builds and adds every operator, in chain order.
     *
     * <p>Runs either at parse time or, for a pipeline whose operators come from a connector, from
     * {@code StreamPipeline.init()} through the late binder. Both paths are the same code, which is why
     * a deferred pipeline gets identical validation to an eager one.
     */
    private static void buildOperators(StreamPipeline pipeline, OMElement elem, Properties properties)
            throws StreamException {
        int index = 0;
        for (Iterator<?> it = elem.getChildElements(); it.hasNext(); ) {
            Object child = it.next();
            if (!(child instanceof OMElement childElem)) {
                continue;
            }
            if (DESCRIPTION_ELT.equals(childElem.getQName())) {
                pipeline.setDescription(childElem.getText());
                continue;
            }

            // Read and remove the framework's own attributes before the operator factory sees the
            // element.
            rejectUnhonouredMaterialize(pipeline.getName(), index,
                    takeReservedAttribute(childElem, "materialize"));
            String maxReprocessedAttr = takeReservedAttribute(childElem, "maxReprocessed");

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
                pipeline.addOperator(operator, nameIsExplicit,
                        parseMaxReprocessed(pipeline.getName(), index, operator, maxReprocessedAttr));
                index++;
                continue;
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
                pipeline.addOperation(invoke, childElem.getLocalName(),
                        nameAttr == null ? null : nameAttr.getAttributeValue(), nameIsExplicit,
                        parseMaxReprocessed(pipeline.getName(), index, null, maxReprocessedAttr));
            } else if (mediator instanceof StreamOperator operator) {
                pipeline.addOperator(operator, nameIsExplicit,
                        parseMaxReprocessed(pipeline.getName(), index, operator, maxReprocessedAttr));
            } else {
                throw new StreamException("<" + childElem.getLocalName() + "> at position " + index
                        + " is not a stream operator. A pipeline stage must be a registered stream"
                        + " operator or a connector operation whose implementation implements"
                        + " StreamSource, StreamTransform or StreamSink");
            }
            index++;
        }

        if (index == 0) {
            throw new StreamException("it has no operators");
        }
    }

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
            if (!"name".equals(local) && !"sourceProvided".equals(local)
                    && !"sourceIdentity".equals(local) && !"key".equals(local)) {
                throw new SynapseException("<streamPipeline> has an unknown attribute '" + local
                        + "'. Known attributes are 'name', 'sourceProvided' and 'sourceIdentity'");
            }
        }
    }

    private static boolean parseBoolean(String pipelineName, String attribute, String value) {
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new SynapseException("stream pipeline '" + pipelineName + "' has '" + attribute
                + "=\"" + value + "\"'; expected \"true\" or \"false\"");
    }
}
