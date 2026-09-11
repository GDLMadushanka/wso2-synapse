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

package org.apache.synapse.stream.pipeline;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.Mediator;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseException;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.mediators.AbstractListMediator;
import org.apache.synapse.mediators.ext.ClassMediator;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.synapse.mediators.template.InvokeParam;
import org.apache.synapse.mediators.Value;
import org.apache.synapse.mediators.template.InvokeMediator;
import org.apache.synapse.mediators.template.TemplateMediator;
import org.apache.synapse.stream.StreamOperator;

/**
 * One position in a pipeline, and how the operator sitting there is reached.
 *
 * <p>A connector operation such as {@code <file.streamRead/>} does not resolve to its own class. It
 * becomes an {@link InvokeMediator} aimed at a template whose body holds the class, so the operator is
 * two levels down. An operator registered through the operator SPI is the instance itself. This holds
 * either shape and hides the difference from the pipeline.
 *
 * <h2>Why the connector route exists at all</h2>
 * It is the one that needs <b>no change to how connectors are written</b>. A connector ships a template
 * plus a {@code <class>} mediator, exactly as every other operation does; the class implements a stream
 * role in addition to being a mediator, and that is the whole of it. No operator SPI, no service
 * registration, no new packaging.
 *
 * <p>It also solves a timing problem for free. A connector's classes are not on any classloader until
 * the library deployer has run, and that happens <i>after</i> the Synapse configuration is parsed — so a
 * pipeline naming {@code <file.streamRead>} cannot be resolved to a class at parse time however hard we
 * try. An {@code InvokeMediator} holds only a template <i>name</i>, so parsing succeeds and the class is
 * found later.
 *
 * <h2>Why the template is looked up per run and never cached</h2>
 * This follows {@link InvokeMediator#mediate}, which does the same on every invocation, and it matters
 * for the same reason: redeploying a connector replaces the {@link TemplateMediator} in the
 * configuration with a new instance built from the new library. A held reference would survive that and
 * keep running code from an undeployed classloader — with no error, which is the worst way for a pipeline
 * to be wrong. A lookup per operator per run is nothing against a run that moves gigabytes.
 *
 * <h2>What an operator owes in return</h2>
 * A connector operation's parameters live on the <b>message</b>, bound only for the duration of the build
 * call, because one template instance is shared by every pipeline and every concurrent run that uses the
 * operation. An operator must therefore <b>capture what it needs while it is bound</b>: the stream it
 * returns is read later, when nothing is bound and {@code ctx.message()} no longer carries its
 * parameters.
 */
class OperatorEntry {

    private static final Log log = LogFactory.getLog(OperatorEntry.class);

    /** Set when the operator is the configured instance itself, as the operator SPI gives. */
    private final StreamOperator staticOperator;

    /** Set when the operator arrived as a connector operation. */
    private final InvokeMediator invoke;

    /** Element name as configured, for error messages. */
    private final String elementName;

    private OperatorEntry(StreamOperator staticOperator, InvokeMediator invoke, String elementName) {
        this.staticOperator = staticOperator;
        this.invoke = invoke;
        this.elementName = elementName;
    }

    /** An operator built at parse time, by the operator SPI. */
    /**
     * The parameters this stage was configured with, for the ones written as literals.
     *
     * <p>An expression cannot be evaluated without a message, so it is <b>omitted</b> rather than
     * included with a placeholder — a checker must be able to tell "not configured" from "configured,
     * but not knowable yet", and only absence says the second honestly.
     *
     * <p>Empty for an SPI-built operator, which holds its configuration in its own fields and has
     * nothing template-shaped to read.
     */
    Map<String, String> literalParameters() {
        if (invoke == null || invoke.getpName2ParamMap() == null) {
            return Collections.emptyMap();
        }
        Map<String, String> literals = new LinkedHashMap<>();
        for (Map.Entry<String, InvokeParam> param : invoke.getpName2ParamMap().entrySet()) {
            Value value = param.getValue() == null ? null : param.getValue().getInlineValue();
            if (value != null && !value.hasExprTypeKey() && value.getKeyValue() != null) {
                literals.put(param.getKey(), value.getKeyValue());
            }
        }
        return literals;
    }

    static OperatorEntry ofOperator(StreamOperator operator, String elementName) {
        return new OperatorEntry(operator, null, elementName);
    }

    /** A connector operation, reached through its template. */
    static OperatorEntry ofOperation(InvokeMediator invoke, String elementName) {
        return new OperatorEntry(null, invoke, elementName);
    }

    String elementName() {
        return elementName;
    }

    /** Whether this position needs a Synapse configuration to be resolved at all. */
    boolean isDeferred() {
        return staticOperator == null;
    }

    /**
     * Reach the operator this position runs, as of now.
     *
     * @param config       configuration to look the template up in
     * @param pipelineName owning pipeline, for error messages
     * @return the operator and the template it was reached through
     * @throws SynapseException if the template is absent or holds no stream operator
     */
    Resolution resolve(SynapseConfiguration config, String pipelineName) {
        if (staticOperator != null) {
            return new Resolution(staticOperator, null);
        }
        if (config == null) {
            throw new SynapseException("stream pipeline '" + pipelineName + "': operation '"
                    + elementName + "' cannot be resolved without a Synapse configuration");
        }

        String templateName = invoke.getTargetTemplate();
        TemplateMediator template = config.getSequenceTemplate(templateName);
        if (template == null) {
            throw new SynapseException("stream pipeline '" + pipelineName + "': operation '"
                    + elementName + "' targets template '" + templateName + "', which is not"
                    + " deployed. Deploy the connector that provides it.");
        }

        StreamOperator operator = findOperator(template);
        if (operator == null) {
            throw new SynapseException("stream pipeline '" + pipelineName + "': operation '"
                    + elementName + "' resolves to template '" + templateName + "', whose"
                    + " implementation does not implement StreamSource, StreamTransform or"
                    + " StreamSink. Only stream operators may appear in a pipeline.");
        }
        return new Resolution(operator, template);
    }

    /**
     * Walk a template body looking for a class mediator wrapping a stream operator.
     *
     * <p>A connector template's body is a sequence holding a single class mediator, but the search
     * tolerates nesting rather than assuming that shape.
     */
    private static StreamOperator findOperator(AbstractListMediator body) {
        for (Mediator child : body.getList()) {
            if (child instanceof ClassMediator) {
                Mediator delegate = ((ClassMediator) child).getMediator();
                if (delegate instanceof StreamOperator) {
                    return (StreamOperator) delegate;
                }
            } else if (child instanceof StreamOperator) {
                return (StreamOperator) child;
            } else if (child instanceof AbstractListMediator) {
                StreamOperator nested = findOperator((AbstractListMediator) child);
                if (nested != null) {
                    return nested;
                }
            }
        }
        return null;
    }

    /**
     * An operator resolved for one run, together with the template it was reached through.
     *
     * <p>Run-scoped rather than cached on the entry, so concurrent runs never share a reference and a
     * connector redeployed between runs is picked up by the next one.
     */
    final class Resolution {

        private final StreamOperator operator;
        private final TemplateMediator template;

        private Resolution(StreamOperator operator, TemplateMediator template) {
            this.operator = operator;
            this.template = template;
        }

        StreamOperator operator() {
            return operator;
        }

        /** The literal parameters of the entry this came from — see {@link OperatorEntry#literalParameters}. */
        Map<String, String> literalParameters() {
            return OperatorEntry.this.literalParameters();
        }

        /**
         * Bind this operation's configured parameters onto the message.
         *
         * <p>Only meaningful for a connector operation; an operator built by the SPI holds its
         * configuration in its own fields already. The binding is deliberately scoped to the call: a
         * template instance is shared across every pipeline and every concurrent run that uses the
         * operation, so per-invocation configuration cannot live on the instance.
         *
         * @param synCtx the message to bind against, or {@code null} when there is none
         * @return {@code true} if a context was pushed and must be released
         */
        boolean bind(MessageContext synCtx) {
            if (template == null || synCtx == null) {
                return false;
            }
            invoke.bindParameters(synCtx, template);
            return true;
        }

        /**
         * Release a binding made by {@link #bind}.
         *
         * @param synCtx the message bound against
         */
        void release(MessageContext synCtx) {
            if (template == null || synCtx == null) {
                return;
            }
            try {
                template.popFuncContextFrom(synCtx);
            } catch (Exception e) {
                log.warn("Failed to release template parameters for '" + elementName + "'", e);
            }
        }
    }
}
