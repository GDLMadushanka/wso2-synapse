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
import org.apache.synapse.stream.StreamException;
import org.apache.synapse.stream.StreamOperator;

import javax.xml.namespace.QName;
import java.util.Properties;

/**
 * Builds one {@link StreamOperator} from one XML element inside a {@code <streamPipeline>}.
 * <p>
 * This exists because Synapse has no finder for the children of a non-mediator artifact.
 * {@code MediatorFactoryFinder} returns a {@code Mediator}, so reusing it would force every operator
 * to implement {@code Mediator} as well — a contract with {@code mediate(MessageContext)} semantics
 * that an operator does not have and cannot honour.
 *
 * <h2>Registration</h2>
 * Implementations are discovered through {@code META-INF/services}:
 * <pre>
 *   META-INF/services/org.apache.synapse.config.xml.stream.StreamOperatorFactory
 * </pre>
 * That works both for jars on the server classpath and for operators shipped inside an MI connector,
 * because {@link StreamOperatorFactoryFinder} also scans deployed library classloaders.
 *
 * <h2>Two things implementations must get right</h2>
 * <b>Parse configuration into the operator, not out of the message.</b> A factory produces one
 * operator instance per pipeline element, so configuration read here becomes immutable per-instance
 * state. Reading it from the message at run time instead would be both slower and, for an operator
 * shared between pipelines, wrong.
 * <p>
 * <b>Reject unknown attributes rather than ignoring them.</b> A typo that deploys as a default is a
 * transfer that silently does the wrong thing.
 * <p>
 * Also: an operator jar must depend on {@code synapse-core} at {@code provided} scope. A bundled copy
 * gives the operator a different {@code StreamOperator} class than the pipeline loads, and every
 * {@code instanceof} check then fails with a message that points nowhere near the cause.
 */
public interface StreamOperatorFactory {

    /**
     * The element this factory handles, for example
     * {@code {http://ws.apache.org/ns/synapse}file.streamRead}.
     *
     * @return the element name; never {@code null}
     */
    QName getTagQName();

    /**
     * Builds an operator from its configuration element.
     *
     * @param elem       the element, as written in the pipeline
     * @param properties deployment properties, as passed to other Synapse factories
     * @return the operator; never {@code null}
     * @throws StreamException if the configuration is unusable, so that deployment fails rather than
     *                         the first transfer
     */
    StreamOperator createOperator(OMElement elem, Properties properties) throws StreamException;
}
