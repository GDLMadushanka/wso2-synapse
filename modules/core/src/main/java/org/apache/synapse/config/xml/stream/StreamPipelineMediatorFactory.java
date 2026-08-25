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
import org.apache.synapse.Mediator;
import org.apache.synapse.SynapseException;
import org.apache.synapse.config.xml.AbstractMediatorFactory;
import org.apache.synapse.config.xml.XMLConfigConstants;
import org.apache.synapse.mediators.stream.StreamPipelineMediator;

import javax.xml.namespace.QName;
import java.util.Iterator;
import java.util.Properties;

/**
 * Resolves {@code <streamPipeline key="…"/>} inside a sequence.
 *
 * <h2>Two grammars, one local name</h2>
 * The artifact grammar declares a pipeline; this reference grammar points at one. They share a local
 * name and are told apart by position, so each rejects the other's attributes rather than coercing
 * them — both mistakes are easy to make and produce very different runtime behaviour if accepted.
 */
public class StreamPipelineMediatorFactory extends AbstractMediatorFactory {

    private static final QName ATT_KEY = new QName("key");

    @Override
    public QName getTagQName() {
        return XMLConfigConstants.STREAM_PIPELINE_ELT;
    }

    @Override
    protected Mediator createSpecificMediator(OMElement elem, Properties properties) {
        StreamPipelineMediator mediator = new StreamPipelineMediator();

        OMAttribute key = elem.getAttribute(ATT_KEY);
        if (key == null || key.getAttributeValue() == null || key.getAttributeValue().isBlank()) {
            throw new SynapseException("a <streamPipeline .../> reference inside a sequence requires a"
                    + " 'key' naming a deployed pipeline");
        }
        mediator.setKey(key.getAttributeValue().trim());

        // Artifact-grammar attributes in reference position are rejected, not ignored.
        for (Iterator<?> it = elem.getAllAttributes(); it.hasNext(); ) {
            OMAttribute a = (OMAttribute) it.next();
            String local = a.getQName().getLocalPart();
            if ("name".equals(local) || "sourceProvided".equals(local)) {
                throw new SynapseException("a <streamPipeline .../> reference has '" + local + "'."
                        + " That belongs on the deployed artifact; a reference carries only 'key'");
            }
        }
        if (elem.getFirstElement() != null) {
            throw new SynapseException("a <streamPipeline key=\"" + mediator.getKey() + "\"/>"
                    + " reference must have no children; operators belong on the deployed artifact");
        }

        processAuditStatus(mediator, elem);
        return mediator;
    }
}
