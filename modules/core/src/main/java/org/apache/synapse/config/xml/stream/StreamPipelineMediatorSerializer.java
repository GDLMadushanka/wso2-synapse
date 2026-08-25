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
import org.apache.synapse.Mediator;
import org.apache.synapse.SynapseException;
import org.apache.synapse.config.xml.AbstractMediatorSerializer;
import org.apache.synapse.mediators.stream.StreamPipelineMediator;

/**
 * Serializes {@code <streamPipeline key="…"/>}. Emits only the key — a reference carries no
 * definition.
 */
public class StreamPipelineMediatorSerializer extends AbstractMediatorSerializer {

    @Override
    protected OMElement serializeSpecificMediator(Mediator m) {
        if (!(m instanceof StreamPipelineMediator mediator)) {
            handleException("Unsupported mediator passed in for serialization: " + m.getType());
            return null;
        }
        if (mediator.getKey() == null) {
            throw new SynapseException("cannot serialize a <streamPipeline/> reference with no key");
        }

        OMElement elem = fac.createOMElement("streamPipeline", synNS);
        elem.addAttribute(fac.createOMAttribute("key", nullNS, mediator.getKey()));
        saveTracingState(elem, mediator);
        return elem;
    }

    @Override
    public String getMediatorClassName() {
        return StreamPipelineMediator.class.getName();
    }
}
