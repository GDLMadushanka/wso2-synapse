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
import org.apache.synapse.mediators.Value;
import org.apache.synapse.mediators.stream.MftSubmitMediator;

/** Writes an {@code <mftSubmit/>} back out as the XML it was built from. */
public class MftSubmitMediatorSerializer extends AbstractMediatorSerializer {

    @Override
    protected OMElement serializeSpecificMediator(Mediator m) {
        if (!(m instanceof MftSubmitMediator mediator)) {
            handleException("Unsupported mediator passed in for serialization: " + m.getType());
            return null;
        }
        if (mediator.getStore() == null) {
            throw new SynapseException("cannot serialize an <mftSubmit/> with no store");
        }

        OMElement elem = fac.createOMElement("mftSubmit", synNS);
        elem.addAttribute(fac.createOMAttribute("store", nullNS, mediator.getStore()));
        if (mediator.getIdempotencyKey() != null) {
            elem.addAttribute(fac.createOMAttribute("idempotencyKey", nullNS,
                    text(mediator.getIdempotencyKey())));
        }

        for (MftSubmitMediator.Parameter parameter : mediator.getParameters()) {
            OMElement property = fac.createOMElement("property", synNS);
            property.addAttribute(fac.createOMAttribute("name", nullNS, parameter.name()));
            Value value = parameter.value();
            if (value.getExpression() != null) {
                property.addAttribute(fac.createOMAttribute("expression", nullNS,
                        value.getExpression().toString()));
            } else {
                property.addAttribute(fac.createOMAttribute("value", nullNS, value.getKeyValue()));
            }
            elem.addChild(property);
        }

        saveTracingState(elem, mediator);
        return elem;
    }

    /** An expression round-trips as {@code {…}}, which is how the factory recognises one. */
    private static String text(Value value) {
        return value.getExpression() == null
                ? value.getKeyValue()
                : "{" + value.getExpression().toString() + "}";
    }

    @Override
    public String getMediatorClassName() {
        return MftSubmitMediator.class.getName();
    }
}
