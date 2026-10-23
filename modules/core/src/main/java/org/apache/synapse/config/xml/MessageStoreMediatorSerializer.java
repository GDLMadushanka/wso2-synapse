/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *   * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.synapse.config.xml;

import org.apache.axiom.om.OMAttribute;
import org.apache.axiom.om.OMElement;
import org.apache.synapse.Mediator;
import org.apache.synapse.mediators.Value;
import org.apache.synapse.mediators.store.MessageStoreMediator;

import javax.xml.namespace.QName;

/**
 * Serialize  an instance of a MessageStore mediator to the XML configuration .
 * <pre>
 * &lt;store messageStore = "message store name" [sequence = "sequence name"] /&gt;
 * </pre>
 * TODO Message store mediator will be improved with more user options
 */
public class MessageStoreMediatorSerializer extends AbstractMediatorSerializer {

    private static final QName STORE_Q    = new QName(XMLConfigConstants.SYNAPSE_NAMESPACE, "store");
    private static final String  ATT_MESSAGE_STORE   = "messageStore";
    private static final String  ATT_SEQUENCE   = "sequence";
    private static final String  ATT_TYPE = "type";
    private static final String  ATT_IDEMPOTENCY_KEY = "idempotencyKey";

    @Override
    protected OMElement serializeSpecificMediator(Mediator m) {

        if (!(m instanceof MessageStoreMediator)) {
            handleException("Unsupported mediator passed in for serialization : " + m.getType());
        }


        MessageStoreMediator messageStoreMediator = (MessageStoreMediator)m;

        SynapsePath messageStoreExp = messageStoreMediator.getMessageStoreExp();

        String messageStoreName = messageStoreMediator.getMessageStoreName();

        OMElement storeElem = fac.createOMElement("store",synNS);

        String name = messageStoreMediator.getName();

        if(name != null) {
            OMAttribute nameAtt = fac.createOMAttribute("name" , nullNS , name);
            storeElem.addAttribute(nameAtt);
        }

        //In normal operations messageStoreName can't be null
        //But we do a null check here since in run time there can be manual modifications
        if (messageStoreExp != null) {
            SynapsePathSerializer.serializePathWithBraces(messageStoreExp, storeElem, "messageStore");
        } else if (messageStoreName != null) {
            OMAttribute msName = fac.createOMAttribute(ATT_MESSAGE_STORE, nullNS, messageStoreName);
            storeElem.addAttribute(msName);
        } else {
            handleException("Can't serialize MessageStore Mediator message store is null ");
        }


        String sequence = messageStoreMediator.getOnStoreSequence();
        // sequence is an optional parameter
        if(sequence != null) {
            OMAttribute sequenceAtt = fac.createOMAttribute(ATT_SEQUENCE , nullNS ,sequence);
            storeElem.addAttribute(sequenceAtt);
        }

        String type = messageStoreMediator.getType();
        // Written only when it is MFT: DEFAULT is parsed back to null, so writing it would turn an
        // element that round-tripped as it was written into one that gained an attribute.
        if (type != null) {
            storeElem.addAttribute(fac.createOMAttribute(ATT_TYPE, nullNS, type));
        }

        Value idempotencyKey = messageStoreMediator.getIdempotencyKey();
        if (idempotencyKey != null) {
            storeElem.addAttribute(fac.createOMAttribute(ATT_IDEMPOTENCY_KEY, nullNS,
                    text(idempotencyKey)));
        }

        for (MessageStoreMediator.Parameter parameter : messageStoreMediator.getParameters()) {
            OMElement property = fac.createOMElement("property", synNS);
            property.addAttribute(fac.createOMAttribute("name", nullNS, parameter.name()));
            Value value = parameter.value();
            if (value.getExpression() != null) {
                property.addAttribute(fac.createOMAttribute("expression", nullNS,
                        value.getExpression().toString()));
            } else {
                property.addAttribute(fac.createOMAttribute("value", nullNS, value.getKeyValue()));
            }
            storeElem.addChild(property);
        }

        serializeComments(storeElem, messageStoreMediator.getCommentsList());

        return storeElem;

    }

    /** An expression round-trips as {@code {…}}, which is how the factory recognises one. */
    private static String text(Value value) {
        return value.getExpression() == null
                ? value.getKeyValue()
                : "{" + value.getExpression().toString() + "}";
    }

    public String getMediatorClassName() {
        return MessageStoreMediator.class.getName();
    }
}
