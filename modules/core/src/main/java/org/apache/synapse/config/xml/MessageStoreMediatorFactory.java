/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.synapse.config.xml;

import org.apache.axiom.om.OMAttribute;
import org.apache.axiom.om.OMElement;
import org.apache.synapse.Mediator;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.SynapseException;
import org.apache.synapse.mediators.Value;
import org.apache.synapse.mediators.store.MessageStoreMediator;
import org.jaxen.JaxenException;

import javax.xml.namespace.QName;
import java.util.Iterator;
import java.util.Properties;

/**
 * Creates an instance of a MessageStore mediator using XML configuration specified
 *
 * <pre>
 * &lt;store messageStore = "message store name" [sequence = "sequence name"]
 *        [type = "DEFAULT|MFT"] [idempotencyKey = "literal or {expression}"]&gt;
 *     &lt;property name="string" (value="literal or {expression}" | expression="path")/&gt;*
 * &lt;/store&gt;
 * </pre>
 *
 * <p>{@code idempotencyKey} and {@code <property/>} children belong to {@code type="MFT"} and are
 * <b>refused</b> without it, here at parse time rather than on the store the element names: the
 * discriminator is on the element, so a mistake is caught where it is written. Child elements other
 * than {@code <property/>} are ignored exactly as they always were.
 *
 * TODO Message store mediator will be improved with more user options
 */
public class MessageStoreMediatorFactory extends AbstractMediatorFactory{

    private static final QName STORE_Q    = new QName(XMLConfigConstants.SYNAPSE_NAMESPACE, "store");
    private static final QName ATT_MESSAGE_STORE   = new QName("messageStore");
    private static final QName ATT_SEQUENCE   = new QName("sequence");
    private static final QName ATT_TYPE = new QName("type");
    private static final QName ATT_IDEMPOTENCY_KEY = new QName("idempotencyKey");
    private static final QName PROPERTY_Q =
            new QName(XMLConfigConstants.SYNAPSE_NAMESPACE, "property");
    private static final QName ATT_VALUE = new QName("value");
    private static final QName ATT_EXPRESSION = new QName("expression");

    @Override
    protected Mediator createSpecificMediator(OMElement elem, Properties properties) {
        MessageStoreMediator messageStoreMediator = new MessageStoreMediator();
        processAuditStatus(messageStoreMediator, elem);
        OMAttribute nameAtt = elem.getAttribute(ATT_NAME);
        if(nameAtt != null) {
            messageStoreMediator.setName(nameAtt.getAttributeValue());
        }

        OMAttribute messageStoreNameAtt = elem.getAttribute(ATT_MESSAGE_STORE);

        if (messageStoreNameAtt != null) {
            if (checkForExpression(messageStoreNameAtt)) {
                String path = messageStoreNameAtt.getAttributeValue().substring(1, messageStoreNameAtt.getAttributeValue().length()-1);
                try {
                    messageStoreMediator.setMessageStoreExp(SynapsePathFactory.getSynapsePath(elem, path));
                } catch (JaxenException e) {
                    String msg = "Invalid XPath expression for attribute 'messageStore' : " + messageStoreNameAtt
                            .getAttributeValue();
                    throw new SynapseException(msg, e);
                }
            } else {
                messageStoreMediator.setMessageStoreName(FactoryUtils.getFullyQualifiedName(properties, messageStoreNameAtt.getAttributeValue()));
            }
        } else {
            throw new SynapseException("Message Store mediator must have a Message store defined");
        }

        OMAttribute sequenceAtt = elem.getAttribute(ATT_SEQUENCE);

        if(sequenceAtt != null) {
            messageStoreMediator.setOnStoreSequence(FactoryUtils.getFullyQualifiedName(properties, sequenceAtt.getAttributeValue()));
        }

        boolean mft = readType(elem, messageStoreMediator);
        readIdempotencyKey(elem, messageStoreMediator, mft);
        readProperties(elem, messageStoreMediator, mft);

        addAllCommentChildrenToList(elem, messageStoreMediator.getCommentsList());

        return messageStoreMediator;
    }

    /**
     * @return whether this element submits a transfer rather than storing a message
     */
    private static boolean readType(OMElement elem, MessageStoreMediator mediator) {
        OMAttribute typeAtt = elem.getAttribute(ATT_TYPE);
        if (typeAtt == null || typeAtt.getAttributeValue() == null
                || typeAtt.getAttributeValue().isBlank()) {
            return false;
        }
        String type = typeAtt.getAttributeValue().trim();
        if (MessageStoreMediator.MFT_TYPE.equalsIgnoreCase(type)) {
            mediator.setType(MessageStoreMediator.MFT_TYPE);
            return true;
        }
        if (MessageStoreMediator.DEFAULT_TYPE.equalsIgnoreCase(type)) {
            // Left null rather than stored, so the storing path is reached by the same comparison an
            // element with no type at all reaches it by.
            return false;
        }
        throw new SynapseException("<store type=\"" + type + "\"/> is not a known type. It is \""
                + MessageStoreMediator.DEFAULT_TYPE + "\" (the default, which stores a message) or \""
                + MessageStoreMediator.MFT_TYPE + "\" (which submits a transfer)");
    }

    private static void readIdempotencyKey(OMElement elem, MessageStoreMediator mediator, boolean mft) {
        OMAttribute key = elem.getAttribute(ATT_IDEMPOTENCY_KEY);
        if (key == null || key.getAttributeValue() == null || key.getAttributeValue().isBlank()) {
            return;
        }
        if (!mft) {
            throw new SynapseException("<store/> has an 'idempotencyKey' but no type=\""
                    + MessageStoreMediator.MFT_TYPE + "\". An idempotency key is a transfer\'s"
                    + " assertion that two submissions are the same request; storing a message has no"
                    + " such notion");
        }
        // ValueFactory, not a hand-rolled read of the braces: it is what already knows that
        // "{json-eval($.x)}" is an expression and plain text is a literal, on every other element.
        mediator.setIdempotencyKey(
                new ValueFactory().createValue(ATT_IDEMPOTENCY_KEY.getLocalPart(), elem));
    }

    private static void readProperties(OMElement elem, MessageStoreMediator mediator, boolean mft) {
        for (Iterator<?> it = elem.getChildElements(); it.hasNext(); ) {
            OMElement child = (OMElement) it.next();
            if (!PROPERTY_Q.equals(child.getQName())) {
                // Anything else stays ignored, as it has always been. Only <property/> is newly
                // meaningful, so only <property/> is newly refused where it cannot mean anything.
                continue;
            }
            if (!mft) {
                throw new SynapseException("<store/> has a <property/> child but no type=\""
                        + MessageStoreMediator.MFT_TYPE + "\". Properties are the values a transfer"
                        + " carries; a stored message carries its own payload");
            }
            mediator.addParameter(nameOf(child), propertyValue(child));
        }
    }

    private static String nameOf(OMElement property) {
        OMAttribute name = property.getAttribute(ATT_NAME);
        if (name == null || name.getAttributeValue() == null || name.getAttributeValue().isBlank()) {
            throw new SynapseException("a <property/> of <store type=\""
                    + MessageStoreMediator.MFT_TYPE + "\"/> has no 'name', so nothing could read it");
        }
        return name.getAttributeValue().trim();
    }

    /**
     * @return the literal or the expression, refusing both or neither — a property that carried both
     *         would leave which one wins to the reading order of two attributes
     */
    private static Value propertyValue(OMElement property) {
        OMAttribute value = property.getAttribute(ATT_VALUE);
        OMAttribute expression = property.getAttribute(ATT_EXPRESSION);

        if (value != null && expression != null) {
            throw new SynapseException("<property name=\"" + property.getAttributeValue(ATT_NAME)
                    + "\"/> has both \'value\' and \'expression\'; it takes one");
        }
        if (value != null) {
            // Through ValueFactory as well, so value="{json-eval($.x)}" means on this element what it
            // means on every other one.
            return new ValueFactory().createValue(ATT_VALUE.getLocalPart(), property);
        }
        if (expression != null) {
            // A bare path, the way every other mediator\'s expression= attribute is written -- no
            // braces. ValueFactory would read it as a literal string, since braces are how it tells an
            // expression from a key.
            try {
                return new Value(SynapsePathFactory.getSynapsePath(property, ATT_EXPRESSION));
            } catch (Exception e) {
                throw new SynapseException("<property name=\"" + property.getAttributeValue(ATT_NAME)
                        + "\"/> has an expression that could not be parsed", e);
            }
        }
        throw new SynapseException("<property name=\"" + property.getAttributeValue(ATT_NAME)
                + "\"/> has neither \'value\' nor \'expression\', so it carries nothing");
    }

    public QName getTagQName() {
       return STORE_Q;
    }

    /**
     * Checks whether the value of the attribute is enclosed by curly braces.
     * If yes, the value is taken as an expression.
     * Else, it will be a string value.
     *
     * @param atr the atrribute whose value is checked for enclosing curly braces.
     * @return true if the value is enclosed by curly braces.
     */
    private boolean checkForExpression(OMAttribute atr){
        return ((atr.getAttributeValue().startsWith("{")) && (atr.getAttributeValue().endsWith("}")));
    }
}
