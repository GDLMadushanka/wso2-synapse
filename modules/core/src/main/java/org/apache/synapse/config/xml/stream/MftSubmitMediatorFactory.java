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
import org.apache.synapse.config.xml.SynapsePathFactory;
import org.apache.synapse.config.xml.ValueFactory;
import org.apache.synapse.config.xml.XMLConfigConstants;
import org.apache.synapse.mediators.Value;
import org.apache.synapse.mediators.stream.MftSubmitMediator;

import java.util.Iterator;
import java.util.Properties;
import javax.xml.namespace.QName;

/**
 * Builds an {@code <mftSubmit/>} from its XML.
 *
 * <pre>
 *   &lt;mftSubmit store="mft-queue" idempotencyKey="{json-eval($.requestId)}"&gt;
 *       &lt;property name="connection" value="conn1"/&gt;
 *       &lt;property name="path" expression="json-eval($.source)"/&gt;
 *   &lt;/mftSubmit&gt;
 * </pre>
 *
 * <p>Unknown attributes and unknown children are refused rather than ignored. A misspelt
 * {@code <propery>} that deployed quietly would submit a transfer missing the one value its source
 * needed, and the failure would surface minutes later inside a run, naming an operator rather than the
 * typo.
 */
public class MftSubmitMediatorFactory extends AbstractMediatorFactory {

    private static final QName ATT_STORE = new QName("store");
    private static final QName ATT_IDEMPOTENCY_KEY = new QName("idempotencyKey");
    private static final QName PROPERTY_Q =
            new QName(XMLConfigConstants.SYNAPSE_NAMESPACE, "property");
    private static final QName ATT_NAME = new QName("name");
    private static final QName ATT_VALUE = new QName("value");
    private static final QName ATT_EXPRESSION = new QName("expression");

    @Override
    public QName getTagQName() {
        return XMLConfigConstants.MFT_SUBMIT_ELT;
    }

    @Override
    protected Mediator createSpecificMediator(OMElement elem, Properties properties) {
        MftSubmitMediator mediator = new MftSubmitMediator();

        OMAttribute store = elem.getAttribute(ATT_STORE);
        if (store == null || store.getAttributeValue() == null || store.getAttributeValue().isBlank()) {
            throw new SynapseException("<mftSubmit/> requires a 'store' naming the message store to"
                    + " submit to");
        }
        mediator.setStore(store.getAttributeValue().trim());

        OMAttribute key = elem.getAttribute(ATT_IDEMPOTENCY_KEY);
        if (key != null && key.getAttributeValue() != null && !key.getAttributeValue().isBlank()) {
            // ValueFactory, not a hand-rolled read of the braces: it is what already knows that
            // {json-eval(...)}, {${...}} and a plain XPath are three different things, and that a
            // literal containing braces can be escaped.
            mediator.setIdempotencyKey(new ValueFactory().createValue(
                    ATT_IDEMPOTENCY_KEY.getLocalPart(), elem));
        }

        for (Iterator<?> it = elem.getChildElements(); it.hasNext(); ) {
            OMElement child = (OMElement) it.next();
            if (!PROPERTY_Q.equals(child.getQName())) {
                throw new SynapseException("<mftSubmit store=\"" + mediator.getStore() + "\"> has a"
                        + " <" + child.getLocalName() + "> child. It takes <property/> children only,"
                        + " each one a value the transfer carries");
            }
            mediator.addParameter(nameOf(child, mediator.getStore()), propertyValue(child));
        }

        processAuditStatus(mediator, elem);
        return mediator;
    }

    private static String nameOf(OMElement property, String store) {
        OMAttribute name = property.getAttribute(ATT_NAME);
        if (name == null || name.getAttributeValue() == null || name.getAttributeValue().isBlank()) {
            throw new SynapseException("a <property/> of <mftSubmit store=\"" + store + "\"> has no"
                    + " 'name'; the name is what the pipeline's operators read the value as");
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
                    + "\"/> has both 'value' and 'expression'; it takes one");
        }
        if (value != null) {
            // Through ValueFactory as well, so value="{json-eval($.x)}" means on this element what it
            // means on every other one.
            return new ValueFactory().createValue(ATT_VALUE.getLocalPart(), property);
        }
        if (expression != null) {
            // A bare path, the way every other mediator's expression= attribute is written -- no
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
                + "\"/> has neither 'value' nor 'expression', so it carries nothing");
    }
}
