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

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.OMNamespace;
import org.apache.synapse.SynapseException;
import org.apache.synapse.config.xml.XMLConfigConstants;
import org.apache.synapse.stream.SourceIdentityPolicy;
import org.apache.synapse.stream.sequence.StreamSequence;

import javax.xml.namespace.QName;

/**
 * Writes a {@link StreamSequence} back out, for artifact restore and the management API.
 *
 * <h2>Where the operator XML comes from</h2>
 * Operator configuration is re-emitted from the element the sequence was built from, rather than
 * regenerated. That is a deliberate trade: it makes round-trip fidelity true <b>by construction</b>,
 * and it means an operator author does not have to write a serializer to go with every factory —
 * which they would eventually forget to keep in step with the parser.
 * <p>
 * Sequence-level attributes are refreshed from the object, so a name or description changed through
 * the management API is reflected. Operator configuration changed programmatically would not be,
 * because nothing can currently change it programmatically; if that becomes possible, this is where
 * it has to be revisited.
 */
public class StreamSequenceSerializer {

    private static final OMFactory FACTORY = OMAbstractFactory.getOMFactory();
    private static final OMNamespace SYNAPSE_NS =
            FACTORY.createOMNamespace(XMLConfigConstants.SYNAPSE_NAMESPACE, "");

    private StreamSequenceSerializer() {
    }

    /**
     * @param parent   element to attach to, or {@code null} for a standalone artifact
     * @param sequence the sequence
     * @return the serialized element
     */
    public static OMElement serializeStreamSequence(OMElement parent, StreamSequence sequence) {
        if (sequence == null) {
            throw new SynapseException("cannot serialize a null stream sequence");
        }

        OMElement source = sequence.getSourceElement();
        if (source == null) {
            throw new SynapseException("stream sequence '" + sequence.getName() + "' has no source"
                    + " element to serialize; it was assembled programmatically rather than built"
                    + " from configuration");
        }

        OMElement out = source.cloneOMElement();

        // Refresh what the object owns. Attribute order is fixed so serialized artifacts diff
        // cleanly in version control.
        removeAttribute(out, "name");
        removeAttribute(out, "sourceIdentity");
        removeAttribute(out, "resume");
        out.addAttribute(FACTORY.createOMAttribute("name", null, sequence.getName()));
        // No sourceProvided: an absent <source> section says it, and the sections come through on
        // the cloned element untouched. See ADR-0068.
        // Written only when it is not the default, so an artifact nobody configured stays as authored.
        if (sequence.getSourceIdentity() != SourceIdentityPolicy.WARN) {
            out.addAttribute(FACTORY.createOMAttribute("sourceIdentity", null,
                    sequence.getSourceIdentity().name().toLowerCase(java.util.Locale.ROOT)));
        }
        // Only ever "false": the factory refuses resume="true", so round-tripping it would emit a
        // value that will not parse back.
        if (!sequence.isResume()) {
            out.addAttribute(FACTORY.createOMAttribute("resume", null, "false"));
        }

        if (parent != null) {
            parent.addChild(out);
        }
        return out;
    }

    private static void removeAttribute(OMElement elem, String localName) {
        var attr = elem.getAttribute(new QName(localName));
        if (attr != null) {
            elem.removeAttribute(attr);
        }
    }
}
