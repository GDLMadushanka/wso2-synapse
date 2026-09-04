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
import org.apache.synapse.stream.pipeline.StreamPipeline;

import javax.xml.namespace.QName;

/**
 * Writes a {@link StreamPipeline} back out, for artifact restore and the management API.
 *
 * <h2>Where the operator XML comes from</h2>
 * Operator configuration is re-emitted from the element the pipeline was built from, rather than
 * regenerated. That is a deliberate trade: it makes round-trip fidelity true <b>by construction</b>,
 * and it means an operator author does not have to write a serializer to go with every factory —
 * which they would eventually forget to keep in step with the parser.
 * <p>
 * Pipeline-level attributes are refreshed from the object, so a name or description changed through
 * the management API is reflected. Operator configuration changed programmatically would not be,
 * because nothing can currently change it programmatically; if that becomes possible, this is where
 * it has to be revisited.
 */
public class StreamPipelineSerializer {

    private static final OMFactory FACTORY = OMAbstractFactory.getOMFactory();
    private static final OMNamespace SYNAPSE_NS =
            FACTORY.createOMNamespace(XMLConfigConstants.SYNAPSE_NAMESPACE, "");

    private StreamPipelineSerializer() {
    }

    /**
     * @param parent   element to attach to, or {@code null} for a standalone artifact
     * @param pipeline the pipeline
     * @return the serialized element
     */
    public static OMElement serializeStreamPipeline(OMElement parent, StreamPipeline pipeline) {
        if (pipeline == null) {
            throw new SynapseException("cannot serialize a null stream pipeline");
        }

        OMElement source = pipeline.getSourceElement();
        if (source == null) {
            throw new SynapseException("stream pipeline '" + pipeline.getName() + "' has no source"
                    + " element to serialize; it was assembled programmatically rather than built"
                    + " from configuration");
        }

        OMElement out = source.cloneOMElement();

        // Refresh what the object owns. Attribute order is fixed so serialized artifacts diff
        // cleanly in version control.
        removeAttribute(out, "name");
        removeAttribute(out, "sourceProvided");
        removeAttribute(out, "sourceIdentity");
        out.addAttribute(FACTORY.createOMAttribute("name", null, pipeline.getName()));
        if (pipeline.isSourceProvided()) {
            out.addAttribute(FACTORY.createOMAttribute("sourceProvided", null, "true"));
        }
        // Written only when it is not the default, so an artifact nobody configured stays as authored.
        if (pipeline.getSourceIdentity() != SourceIdentityPolicy.WARN) {
            out.addAttribute(FACTORY.createOMAttribute("sourceIdentity", null,
                    pipeline.getSourceIdentity().name().toLowerCase(java.util.Locale.ROOT)));
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
