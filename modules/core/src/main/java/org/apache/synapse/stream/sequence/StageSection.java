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

package org.apache.synapse.stream.sequence;

import org.apache.synapse.stream.StreamSink;
import org.apache.synapse.stream.StreamSource;
import org.apache.synapse.stream.StreamTransform;

/**
 * The section of a {@code <streamSequence>} a stage was written in (ADR-0068).
 *
 * <h2>Why the configuration says this at all</h2>
 * The role is already decidable from the class — {@code validate()} reads it with {@code instanceof}
 * and refuses an operator claiming two. The section is not there to tell the framework anything it
 * could not work out. It is there so that an <b>editor</b> can, because a schema validates structure
 * at edit time without loading a class, and "exactly one source, at most one sink" is cardinality
 * that no interface can express.
 *
 * <p>So this carries the deployer's <b>claim</b>, and {@link StreamSequence#validate()} checks the
 * claim against the class once the class exists. For a connector operation that is at {@code init()},
 * not at parse time, because the operator lives in a template the library deployer has not created
 * yet — see ADR-0026.
 *
 * <p>A sequence assembled programmatically declares nothing and carries {@code null} throughout.
 * That is not a gap: there is no document for an editor to check, and the positional rules in
 * {@code validate()} govern it exactly as they always did.
 */
public enum StageSection {

    /** {@code <source>} — one {@link StreamSource}. Absent means the caller supplies the bytes. */
    SOURCE("source", StreamSource.class),

    /** {@code <transforms>} — zero or more {@link StreamTransform}. */
    TRANSFORMS("transforms", StreamTransform.class),

    /**
     * {@code <sink>} — one {@link StreamSink}.
     *
     * <p>Absent means the framework drains the chain itself and the last transform's artifact is the
     * output. That is not a special case added for this grammar: {@code execute()} has always
     * supplied the pull when no operator does, because a sequence may end in a materialising
     * transform. The section is optional so that this stays expressible <b>and</b> {@code <sink>}
     * keeps meaning exactly {@code StreamSink}.
     */
    SINK("sink", StreamSink.class);

    private final String elementName;
    private final Class<?> role;

    StageSection(String elementName, Class<?> role) {
        this.elementName = elementName;
        this.role = role;
    }

    /** The element name as written in configuration. */
    public String elementName() {
        return elementName;
    }

    /** The role interface an operator in this section must implement. */
    public Class<?> role() {
        return role;
    }

    /** Whether this section may hold more than one operator. */
    public boolean allowsMany() {
        return this == TRANSFORMS;
    }

    /** The section whose element name this is, or {@code null} if it is not a section at all. */
    public static StageSection forElement(String localName) {
        for (StageSection section : values()) {
            if (section.elementName.equals(localName)) {
                return section;
            }
        }
        return null;
    }

    /** The three names, in chain order, for a message that has to list them. */
    public static String names() {
        return "<source>, <transforms>, <sink>";
    }
}
