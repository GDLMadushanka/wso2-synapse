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
import org.apache.synapse.stream.StreamContext;
import org.apache.synapse.stream.StreamOperator;
import org.apache.synapse.stream.StreamSink;

import javax.xml.namespace.QName;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/** Registered through the test classpath, to exercise the ServiceLoader pass. */
public class ClasspathTestOperatorFactory implements StreamOperatorFactory {

    static final QName TAG = new QName("http://ws.apache.org/ns/synapse", "test.classpathSink");

    @Override
    public QName getTagQName() {
        return TAG;
    }

    @Override
    public StreamOperator createOperator(OMElement elem, Properties properties) {
        // A real operator factory rejects unknown attributes rather than deploying a default. That
        // also makes it possible to prove the framework stripped its own attributes before we saw
        // the element: if it did not, this throws.
        for (java.util.Iterator<?> it = elem.getAllAttributes(); it.hasNext(); ) {
            org.apache.axiom.om.OMAttribute a = (org.apache.axiom.om.OMAttribute) it.next();
            if (!"name".equals(a.getQName().getLocalPart())) {
                throw new IllegalArgumentException(
                        "test.classpathSink got an unexpected attribute: " + a.getQName());
            }
        }
        return new Sink("test.classpathSink");
    }

    /** Trivial drain sink; enough to be a real operator. */
    public static class Sink implements StreamSink {

        private final String name;

        Sink(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public void consume(InputStream in, StreamContext ctx) throws IOException {
            byte[] buf = new byte[8192];
            while (in.read(buf) != -1) {
                // drain
            }
        }
    }
}
