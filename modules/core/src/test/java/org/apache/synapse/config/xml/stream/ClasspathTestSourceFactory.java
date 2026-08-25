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
import org.apache.synapse.stream.StreamSource;

import javax.xml.namespace.QName;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/** A trivial source, so tests can build a complete source-to-sink pipeline. */
public class ClasspathTestSourceFactory implements StreamOperatorFactory {

    static final QName TAG = new QName("http://ws.apache.org/ns/synapse", "test.classpathSource");

    @Override
    public QName getTagQName() {
        return TAG;
    }

    @Override
    public StreamOperator createOperator(OMElement elem, Properties properties) {
        return new Source();
    }

    static class Source implements StreamSource {

        @Override
        public String name() {
            return "test.classpathSource";
        }

        @Override
        public boolean deterministic() {
            return true;
        }

        @Override
        public InputStream open(StreamContext ctx) {
            return ctx.resources().register(name(),
                    new ByteArrayInputStream("test payload".getBytes(StandardCharsets.UTF_8)));
        }
    }
}
