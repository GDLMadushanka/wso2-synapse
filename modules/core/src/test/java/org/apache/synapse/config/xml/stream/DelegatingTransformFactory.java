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
import org.apache.synapse.stream.StreamTransform;

import javax.xml.namespace.QName;
import java.io.InputStream;
import java.util.Properties;

/** A checkpointed, materialising transform that asserts nothing about replay — the {@code forEach} shape. */
public class DelegatingTransformFactory implements StreamOperatorFactory {

    static final QName TAG = new QName("http://ws.apache.org/ns/synapse", "test.delegatingForEach");

    @Override
    public QName getTagQName() {
        return TAG;
    }

    @Override
    public StreamOperator createOperator(OMElement elem, Properties properties) {
        return new Op();
    }

    /** Materialises and checkpoints, but cannot know whether replaying a record is harmful. */
    public static class Op implements StreamTransform {

        @Override
        public String name() {
            return "test.delegatingForEach";
        }

        @Override
        public boolean checkpointed() {
            return true;
        }

        @Override
        public boolean materialises() {
            return true;
        }

        @Override
        public InputStream wrap(InputStream in, StreamContext ctx) {
            return in;
        }
    }
}
