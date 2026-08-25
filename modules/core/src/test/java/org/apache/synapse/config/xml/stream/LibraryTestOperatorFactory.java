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
import org.apache.synapse.stream.StreamOperator;

import javax.xml.namespace.QName;
import java.util.Properties;

/**
 * Deliberately <b>not</b> listed in the test classpath services file. A test registers it only via a
 * jar loaded through a {@code LibClassLoader}, which is how an operator inside a deployed connector
 * becomes visible.
 */
public class LibraryTestOperatorFactory implements StreamOperatorFactory {

    static final QName TAG = new QName("http://ws.apache.org/ns/synapse", "test.librarySink");

    @Override
    public QName getTagQName() {
        return TAG;
    }

    @Override
    public StreamOperator createOperator(OMElement elem, Properties properties) {
        return new ClasspathTestOperatorFactory.Sink("test.librarySink");
    }
}
