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

package org.apache.synapse.stream;

import java.io.InputStream;

/**
 * Middle link of a byte chain: decorates the stream it is given, without reading it.
 *
 * @see StreamOperator
 */
public interface StreamTransform extends StreamOperator {

    /**
     * Returns a decorator over {@code in}, <b>without reading from it</b>.
     * <p>
     * <b>Must perform no I/O</b>, including on {@code in} — not one byte, not
     * {@code available()}. The most common way to break this is not your own code but a library
     * constructor: {@code GZIPInputStream} reads its header when constructed, and
     * {@code ZipInputStream}, some {@code CipherInputStream} providers and most format readers
     * do likewise. Wrap them lazily and construct on the first {@code read()}.
     * <p>
     * Three further obligations on the returned stream:
     * <ul>
     *   <li>Closing it should close {@code in}. Extending {@code FilterInputStream} gives this.</li>
     *   <li><b>Never return {@code in} unchanged</b> as a way of expressing a no-op. A
     *       pass-through must still be a distinct wrapper, or per-stage accounting is wrong.</li>
     *   <li><b>Never return {@code 0}</b> from {@code read()} for a {@code len > 0} request.
     *       There is no "not yet" state in the contract and callers may spin.</li>
     * </ul>
     * Failures inside the returned stream surface as {@code IOException} from its
     * {@code read()}; the enclosing stage wrapper converts them and attributes the stage.
     *
     * @param in  the upstream stream; must not be read here
     * @param ctx everything this operator gets for this one invocation
     * @return a decorator over {@code in}; never {@code null} and never {@code in} itself
     * @throws StreamException if the configuration is unusable
     */
    InputStream wrap(InputStream in, StreamContext ctx) throws StreamException;
}
