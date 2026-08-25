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
 * Head of a byte chain: produces a stream without reading it.
 * <p>
 * Exactly one source per pipeline, and it is first — unless the pipeline declares
 * {@code sourceProvided="true"}, in which case the caller supplies a {@link StreamSeed} and the
 * pipeline has no source at all.
 *
 * @see StreamOperator
 */
public interface StreamSource extends StreamOperator {

    /**
     * Returns a stream over this source's bytes, <b>without reading any of them</b>.
     * <p>
     * <b>Must perform no I/O.</b> No connect, no DNS lookup, no authentication, no open, no
     * stat, no read. Validating your own configuration, allocating buffers, constructing
     * decorators and registering resources with {@link StreamContext#resources()} are all fine.
     * Everything else is deferred to the first {@code read()} on the returned stream.
     * <p>
     * The reason is not tidiness: the build loop runs every operator before a byte moves, and a
     * resumed run discards the chain belonging to segments it is skipping. An eager connect
     * would hold a live remote handle, unused, for the whole run.
     * <p>
     * Emptiness is signalled by the returned stream yielding {@code -1} on its first read, never
     * by returning {@code null}.
     *
     * @param ctx everything this operator gets for this one invocation
     * @return a lazy stream over the source; never {@code null}
     * @throws StreamException if the configuration is unusable — detectable without I/O, and so
     *                         not retryable
     */
    InputStream open(StreamContext ctx) throws StreamException;

    /**
     * Whether a later attempt can obtain these bytes again.
     *
     * <p>This decides whether a run over this source is <b>durable</b> or <b>ephemeral</b>, so it is
     * the one capability that cannot live on the pipeline: the same pipeline is resumable when fed a
     * file and not resumable when fed a request body.
     *
     * <p><b>The default is {@link StreamOrigin#REOPENABLE}, which breaks the convention that every
     * capability defaults to the cautious answer.</b> That is deliberate, and the reasoning is worth
     * stating because it is the opposite of the neighbouring flags:
     *
     * <ul>
     *   <li>Wrongly defaulting to {@code ONE_SHOT} would <b>silently</b> take resume away from a
     *       500 GB file transfer. Nothing fails; the transfer simply starts from zero every time it
     *       is retried, and no message says why.</li>
     *   <li>Wrongly declaring {@code REOPENABLE} fails <b>loudly</b>: the second attempt tries to
     *       re-open, cannot, and reports it. Nothing is silently lost.</li>
     * </ul>
     *
     * Between a silent and a loud failure the loud one wins, and file-like sources — the
     * overwhelming majority — are genuinely re-openable. So silence means re-openable here, where
     * for {@code deterministic()} silence means the safe {@code false}.
     *
     * <p>Declare {@code ONE_SHOT} if your bytes arrive on a connection you do not control: a
     * multipart request body, a socket, a pipe. If you are unsure, you are probably re-openable —
     * the test is whether {@code open()} called tomorrow could return the same bytes.
     *
     * @return whether these bytes can be obtained again; never {@code null}
     */
    default StreamOrigin origin() {
        return StreamOrigin.REOPENABLE;
    }
}
