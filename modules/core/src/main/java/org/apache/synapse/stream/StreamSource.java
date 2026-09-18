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

    /**
     * Says what this source is about to read, <b>without reading it</b>.
     *
     * <h2>I/O is expected here — this is the one place it is</h2>
     * The opposite of {@link #open(StreamContext)}, deliberately. One stat, one {@code HEAD}, one
     * metadata call: enough to name the resource and measure it, and no more. Everything
     * {@code open()} defers exists because the build loop runs before a byte moves; this runs
     * <b>once</b>, before the run is named, precisely so the answer can decide what happens next.
     *
     * <h2>What it is for</h2>
     * A queued transfer has no seed, so without this the framework knows nothing about the resource
     * until the first {@code read()} — by which time the workspace has been chosen and a previous
     * attempt's artifacts may already have been adopted. With it, a second attempt over a <i>replaced</i>
     * resource is caught before it can splice two sources into one artifact, and
     * {@code sourceIdentity="strict"} means something on a pipeline that opens its own source.
     *
     * <h2>Returning null is fine</h2>
     * The default. A source that cannot identify a resource cheaply should say so rather than guess: the
     * run then behaves exactly as it does today, resuming on the strength of its run id alone. What is
     * <b>not</b> fine is an identity that two different resources could share — see
     * {@link SourceIdentity}.
     *
     * <p>Called only when the answer can change something: a pipeline keeping durable state, with the
     * source-identity guard on. A scratch run pays no round trip.
     *
     * @param ctx everything this operator gets for this one invocation
     * @return what this source would open, or {@code null} if it cannot say cheaply
     * @throws StreamException if the resource cannot be reached or does not exist. Retryable when it
     *                         may appear later, which is the usual case for a queued transfer
     */
    default SourceIdentity identify(StreamContext ctx) throws StreamException {
        return null;
    }
}
