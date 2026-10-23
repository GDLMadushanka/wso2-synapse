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
 * Exactly one source per sequence, and it is first — unless the sequence declares
 * {@code sourceProvided="true"}, in which case the caller supplies a {@link StreamSeed} and the
 * sequence has no source at all.
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
     * Whether this source can begin at a byte offset instead of at the start.
     *
     * <p>Declaring {@code true} is a promise that the source will honour
     * {@link StreamContext#upstreamStartsAt()}: the first byte it delivers is the byte at that offset,
     * and it delivers nothing before it. A source that ignores the offset while claiming this produces
     * a transfer that is silently short — the sink believes the prefix was already written and appends
     * the wrong bytes after it.
     *
     * <p>Cheap to honour for anything addressable by range: a file seeks, a blob asks for
     * {@code bytes=N-}. Impossible for a stream that arrives once, which is why the default is false
     * and why a source must opt in rather than be assumed.
     *
     * <p>What it buys: a resumed transfer that skips the prefix rather than re-reading and discarding
     * it. At 500 GB with a failure at 450, the difference is hours of network for no output.
     */
    default boolean seekable() {
        return false;
    }

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
     * {@code sourceIdentity="strict"} means something on a sequence that opens its own source.
     *
     * <h2>Returning null is fine</h2>
     * The default. A source that cannot identify a resource cheaply should say so rather than guess: the
     * run then behaves exactly as it does today, resuming on the strength of its run id alone. What is
     * <b>not</b> fine is an identity that two different resources could share — see
     * {@link SourceIdentity}.
     *
     * <p>Called only when the answer can change something: a sequence keeping durable state, with the
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
