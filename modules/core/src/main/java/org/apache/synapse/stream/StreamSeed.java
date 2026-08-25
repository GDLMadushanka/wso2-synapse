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
 * A stream supplied by the caller, together with the identity of what it came from.
 * <p>
 * Passed to a pipeline declaring {@code sourceProvided="true"}, whose first operator is a
 * transform rather than a source. The motivating case is a file inbound endpoint, which has
 * already opened the file — it holds the connection, the lock, the size and the modification
 * time, because it needed all of them to filter and lock the file. Making the pipeline re-open by
 * path would be wasteful, and racy: the file can change between the endpoint's stat and the
 * pipeline's open.
 *
 * <h2>The identity fields are not decoration</h2>
 * They feed the source-identity guard, which is what makes resume safe. Skip-forward is only
 * sound if the re-derived prefix is byte-identical, so a resumed run must be able to check that
 * the source has not changed underneath it. Without that check, a replaced source file causes a
 * resumed run to skip records of <i>different</i> data and produce a complete, plausible, corrupt
 * result.
 *
 * <h2>Ownership of the stream stays with the provider</h2>
 * The pipeline does not close it. A transform's {@code FilterInputStream.close()} cascades
 * downward and a resource scope closes what it registers, so the pipeline shields the stream
 * before building the chain to avoid closing it twice.
 *
 * <h2>The origin is required, and deliberately has no default</h2>
 * A {@link StreamSource} defaults to {@link StreamOrigin#REOPENABLE} because most sources are. A
 * seed cannot, because the provider is the only party that knows what it is holding: a file inbound
 * endpoint holds a file it could re-open, and an API mediator holds a request body it could not.
 * Defaulting either way would give one of those two callers the wrong answer silently, and both
 * callers are equally common. So every construction states it.
 *
 * @param stream       the already-open stream; never {@code null}
 * @param sourceId     stable identity of the origin, such as a full URI; never {@code null} or blank
 * @param size         byte length, or {@link #UNKNOWN} if the provider does not know
 * @param lastModified modification time in epoch millis, or {@link #UNKNOWN} if unknown
 * @param origin       whether a later attempt could obtain these bytes again; never {@code null}
 */
public record StreamSeed(InputStream stream, String sourceId, long size, long lastModified,
                         StreamOrigin origin) {

    /**
     * Sentinel for a size or modification time the provider could not determine.
     * <p>
     * Both are best-effort. A guard cannot verify what it was never told, so a seed carrying
     * {@code UNKNOWN} weakens resume rather than breaking it.
     */
    public static final long UNKNOWN = -1L;

    public StreamSeed {
        if (stream == null) {
            throw new IllegalArgumentException("seed stream is required");
        }
        if (sourceId == null || sourceId.isBlank()) {
            throw new IllegalArgumentException(
                    "seed sourceId is required: without it a resumed run cannot verify the source is unchanged");
        }
        if (size < UNKNOWN) {
            throw new IllegalArgumentException("seed size must be >= 0, or UNKNOWN: " + size);
        }
        if (lastModified < UNKNOWN) {
            throw new IllegalArgumentException("seed lastModified must be >= 0, or UNKNOWN: " + lastModified);
        }
        if (origin == null) {
            throw new IllegalArgumentException(
                    "seed origin is required: only the provider knows whether these bytes could be read"
                            + " again, and guessing it either loses resume silently or demands a job id"
                            + " the caller has no way to supply");
        }
    }

    /** @return {@code true} if a later attempt could obtain these bytes again */
    public boolean isReopenable() {
        return origin == StreamOrigin.REOPENABLE;
    }

    /** @return {@code true} if a byte length was supplied */
    public boolean hasSize() {
        return size != UNKNOWN;
    }

    /** @return {@code true} if a modification time was supplied */
    public boolean hasLastModified() {
        return lastModified != UNKNOWN;
    }
}
