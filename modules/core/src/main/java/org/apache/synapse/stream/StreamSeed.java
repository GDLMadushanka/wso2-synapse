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
 * <h2>The identity is the provider's, and the framework never looks inside it</h2>
 * The framework does exactly two things with {@link #identity()}: it hashes it to name the run, and it
 * compares it for equality to notice that a source changed. It has no opinion about what makes two
 * sources the same, because it cannot have one — only the provider knows. A file inbound endpoint knows
 * that a file replaced in place is a different source; an email inbound knows that an IMAP
 * {@code UIDVALIDITY + UID} names a message for the life of a mailbox and needs no timestamp; an API
 * mediator holding a request body knows it can never recognise those bytes again.
 * <p>
 * So the identity must carry <b>everything that distinguishes this source from a different one</b>:
 * <pre>
 *   file inbound     uri + '|' + size + '|' + mtime        a replaced file is a new identity
 *   email inbound    uidvalidity + '|' + uid + '|' + part  no modification time exists, or is needed
 *   http multipart   null                                  cannot be recognised again
 * </pre>
 * This replaces an earlier design in which the framework composed identity from {@code sourceId},
 * {@code size} and {@code lastModified} itself. That was a filesystem's notion of sameness promoted to
 * a universal rule, and the second provider broke it — see
 * ADR-0033.
 *
 * <h2>Which makes this an unenforceable promise, and a dangerous one</h2>
 * <b>Two seeds with equal identity must be the same bytes.</b> Nothing checks this and nothing can.
 * Resume works by skipping a prefix it believes it already wrote, so an identity that collides across
 * <i>different</i> sources makes a resumed run skip records of different data and commit a complete,
 * plausible, corrupt artifact — no exception, no warning, a file that looks right. An identity too weak
 * to distinguish two sources is worse than no identity at all: {@code null} costs a re-run, a collision
 * costs correctness. When in doubt, include more.
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
 * @param identity     everything that distinguishes this source from a different one, as one opaque
 *                     value, or {@code null} for a source that could never be recognised again — which
 *                     makes the run scratch. Read the promise above before choosing one
 * @param size         byte length, or {@link #UNKNOWN} if the provider does not know. <b>Telemetry
 *                     only</b> — it feeds progress reporting and diagnostic messages, and no decision
 *                     about whether two sources are the same. Fold it into {@code identity} if it is
 *                     part of what distinguishes them
 * @param lastModified modification time in epoch millis, or {@link #UNKNOWN} if unknown. Telemetry
 *                     only, for the same reason as {@code size}
 * @param origin       whether a later attempt could obtain these bytes again; never {@code null}
 */
public record StreamSeed(InputStream stream, String identity, long size, long lastModified,
                         StreamOrigin origin) {

    /**
     * Sentinel for a size or modification time the provider could not determine.
     * <p>
     * Both are best-effort. A guard cannot verify what it was never told, so a seed carrying
     * {@code UNKNOWN} weakens resume rather than breaking it.
     */
    public static final long UNKNOWN = -1L;

    /**
     * Longest identity that can be recorded, matching {@code MFT_JOB.SOURCE_ID VARCHAR(1024)}.
     *
     * <p>Refused rather than truncated, for the reason {@code Checkpoint}'s payload cap is: a truncated
     * identity still <i>looks</i> like one, and two sources whose identities agree for the first
     * kilobyte would silently become the same run. Refusing is loud and happens before any work.
     *
     * <p>A provider needing more should hash its own material down — it knows which parts matter, and
     * the framework only ever compares the result for equality.
     */
    public static final int MAX_IDENTITY_CHARS = 1024;

    public StreamSeed {
        if (stream == null) {
            throw new IllegalArgumentException("seed stream is required");
        }
        // Blank normalises to absent. A provider that has no identity says so with null; one that
        // accidentally builds "" has the same amount of information, and treating the two differently
        // would make an empty string name a run that every other empty string also names.
        if (identity != null && identity.isBlank()) {
            identity = null;
        }
        if (identity != null && identity.length() > MAX_IDENTITY_CHARS) {
            throw new IllegalArgumentException("seed identity is " + identity.length()
                    + " characters, over the " + MAX_IDENTITY_CHARS + " that can be recorded; hash it"
                    + " down -- only equality is ever compared");
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
    /**
     * Whether this seed can name a run, and therefore whether anything could resume it.
     *
     * <p>Not the same question as {@link #isReopenable()}, and the two are independent. A source that
     * can be re-read but not recognised cannot resume — there is nothing to look up. A source that can
     * be recognised but not re-read cannot resume either. Both are required.
     */
    public boolean hasIdentity() {
        return identity != null;
    }

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
