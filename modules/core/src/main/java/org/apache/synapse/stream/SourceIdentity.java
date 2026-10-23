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

/**
 * What a source says it is about to read, obtained without reading it.
 *
 * <p>The same three values a {@link StreamSeed} carries, for the case where nobody handed the sequence
 * a stream: a queued transfer names a resource, and only the source operator knows how to look at it.
 *
 * <h2>The identity is opaque</h2>
 * The framework compares it for equality and nothing else — it has no opinion about what is inside
 * (ADR-0033). What goes in is the provider's call, and the test is only this: <b>two identities that
 * compare equal must be the same bytes</b>. Too weak is worse than absent, because {@code null} costs a
 * re-run while a collision costs correctness.
 *
 * <p>Protocols differ in how well they can answer. A VFS stat gives size and last-modified, which cannot
 * tell apart a file rewritten to the same length within the same second. An object store gives an ETag,
 * which can. A source should use the strongest token it has.
 *
 * <p>What must <b>not</b> go in: anything with a credential in it. A resolved VFS URI embeds
 * {@code username:password@}, and this value is written to a job row and logged when the guard reports a
 * mismatch.
 *
 * @param identity     compared for equality; at most {@link StreamSeed#MAX_IDENTITY_CHARS} characters
 * @param size         bytes, or {@link StreamSeed#UNKNOWN} when the source cannot say cheaply
 * @param lastModified epoch millis, or {@link StreamSeed#UNKNOWN}
 */
public record SourceIdentity(String identity, long size, long lastModified) {

    public SourceIdentity {
        if (identity != null && identity.isBlank()) {
            identity = null;
        }
        if (identity != null && identity.length() > StreamSeed.MAX_IDENTITY_CHARS) {
            throw new IllegalArgumentException("a source identity may be at most "
                    + StreamSeed.MAX_IDENTITY_CHARS + " characters; hash it down rather than handing"
                    + " over something this long, which is refused rather than truncated because a"
                    + " truncated identity compares equal to a different source");
        }
        if (size < 0L) {
            size = StreamSeed.UNKNOWN;
        }
        if (lastModified < 0L) {
            lastModified = StreamSeed.UNKNOWN;
        }
    }

    /**
     * An identity with no measurements, for a source that can name a resource but not size it.
     *
     * @param identity what names the resource
     * @return the identity
     */
    public static SourceIdentity of(String identity) {
        return new SourceIdentity(identity, StreamSeed.UNKNOWN, StreamSeed.UNKNOWN);
    }

    /** Whether this says anything the framework can compare. */
    public boolean hasIdentity() {
        return identity != null;
    }
}
