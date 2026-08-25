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
 * Signals that a byte chain could not be built or could not be pulled to completion.
 * <p>
 * Carries two things the caller needs: <b>which stage failed</b>, and <b>whether retrying could
 * plausibly help</b>.
 *
 * <h2>Stage attribution</h2>
 * The stage is not a constructor argument. An operator does not name its own stage; the framework
 * stamps it via {@link #withStage(String)} as the exception passes outward, so attribution always
 * comes from the code that was executing rather than from a guess. {@code withStage} returns a
 * <b>copy</b> rather than mutating, so an identity assigned by an inner frame cannot be
 * overwritten by an outer one.
 *
 * <h2>Retryability</h2>
 * {@code true} means a later attempt could succeed with no change to input or configuration:
 * connection reset, timeout, read timeout, HTTP 429/502/503/504, transient DNS failure, disk
 * full, an expired token that will be refreshed, cooperative cancellation.
 * <p>
 * {@code false} means repeating will fail identically: malformed or truncated input, a wrong
 * decryption key, a failed integrity check, unknown host, HTTP 400/401/403/404/409, permission
 * denied on a path that will not change, any configuration error.
 * <p>
 * <b>Default to {@code false} when uncertain.</b> The asymmetry is the reason: a non-retryable
 * error marked retryable produces an infinite retry loop that presents as a hang, and nobody is
 * paged for a hang. The reverse merely turns a transient blip into a transfer someone restarts by
 * hand.
 * <p>
 * This class only reports a verdict. The pipeline never retries internally — retry count, backoff
 * and whether the transfer can be re-attempted at all belong to the caller.
 */
public class StreamException extends Exception {

    private static final long serialVersionUID = 1L;

    /** Stage this failure is attributed to, or {@code null} until the framework stamps one. */
    private final String stage;

    private final boolean retryable;

    /** Not retryable, unattributed. */
    public StreamException(String message) {
        this(message, null, null, false);
    }

    /** Unattributed, with an explicit retryability verdict. */
    public StreamException(String message, boolean retryable) {
        this(message, null, null, retryable);
    }

    /** Not retryable, unattributed, wrapping a cause. */
    public StreamException(String message, Throwable cause) {
        this(message, null, cause, false);
    }

    /** Unattributed, wrapping a cause, with an explicit retryability verdict. */
    public StreamException(String message, Throwable cause, boolean retryable) {
        this(message, null, cause, retryable);
    }

    private StreamException(String message, String stage, Throwable cause, boolean retryable) {
        super(message, cause);
        this.stage = stage;
        this.retryable = retryable;
    }

    /**
     * The stage this failure is attributed to.
     *
     * @return the stage name, or {@code null} if the framework has not stamped one yet
     */
    public String getStage() {
        return stage;
    }

    /**
     * Whether a later attempt could plausibly succeed unchanged. Advice to the caller, not an
     * instruction — see the class javadoc.
     *
     * @return the retryability verdict
     */
    public boolean isRetryable() {
        return retryable;
    }

    /**
     * Returns a copy attributed to {@code stage}, preserving message, cause and retryability.
     * <p>
     * If this exception is already attributed, returns {@code this} unchanged: the innermost
     * stage is the correct attribution and an outer frame must not overwrite it.
     *
     * @param stage the stage to attribute this failure to
     * @return an attributed copy, or {@code this} if already attributed
     */
    public StreamException withStage(String stage) {
        if (this.stage != null) {
            return this;
        }
        return new StreamException(getMessage(), stage, getCause(), retryable);
    }
}
