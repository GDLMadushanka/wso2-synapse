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

import java.io.Closeable;

/**
 * A resource whose close <b>publishes</b> work, and which therefore needs a way to discard it.
 *
 * <h2>The bug this exists to prevent</h2>
 * A materialising operator writes to a temporary name and renames it into place, so that the
 * presence of the final name means the artifact is complete — restart depends on exactly that.
 * The rename happens on close.
 *
 * <p>But a run's resources are closed on <b>every</b> exit path, success or failure, because
 * closing them is how handles are released. With only a {@code close()} method there is no way to
 * express "release the handle but do not publish", so a transfer that failed at 60% renamed its
 * truncated output into place and the next attempt skipped that segment as complete. A plausible,
 * complete-looking, wrong output file — the worst available outcome, since nothing reports it.
 *
 * <p>Note the failure that triggered it is the <i>ordinary</i> one: a reset connection, an expired
 * credential, a rejected write, a malformed record. A hard node death does not reach a
 * {@code finally} block at all and so was never the dangerous case here — it leaves the temporary
 * file unpublished, which is correct, if untidy.
 *
 * <h2>The contract</h2>
 * Exactly one of these is called, once:
 * <ul>
 *   <li>{@link #close()} — the run succeeded. Flush, publish, make the work visible.</li>
 *   <li>{@link #abort()} — the run failed or was cancelled. Release everything and leave
 *       <b>nothing</b> visible under a name another run could mistake for complete.</li>
 * </ul>
 *
 * <h2>Why an interface and not a flag on the scope</h2>
 * A wrapping resource must be able to forward the distinction, and a wrapper cannot forward an
 * {@code instanceof} test — the same reasoning that made the operator capabilities flags rather
 * than marker interfaces. Here the direction is reversed: because this <i>is</i> the method being
 * called rather than a fact being queried, requiring the type makes a wrapper's obligation a
 * compile error instead of a silent omission.
 *
 * @see ResourceScope#registerCommitting(String, StreamCommit)
 */
public interface StreamCommit extends Closeable {

    /**
     * Discards this resource's work and releases it.
     *
     * <p>Must leave nothing behind that a later run could read as a completed artifact. Deleting
     * the temporary file is the usual implementation; leaving it is acceptable only if its name
     * cannot be mistaken for a finished one.
     *
     * <p><b>Must be safe to call after a partial write, and must not throw for that reason
     * alone.</b> It runs while a failure is already being reported, so an exception here competes
     * with the diagnosis the caller actually needs. Throw only if the discard itself failed in a
     * way an operator must know about.
     *
     * @throws java.io.IOException if the work could not be discarded
     */
    void abort() throws java.io.IOException;
}
