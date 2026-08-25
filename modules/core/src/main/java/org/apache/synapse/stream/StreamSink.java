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

import java.io.IOException;
import java.io.InputStream;

/**
 * Tail of a byte chain, and the only operator that reads.
 * <p>
 * Exactly one sink per pipeline, and it is last. <b>All execution happens here</b>: the read
 * loop below drives every byte through every upstream decorator to the source. When
 * {@link #consume} returns, the transfer is done.
 *
 * @see StreamOperator
 */
public interface StreamSink extends StreamOperator {

    /**
     * Drains {@code in} to end-of-stream, writing it wherever this sink writes.
     * <p>
     * Obligations:
     * <ul>
     *   <li><b>Read to {@code -1} or throw.</b> Returning normally without draining reports
     *       success on a partial transfer.</li>
     *   <li><b>Do not return before the output is complete.</b> Where the output must be made
     *       durable, register that with
     *       {@link ResourceScope#registerCommitting(String, java.io.Closeable)} so a failed
     *       commit fails the transfer.</li>
     *   <li><b>Write atomically where you can</b> — temporary name, then rename. Writing
     *       straight to the destination means a failure part-way leaves a truncated artifact,
     *       and the pipeline cannot prevent that. Atomicity is a sink's responsibility.</li>
     *   <li><b>Poll {@link JobContext#isCancelled()} between reads</b> and throw a retryable
     *       {@link StreamException} when it becomes true. Cancellation is cooperative; a sink
     *       blocked inside a single long {@code read()} cannot be interrupted.</li>
     *   <li><b>Rate-limit {@link JobContext#progress}.</b> Per-read reporting is not
     *       conformant; use a bounded interval by byte count or elapsed time.</li>
     * </ul>
     *
     * @param in  the assembled chain; read it
     * @param ctx everything this operator gets for this one invocation
     * @throws StreamException if the transfer cannot be completed
     * @throws IOException     propagated from the chain, already attributed to the stage that
     *                         raised it
     */
    void consume(InputStream in, StreamContext ctx) throws StreamException, IOException;
}
