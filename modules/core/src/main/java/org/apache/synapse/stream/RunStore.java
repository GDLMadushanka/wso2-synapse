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

import java.util.List;

/**
 * Reads back what has run.
 *
 * <p>The query side of {@link JobStore}, separated because they are used by different people at
 * different times: a pipeline writes through {@code JobStore} while it executes, and a management API
 * reads through this afterwards. Splitting them keeps the write path — which is on every checkpoint —
 * free of methods it never calls, and keeps a read-only caller from holding something that can record.
 */
public interface RunStore {

    /**
     * @param runId the run to look up
     * @return it, or {@code null} when no run has that id
     * @throws StreamException if the store could not be reached
     */
    Run find(String runId) throws StreamException;

    /**
     * What each stage of one run did, in pipeline order.
     *
     * <p>Separate from {@link #find} because it answers a different question and costs a second query:
     * a listing wants one row per run, and this wants the breakdown of one of them. Empty while the run
     * is still going — the rows are all written at the end (see {@link Stage}) — which a caller must
     * present as "not yet" rather than as "no stages".
     *
     * @param runId the run to break down
     * @return its stages ordered by index; empty when the run has not finished or does not exist
     * @throws StreamException if the store could not be reached
     */
    List<Stage> stages(String runId) throws StreamException;

    /**
     * Lists runs, newest first.
     *
     * @param filter what to match; never {@code null}
     * @return the matching runs; empty when there are none
     * @throws StreamException if the store could not be reached
     */
    List<Run> list(RunFilter filter) throws StreamException;

    /**
     * What a listing is narrowed by. Every field is optional and {@code null} means "any".
     *
     * <p>{@code invoker} is the one that is not a column comparison. It asks whether an artifact
     * appears <b>anywhere in the invocation path</b> — "show me everything that came through
     * {@code mft-runner}", which for a queued transfer is the third segment and for nothing else is a
     * segment at all. An implementation may pre-filter it in SQL with a substring test, but the answer
     * must be segment equality: a substring test alone answers {@code api1} with {@code api10}'s rows.
     *
     * @param pipeline    the pipeline that ran
     * @param status      {@code RUNNING}, {@code DONE}, {@code FAILED}, {@code INTERRUPTED}
     * @param invokerType {@code API}, {@code INBOUND}, {@code PROXY}, {@code QUEUED}, {@code DIRECT}
     * @param invoker     an artifact anywhere in the invocation path
     * @param limit       the most to return; at least 1
     */
    record RunFilter(String pipeline, String status, String invokerType, String invoker, int limit) {

        public RunFilter {
            limit = Math.max(1, limit);
        }
    }
}
