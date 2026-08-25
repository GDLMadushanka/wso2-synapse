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
 * Whether the bytes a run reads can be obtained again by a later attempt.
 *
 * <h2>Why this is not a property of the pipeline</h2>
 * It is tempting to treat "resumable" as something a pipeline declares, but the same pipeline —
 * the same operators, the same configuration — is resumable when fed a file and not resumable when
 * fed the body of an HTTP request. Nothing about the operators changed; the origin did. So this is
 * the axis on which a run is durable or ephemeral, and it belongs to the source, never to the
 * artifact.
 *
 * <p>The consequence is deliberate: one pipeline artifact can be referenced from a file inbound
 * endpoint <i>and</i> from an API resource handling a multipart body, and each invocation gets the
 * behaviour that is correct for it. A pipeline-level mode attribute would force the author to pick
 * one and copy the pipeline to get the other.
 *
 * <h2>This is not about side effects</h2>
 * A one-shot origin can still feed an operator whose effects would be harmful to repeat — there is
 * simply never a second attempt in which to repeat them. How much work a failure may cost is a separate
 * question, answered per stage by {@code maxReprocessed} and by nothing an operator declares.
 *
 * @see StreamSource#origin()
 * @see StreamSeed
 */
public enum StreamOrigin {

    /**
     * A later attempt can obtain the same bytes again — a file, an object store entry, an SFTP path.
     *
     * <p>This only claims that re-opening is <i>possible</i>. It does not claim the bytes will be
     * unchanged: that is what the source-identity guard verifies, using the {@code sourceId},
     * {@code size} and {@code lastModified} recorded when the run first started. The two compose —
     * this says an attempt is worth making, the guard says whether what it found is the same data.
     */
    REOPENABLE,

    /**
     * The bytes exist once and are gone when the run ends — a multipart request body, a socket, a
     * one-shot pipe.
     *
     * <p>There is no second attempt, so checkpoints and restart-economy spills cost something and
     * buy nothing, and the run needs no job id. A structurally materialising operator such as a
     * sort still gets somewhere to write, but that space is <b>scratch</b>: reclaimed when the run
     * ends, never resumed from.
     */
    ONE_SHOT
}
