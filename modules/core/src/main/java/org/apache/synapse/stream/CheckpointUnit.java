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
 * The unit a {@link Checkpoint} position is measured in.
 * <p>
 * A checkpoint that did not say would be ambiguous, and the ambiguity is not theoretical:
 * {@code Reader.skip()} skips <i>characters</i> while {@code InputStream.skip()} skips
 * <i>bytes</i>, so the same stored number resumes to a different place depending on how the body
 * was read.
 */
public enum CheckpointUnit {

    /** Position is a byte offset into the operator's input. */
    BYTES,

    /** Position is a count of records consumed from the operator's input. */
    RECORDS
}
