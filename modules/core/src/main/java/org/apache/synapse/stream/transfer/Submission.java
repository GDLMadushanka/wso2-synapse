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

package org.apache.synapse.stream.transfer;

/**
 * What a submission produced: an id, and whether this call is what created it.
 *
 * <p>{@code created == false} means the id belongs to a transfer that was already there — the same
 * idempotency key, or identical parameters still in flight. The caller gets the same id either way,
 * because from their side one request produced one transfer; the flag is for a response that wants to
 * say {@code 202 Accepted} rather than "already accepted", and for a log that wants to be honest about
 * how many transfers exist.
 *
 * @param jobId   the transfer's id
 * @param created whether this call queued it
 */
public record Submission(long jobId, boolean created) {
}
