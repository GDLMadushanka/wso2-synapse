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

/**
 * Somewhere outside this process that a sink writes, asked where it really got to.
 *
 * <p>The remote counterpart to {@link StageArtifact}. That one is a file the framework owns and can
 * size and truncate itself; this is a part file over SFTP, a set of staged blocks in Azure, an S3
 * multipart upload — state only the operator can reach. Two questions are all the framework needs,
 * and it keeps the reasoning that uses them.
 *
 * <h2>Why the framework must not be told, but must ask</h2>
 * A checkpoint is written <i>after</i> the work it describes, so it is at or behind reality and never
 * ahead — unless something went wrong, in which case it is ahead and following it would skip data
 * that was never written. Reconciling the two is the same argument every time, and getting it wrong
 * is silent: a blob the right length and wrong in the middle, which no length check downstream
 * catches. {@code file.streamWrite} and {@code azure.blobStreamWrite} had each written their own copy
 * of it before this existed, and a third sink would have written a third.
 *
 * <p>This is the gap {@link StageArtifact} left. ADR-0031 took materialising state out of operators'
 * hands for exactly this reason; the case it never covered is a <b>checkpointed sink writing
 * somewhere the framework cannot see</b>, which is every sink that moves bytes off this machine.
 *
 * <h2>The number is in the stage's own unit</h2>
 * {@link CheckpointUnit#BYTES} for a file sink, {@link CheckpointUnit#RECORDS} for a block upload
 * counting blocks. <b>Position, not length</b>: calling it a length would bake bytes into a contract
 * that is not about bytes, and the framework never converts between units — it compares two numbers
 * in the same unit and keeps the smaller. Converting to a byte offset for a seekable source is the
 * operator's job, because only it knows what a unit is worth.
 *
 * <h2>Implementations</h2>
 * <table>
 *   <caption>what the two questions mean per destination</caption>
 *   <tr><th>sink</th><th>unit</th><th>{@code durablePosition}</th><th>{@code discardAbove}</th></tr>
 *   <tr><td>file / SMB</td><td>BYTES</td><td>size of the part file</td><td>truncate it</td></tr>
 *   <tr><td>Azure block blob</td><td>RECORDS</td><td>first gap in the uncommitted block list</td>
 *       <td>nothing — a block above the resume point is never named by the commit</td></tr>
 *   <tr><td>S3 multipart</td><td>RECORDS</td><td>{@code ListParts}, first gap</td>
 *       <td>nothing, same reason</td></tr>
 *   <tr><td>SFTP</td><td>BYTES</td><td>remote stat</td><td>remote truncate</td></tr>
 * </table>
 *
 * <p>That {@code discardAbove} is a no-op for half of them is not a flaw in the contract; it is the
 * difference between a destination that publishes as it goes and one that publishes at the end, and
 * it is why this is two methods rather than one.
 *
 * <h2>Lifetime</h2>
 * Built per run, inside {@code consume()}, from whatever the operator resolved for that run. Never a
 * field: one operator instance serves concurrent runs (invariant 4), and a destination holding one
 * run's connection would hand it to another.
 */
public interface StageDestination {

    /**
     * How far the destination can prove it got, in this stage's checkpoint unit.
     *
     * <p>Proof, not recollection. The answer must come from the destination — a remote stat, a block
     * listing — and not from anything this process remembered, or it cannot contradict the checkpoint
     * and the reconciliation is theatre.
     *
     * <p>{@code 0} when there is nothing there, which is the ordinary first-attempt answer and not a
     * failure. A destination that cannot be reached should throw rather than answer zero: zero means
     * "it holds nothing", and saying that about an unreachable destination would re-send everything.
     */
    long durablePosition() throws IOException;

    /**
     * Make {@code position} the end of it, discarding anything beyond.
     *
     * <p>Called only when the destination holds <i>more</i> than the checkpoint recorded — work that
     * happened but was never recorded, and is about to happen again. Leaving it would duplicate a
     * stretch of the output.
     *
     * <p>A no-op is a correct implementation wherever the surplus is invisible until a commit names
     * it. Azure's uncommitted blocks are the example: no reader can see them, {@code Put Block List}
     * will not name them, and the service reclaims them on its own.
     *
     * @param position the position to cut back to, in this stage's checkpoint unit
     */
    void discardAbove(long position) throws IOException;
}
