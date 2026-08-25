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

package org.apache.synapse.mediators.stream;

import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseException;
import org.apache.synapse.mediators.AbstractMediator;
import org.apache.synapse.stream.JobContext;
import org.apache.synapse.stream.StreamException;
import org.apache.synapse.stream.StreamSeed;
import org.apache.synapse.stream.pipeline.StreamPipeline;

import java.util.Set;

/**
 * Runs a deployed {@code <streamPipeline>} from inside a sequence.
 *
 * <pre>
 *   &lt;streamPipeline key="archive-ingest"/&gt;
 * </pre>
 *
 * Genuinely a mediator: by the time {@link #mediate} returns, the chain has been pulled to
 * completion.
 *
 * <h2>{@code isContentAware()} must stay false</h2>
 * Returning {@code true} makes Synapse build the message payload before mediation — reading the very
 * bytes a pipeline exists to stream. This is a correctness requirement, not a performance tuning
 * knob, and it is the single most damaging one-character change that could be made to this class.
 *
 * <h2>Lookup happens per invocation</h2>
 * The pipeline is resolved by key at {@code mediate()} time rather than when the sequence is built,
 * so redeploying a pipeline takes effect without rebuilding every sequence that references it.
 */
public class StreamPipelineMediator extends AbstractMediator {

    /** Message property a caller may use to supply a {@link JobContext}. */
    public static final String JOB_CONTEXT_PROPERTY = "mft.job.context";

    /** Message property a caller may use to supply a {@link StreamSeed}. */
    public static final String SEED_PROPERTY = "mft.stream.seed";

    /** Set on failure: the stage that failed, so a fault sequence can report it. */
    public static final String FAILED_STAGE_PROPERTY = "mft.failed.stage";

    /** Set on failure: whether a retry could plausibly help. */
    public static final String RETRYABLE_PROPERTY = "mft.retryable";

    private String key;

    @Override
    public boolean mediate(MessageContext synCtx) {
        if (key == null || key.isBlank()) {
            throw new SynapseException("<streamPipeline> has no 'key'");
        }

        Object artifact = synCtx.getConfiguration().getStreamPipeline(key);
        if (artifact == null) {
            throw new SynapseException("no stream pipeline is deployed under the key '" + key + "'");
        }
        StreamPipeline pipeline = (StreamPipeline) artifact;

        JobContext job = asJobContext(synCtx.getProperty(JOB_CONTEXT_PROPERTY));
        StreamSeed seed = consumeSeed(synCtx);

        try {
            pipeline.execute(synCtx, job, seed);
        } catch (StreamException e) {
            // Record what a fault sequence needs, then rethrow so the standard machinery runs it and
            // populates ERROR_CODE / ERROR_MESSAGE alongside these.
            synCtx.setProperty(FAILED_STAGE_PROPERTY, e.getStage());
            synCtx.setProperty(RETRYABLE_PROPERTY, e.isRetryable());
            throw new SynapseException("stream pipeline '" + key + "' failed"
                    + (e.getStage() == null ? "" : " at stage '" + e.getStage() + "'"), e);
        }
        return true;
    }

    /**
     * Never build the payload. See the class javadoc — this is a correctness requirement.
     */
    @Override
    public boolean isContentAware() {
        return false;
    }

    private static JobContext asJobContext(Object value) {
        return value instanceof JobContext job ? job : null;   // the pipeline substitutes NOOP
    }

    /**
     * Reads the seed and <b>removes it</b>, because a seed is good for exactly one run.
     *
     * <p>Leaving it in place made a second reference in the same sequence silently transfer nothing:
     * <pre>
     *   &lt;streamPipeline key="archive-to-s3"/&gt;
     *   &lt;streamPipeline key="archive-to-backup"/&gt;   &lt;!-- wrote an empty file --&gt;
     * </pre>
     * Both read the same seed, holding the same stream. The first drained it; the second found it at
     * end-of-file — still open, because a provided stream is close-shielded so its provider keeps
     * ownership — so {@code read()} returned -1 at once. The second pipeline built its whole chain,
     * read nothing, wrote an empty artifact and reported success.
     *
     * <p>Removing it turns that into the existing, correct error: a {@code sourceProvided} pipeline
     * with no stream is refused by name. Expressing fan-out this way is a mistake either way — the
     * design's answer is a composite sink — but it should fail loudly rather than produce an empty
     * destination file.
     */
    private static StreamSeed consumeSeed(MessageContext synCtx) {
        Object value = synCtx.getProperty(SEED_PROPERTY);
        if (!(value instanceof StreamSeed seed)) {
            return null;
        }
        Set<?> keys = synCtx.getPropertyKeySet();
        if (keys != null) {
            keys.remove(SEED_PROPERTY);
        }
        return seed;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    @Override
    public String getType() {
        return "StreamPipelineMediator";
    }
}
