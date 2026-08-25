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
import org.apache.synapse.mediators.TestUtils;
import org.apache.synapse.stream.StreamContext;
import org.apache.synapse.stream.StreamOrigin;
import org.apache.synapse.stream.StreamSeed;
import org.apache.synapse.stream.StreamSink;
import org.apache.synapse.stream.pipeline.StreamPipeline;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * A seed is good for exactly one run, and the mediator is what enforces that.
 */
public class StreamPipelineMediatorTest {

    private static StreamSeed seed() {
        return new StreamSeed(new ByteArrayInputStream("payload".getBytes()), "file:///in/data.csv",
                7L, 0L, StreamOrigin.REOPENABLE);
    }

    /** A sink that keeps what it was given, so a run's byte count is observable. */
    private static final class CapturingSink implements StreamSink {

        private byte[] bytes = null;

        @Override
        public String name() {
            return "capture";
        }

        @Override
        public void consume(InputStream in, StreamContext ctx) throws IOException {
            bytes = in.readAllBytes();
        }
    }

    private static StreamPipeline deploy(MessageContext synCtx, String key, StreamSink sink) {
        StreamPipeline pipeline = new StreamPipeline();
        pipeline.setName(key);
        pipeline.setSourceProvided(true);
        pipeline.addOperator(sink, true);
        synCtx.getConfiguration().addStreamPipeline(key, pipeline);
        return pipeline;
    }

    /**
     * The defect: two {@code <streamPipeline>} references in one sequence both read the same seed.
     * The first drained the stream; the second found it at end-of-file — still open, because a
     * provided stream is close-shielded so its provider keeps ownership — so it read zero bytes,
     * wrote an empty artifact and reported success.
     *
     * <p>Consuming the property makes the second reference hit the existing, correct error instead.
     */
    @Test
    public void aSecondReferenceIsRefusedInsteadOfSilentlyTransferringNothing() throws Exception {
        MessageContext synCtx = TestUtils.getTestContext("<dummy/>");
        CapturingSink first = new CapturingSink();
        CapturingSink second = new CapturingSink();
        deploy(synCtx, "to-s3", first);
        deploy(synCtx, "to-backup", second);
        synCtx.setProperty(StreamPipelineMediator.SEED_PROPERTY, seed());

        StreamPipelineMediator one = new StreamPipelineMediator();
        one.setKey("to-s3");
        one.mediate(synCtx);

        assertArrayEquals("the first reference gets the bytes",
                "payload".getBytes(), first.bytes);
        assertNull("the seed must not be left for a second reference",
                synCtx.getProperty(StreamPipelineMediator.SEED_PROPERTY));

        StreamPipelineMediator two = new StreamPipelineMediator();
        two.setKey("to-backup");
        try {
            two.mediate(synCtx);
            org.junit.Assert.fail("expected the second reference to be refused");
        } catch (RuntimeException e) {
            org.junit.Assert.assertTrue(e.getMessage() + " / " + e.getCause(),
                    (e.getMessage() + e.getCause()).contains("no stream was supplied"));
        }

        assertNull("the second pipeline must not have written an empty artifact", second.bytes);
    }

    /** A message carrying no seed is left alone rather than having a null written into it. */
    @Test
    public void aMessageWithNoSeedIsUnaffected() throws Exception {
        MessageContext synCtx = TestUtils.getTestContext("<dummy/>");
        deploy(synCtx, "needs-a-seed", new CapturingSink());

        StreamPipelineMediator mediator = new StreamPipelineMediator();
        mediator.setKey("needs-a-seed");
        try {
            mediator.mediate(synCtx);
            org.junit.Assert.fail("a sourceProvided pipeline with no seed must be refused");
        } catch (RuntimeException expected) {
            // the point is only that nothing was written into the property
        }

        assertNull(synCtx.getProperty(StreamPipelineMediator.SEED_PROPERTY));
    }
}
