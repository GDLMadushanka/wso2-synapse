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

package org.apache.synapse.stream.pipeline;

import org.apache.synapse.stream.StreamContext;
import org.apache.synapse.stream.StreamException;
import org.apache.synapse.stream.StreamSink;
import org.apache.synapse.stream.StreamSource;
import org.apache.synapse.stream.StreamTransform;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mock operators for pipeline tests, instrumented to prove the build loop moved no bytes.
 * <p>
 * The counters here are the closest thing to a mechanical enforcement of the rule that
 * {@code open()} and {@code wrap()} perform no I/O. That rule is carried by javadoc everywhere
 * else, so a harness that can assert it is worth more than several tests that assume it.
 */
final class Mocks {

    private Mocks() {
    }

    /** Records reads so a test can assert none happened during the build. */
    static final class CountingStream extends FilterInputStream {

        final AtomicInteger reads = new AtomicInteger();

        CountingStream(byte[] data) {
            super(new ByteArrayInputStream(data));
        }

        @Override
        public int read() throws IOException {
            reads.incrementAndGet();
            return in.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            reads.incrementAndGet();
            return in.read(b, off, len);
        }
    }

    /** A source that opens lazily and records whether anything touched it during the build. */
    static final class Source implements StreamSource {

        private final String name;
        private final byte[] data;
        final AtomicInteger opens = new AtomicInteger();
        CountingStream opened;

        Source(String name, byte[] data) {
            this.name = name;
            this.data = data;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public InputStream open(StreamContext ctx) {
            opens.incrementAndGet();
            opened = new CountingStream(data);
            return ctx.resources().register(name, opened);
        }
    }

    /**
     * A materialising transform that finds its artifact already there and returns a stream over it,
     * ignoring its upstream — the resume shape ADR-0013 describes.
     */
    static final class ResumesFromArtifact implements StreamTransform {

        private final String name;
        private final byte[] artifact;

        ResumesFromArtifact(String name, byte[] artifact) {
            this.name = name;
            this.artifact = artifact;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean materialises() {
            return true;
        }

        @Override
        public InputStream wrap(InputStream in, StreamContext ctx) {
            ctx.resumedFromArtifact();
            return new java.io.ByteArrayInputStream(artifact);
        }
    }

    /** A transform that declares it keeps a durable position, for the validation rules. */
    static final class Checkpointing implements StreamTransform {

        private final String name;

        Checkpointing(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean checkpointed() {
            return true;
        }

        @Override
        public InputStream wrap(InputStream in, StreamContext ctx) {
            return new java.io.FilterInputStream(in) { };
        }
    }

    /** A source whose bytes exist once — an API multipart body or a socket. */
    static final class OneShotSource implements StreamSource {

        private final String name;
        private final byte[] data;

        OneShotSource(String name, byte[] data) {
            this.name = name;
            this.data = data;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public org.apache.synapse.stream.StreamOrigin origin() {
            return org.apache.synapse.stream.StreamOrigin.ONE_SHOT;
        }

        @Override
        public InputStream open(StreamContext ctx) {
            return ctx.resources().register(name, new CountingStream(data));
        }
    }

    /** A pass-through transform that must not read during {@code wrap()}. */
    static final class PassThrough implements StreamTransform {

        private final String name;
        final AtomicInteger wraps = new AtomicInteger();

        PassThrough(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public InputStream wrap(InputStream in, StreamContext ctx) {
            wraps.incrementAndGet();
            return ctx.resources().register(name, new FilterInputStream(in) {
            });
        }
    }

    /** A sink that drains, and records what it saw. */
    static final class Sink implements StreamSink {

        private final String name;
        final List<Byte> received = new ArrayList<>();
        final AtomicInteger consumes = new AtomicInteger();
        private final boolean drain;

        Sink(String name) {
            this(name, true);
        }

        Sink(String name, boolean drain) {
            this.name = name;
            this.drain = drain;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public void consume(InputStream in, StreamContext ctx) throws IOException {
            consumes.incrementAndGet();
            if (!drain) {
                return;
            }
            int b;
            while ((b = in.read()) != -1) {
                received.add((byte) b);
            }
        }

        byte[] bytes() {
            byte[] out = new byte[received.size()];
            for (int i = 0; i < out.length; i++) {
                out[i] = received.get(i);
            }
            return out;
        }
    }

    /** A transform whose stream fails on read, to test fault attribution. */
    static final class FailingTransform implements StreamTransform {

        private final String name;

        FailingTransform(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public InputStream wrap(InputStream in, StreamContext ctx) {
            return new FilterInputStream(in) {
                @Override
                public int read() throws IOException {
                    throw new IOException("boom in " + name);
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    throw new IOException("boom in " + name);
                }
            };
        }
    }

    /** A transform that adds a fixed delay per read, so self time is measurable. */
    static final class SlowTransform implements StreamTransform {

        private final String name;
        private final long millisPerRead;

        SlowTransform(String name, long millisPerRead) {
            this.name = name;
            this.millisPerRead = millisPerRead;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public InputStream wrap(InputStream in, StreamContext ctx) {
            return new FilterInputStream(in) {
                @Override
                public int read() throws IOException {
                    try {
                        Thread.sleep(millisPerRead);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return in.read();
                }
            };
        }
    }

    /** Declares a role but also a second one, to test role-exclusivity rejection. */
    static final class MultiRole implements StreamSource, StreamTransform {

        @Override
        public String name() {
            return "multi";
        }

        @Override
        public InputStream open(StreamContext ctx) {
            return new ByteArrayInputStream(new byte[0]);
        }

        @Override
        public InputStream wrap(InputStream in, StreamContext ctx) {
            return in;
        }
    }

    /** An operator implementing no role interface at all. */
    static final class NoRole implements org.apache.synapse.stream.StreamOperator {

        @Override
        public String name() {
            return "noRole";
        }
    }

    /** A transform that illegally returns its own upstream. */
    static final class IdentityTransform implements StreamTransform {

        @Override
        public String name() {
            return "identity";
        }

        @Override
        public InputStream wrap(InputStream in, StreamContext ctx) {
            return in;
        }
    }

    /** A transform declaring it materialises, used to test the not-yet-supported rejection. */
    static final class Materialising implements StreamTransform {

        private final String name;

        Materialising(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean materialises() {
            return true;
        }

        @Override
        public InputStream wrap(InputStream in, StreamContext ctx) throws StreamException {
            return in;
        }
    }

    /**
     * A lazily materialising transform: it "writes" each byte as it is pulled and also passes it
     * downstream. Lazy on purpose — an eagerly materialising operator would make the terminal case
     * cost a second full pass over its own artifact.
     */
    static final class LazyMaterialising implements StreamTransform {

        private final String name;
        final java.io.ByteArrayOutputStream written = new java.io.ByteArrayOutputStream();

        LazyMaterialising(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean materialises() {
            return true;
        }

        @Override
        public InputStream wrap(InputStream in, StreamContext ctx) {
            return new FilterInputStream(in) {
                @Override
                public int read() throws IOException {
                    int b = in.read();
                    if (b != -1) {
                        written.write(b);
                    }
                    return b;
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    int n = in.read(b, off, len);
                    if (n > 0) {
                        written.write(b, off, n);
                    }
                    return n;
                }
            };
        }
    }
}
