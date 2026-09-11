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
import org.apache.synapse.stream.StageArtifact;
import org.apache.synapse.stream.CheckpointUnit;
import java.io.InputStream;
import java.io.OutputStream;
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
            try {
                // Stands in for a previous run having published this artifact, then takes the real
                // resume path — which is what signals the pipeline and releases the upstream.
                java.nio.file.Files.write(ctx.workspace().resolve("artifact.out"), artifact);
                return ctx.artifact().openComplete();
            } catch (IOException e) {
                throw new IllegalStateException("mock could not stage its artifact", e);
            }
        }
    }

    /** A materialising transform that records the workspace and artifact paths it was handed. */
    static final class CapturingWorkspace implements StreamTransform {

        private final String name;
        java.nio.file.Path seen;
        org.apache.synapse.stream.StageArtifact seenArtifact;
        int seenMaxReprocessed;

        CapturingWorkspace(String name) {
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
            seen = ctx.workspace();
            seenArtifact = ctx.artifact();
            seenMaxReprocessed = ctx.maxReprocessed();
            return new java.io.FilterInputStream(in) { };
        }
    }

    /**
     * A transform that spills to scratch and declares <b>no capability flags at all</b>.
     *
     * <p>That is the point of it: scratch is not durable state, so a pure decorator needing a working
     * file must not have to claim {@code materialises()} — which would also make it a segment
     * boundary — just to get somewhere to write.
     */
    static final class ScratchUsing implements StreamTransform {

        private final String name;
        java.nio.file.Path seenScratch;

        ScratchUsing(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public InputStream wrap(InputStream in, StreamContext ctx) {
            seenScratch = ctx.scratch();
            try {
                java.nio.file.Files.writeString(seenScratch.resolve("run-0"), "a spilled run");
            } catch (IOException e) {
                throw new IllegalStateException("mock could not spill", e);
            }
            return new java.io.FilterInputStream(in) { };
        }
    }

    /**
     * A transform that keeps a durable position <b>and</b> an artifact — the legal mid-chain shape.
     *
     * <p>Both flags, because a checkpointed stage that is not terminal must materialise: its position
     * would otherwise survive a failure that discarded the output of whatever consumes it. See
     * {@link CheckpointOnly} for the shape that rule rejects.
     */
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
        public boolean materialises() {
            return true;
        }

        @Override
        public InputStream wrap(InputStream in, StreamContext ctx) {
            return new java.io.FilterInputStream(in) { };
        }
    }

    /**
     * A transform that keeps a position and no artifact — legal only as the terminal stage, and the
     * counter-example the mid-chain rule exists for.
     */
    static final class CheckpointOnly implements StreamTransform {

        private final String name;

        CheckpointOnly(String name) {
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

    /**
     * A sink that fails, and whose {@code name()} stops working the moment it does.
     *
     * <p>Pins the property that failure attribution reads the <b>configured</b> stage name and never
     * calls {@code op.name()}. Before that fix the name came from the operator, so a connector stage
     * recorded its per-stage row under its configured name and its failure under the connector's class
     * constant — two columns naming one stage differently.
     */
    static final class FailingSinkWithBrokenName implements StreamSink {

        private final String name;
        private boolean poisoned;

        FailingSinkWithBrokenName(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            if (poisoned) {
                throw new IllegalStateException("name() must not be called on the failure path");
            }
            return name;
        }

        @Override
        public void consume(InputStream in, StreamContext ctx) throws StreamException {
            poisoned = true;
            // No stage set, so the pipeline has to attribute it.
            throw new StreamException("sink failed", true);
        }
    }

    /**
     * A sink that keeps a position and no artifact — the remote-authoritative shape, combination 5.
     *
     * <p>Legal precisely because a sink is always terminal, so nothing downstream can be missing
     * anything when its position outlives a failure.
     */
    static final class CheckpointingSink implements StreamSink {

        private final String name;

        CheckpointingSink(String name) {
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
        public void consume(InputStream in, StreamContext ctx) throws StreamException {
            try {
                byte[] buffer = new byte[1024];
                while (in.read(buffer) != -1) {
                    // drained
                }
            } catch (IOException e) {
                throw new StreamException("mock sink failed", e, true);
            }
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
    /** An operator whose name() throws, which must be refused at deployment. */
    static final class ThrowingName implements StreamTransform {

        @Override
        public String name() {
            throw new IllegalStateException("name() reads configuration that is not bound yet");
        }

        @Override
        public InputStream wrap(InputStream in, StreamContext ctx) {
            return new java.io.FilterInputStream(in) {
            };
        }
    }

    /** An operator with a configurable, possibly absent, name. */
    static final class NamedAs implements StreamTransform {

        private final String name;

        NamedAs(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public InputStream wrap(InputStream in, StreamContext ctx) {
            return new java.io.FilterInputStream(in) {
            };
        }
    }

    /**
     * A sink that validates cleanly and then starts throwing from name(), the shape a connector
     * operation resolved per run can take. Its consume() fails, and that failure is what must be
     * reported.
     */
    static final class BreaksAfterValidation implements StreamSink {

        boolean breakName;

        @Override
        public String name() {
            if (breakName) {
                throw new IllegalStateException("parameters are no longer bound");
            }
            return "late-sink";
        }

        @Override
        public void consume(InputStream in, StreamContext ctx) throws IOException {
            throw new IOException("the sink's own failure");
        }
    }

    /**
     * A materialising transform whose artifact writer is a committing resource, so a test can tell
     * whether the run published or discarded. Publishing on a failed run is invariant 5.
     */
    static final class Committing implements StreamTransform {

        private final String name;
        final java.util.concurrent.atomic.AtomicBoolean committed =
                new java.util.concurrent.atomic.AtomicBoolean();
        final java.util.concurrent.atomic.AtomicBoolean aborted =
                new java.util.concurrent.atomic.AtomicBoolean();

        Committing(String name) {
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
            ctx.resources().registerCommitting(name, new org.apache.synapse.stream.StreamCommit() {
                @Override
                public void close() {
                    committed.set(true);
                }

                @Override
                public void abort() {
                    aborted.set(true);
                }
            });
            return new java.io.FilterInputStream(in) {
            };
        }
    }

    /** A transform whose stream throws an Error on read — the path no catch clause enumerated. */
    static final class ErroringTransform implements StreamTransform {

        private final String name;

        ErroringTransform(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public InputStream wrap(InputStream in, StreamContext ctx) {
            return new java.io.FilterInputStream(in) {
                @Override
                public int read(byte[] b, int off, int len) {
                    throw new StackOverflowError("a mediation sequence recursed too deeply");
                }

                @Override
                public int read() {
                    throw new StackOverflowError("a mediation sequence recursed too deeply");
                }
            };
        }
    }

    /** A source that registers a closeable so a test can assert it was released early. */
    static final class RegisteringSource implements StreamSource {

        private final String name;
        private final byte[] data;
        final java.util.concurrent.atomic.AtomicBoolean closed =
                new java.util.concurrent.atomic.AtomicBoolean();

        RegisteringSource(String name, byte[] data) {
            this.name = name;
            this.data = data;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public InputStream open(StreamContext ctx) {
            return ctx.resources().register(name, new java.io.ByteArrayInputStream(data) {
                @Override
                public void close() {
                    closed.set(true);
                }
            });
        }
    }

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

    /** Materialises and is deterministic — a csv→json style converter. Legal above a checkpoint. */
    static final class DeterministicMaterialising implements StreamTransform {
        private final String name;

        DeterministicMaterialising(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean deterministic() {
            return true;
        }

        @Override
        public boolean materialises() {
            return true;
        }

        @Override
        public InputStream wrap(InputStream upstream, StreamContext ctx) {
            return new FilterInputStream(upstream) { };
        }
    }

    /** A sink that declares materialises() — an artifact nothing could ever read. */
    static final class MaterialisingSink implements StreamSink {
        @Override
        public String name() {
            return "materialising-sink";
        }

        @Override
        public boolean materialises() {
            return true;
        }

        @Override
        public void consume(InputStream in, StreamContext ctx) throws IOException {
            in.transferTo(OutputStream.nullOutputStream());
        }
    }

    /**
     * A faithful miniature of a materialising, checkpointed transform: one byte in, one byte out, one
     * record. Appends every record to its artifact, calls {@code unitDone} after the append, and can be
     * told to fail partway so a resume can be exercised.
     *
     * <p>Deliberately does <b>not</b> replay its own artifact prefix — that is the framework's job, and
     * the point of the test is that no operator has to.
     */
    static final class ByteRecords implements StreamTransform {

        private final String name;
        private final int failAfter;
        private final AtomicInteger emitted = new AtomicInteger();

        ByteRecords(String name, int failAfter) {
            this.name = name;
            this.failAfter = failAfter;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean deterministic() {
            return true;
        }

        @Override
        public boolean checkpointed() {
            return true;
        }

        @Override
        public boolean materialises() {
            return true;
        }

        @Override
        public InputStream wrap(InputStream in, StreamContext ctx) {
            StageArtifact artifact = ctx.artifact(CheckpointUnit.RECORDS, "bytes-1");
            return new InputStream() {
                private long record;
                private boolean started;

                @Override
                public int read() throws IOException {
                    if (!started) {
                        started = true;
                        record = artifact.resumePosition();
                        in.skipNBytes(record);          // one byte per record
                    }
                    if (failAfter > 0 && emitted.get() >= failAfter) {
                        throw new IOException(name + " failed after " + failAfter + " records");
                    }
                    int b = in.read();
                    if (b == -1) {
                        return -1;
                    }
                    record++;
                    artifact.append(b);                 // 1. staged
                    artifact.unitDone(record);          // 2. fsync, then advance
                    emitted.incrementAndGet();
                    return b;
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    if (len == 0) {
                        return 0;
                    }
                    int one = read();
                    if (one == -1) {
                        return -1;
                    }
                    b[off] = (byte) one;
                    return 1;                           // one record per read, so the seam is visible
                }

                @Override
                public void close() throws IOException {
                    in.close();
                }
            };
        }
    }

    /** Refuses during wrap(), as a connector operation does when its configuration is unusable. */
    static final class FailsWhenBuilt implements StreamTransform {

        private final String name;

        FailsWhenBuilt(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public InputStream wrap(InputStream upstream, StreamContext ctx) throws StreamException {
            throw new StreamException(name + " needs the invoking message to read its parameters"
                    + " from", false);
        }
    }
}
