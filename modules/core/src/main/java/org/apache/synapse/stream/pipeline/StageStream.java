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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Wraps one stage of a byte chain to measure and attribute it, <b>without the operator
 * cooperating</b>.
 * <p>
 * The pipeline inserts one of these around whatever stream each operator returns. Operators do not
 * opt in, cannot opt out, and are not consulted — which is the point. Any scheme requiring
 * operators to report their own numbers is a scheme where some operators do not, and a missing
 * metric looks exactly like a stage that was fast.
 *
 * <h2>Self time, and why {@code inner} exists</h2>
 * A {@code read()} at stage N blocks until stage N-1 has produced bytes, so {@link #totalNanos()}
 * is <b>inclusive</b> of the entire chain below. That makes the raw numbers useless on their own:
 * every stage's total is roughly the whole transfer. The {@code inner} back-reference recovers the
 * useful figure by subtraction:
 * <pre>
 *   selfNanos() = totalNanos - inner.totalNanos
 * </pre>
 * What remains is the time this stage spent on its own work rather than waiting upstream.
 * <p>
 * <b>Self time is wall clock, not CPU.</b> A GC pause landing inside one stage's read, a lock
 * wait, or a decorator that buffers and returns instantly on most calls will all skew it. It
 * reliably says <i>which</i> stage to look at and only weakly estimates how much a fix would save.
 * Any surface that displays it should say so.
 *
 * <h2>Fault attribution</h2>
 * An {@code IOException} escaping a read is rethrown as a {@link StageIOException} carrying this
 * stage's name, so by the time a failure surfaces from the sink's pull the stage is already
 * attached and nothing has to reconstruct it from a stack trace.
 * <p>
 * An exception that is <b>already</b> attributed passes through untouched. The innermost stage is
 * the correct attribution; an outer wrapper re-stamping it would name the wrong stage.
 */
public class StageStream extends FilterInputStream {

    private final String stage;

    /** The wrapper immediately below this one, or {@code null} at the head of the chain. */
    private final StageStream inner;

    private long bytes;
    private long totalNanos;

    /**
     * @param in    the stream this stage produced
     * @param stage this stage's name, from {@code StreamOperator.name()}
     * @param inner the wrapper below this one, or {@code null} if this is the head
     */
    public StageStream(InputStream in, String stage, StageStream inner) {
        super(in);
        if (in == null) {
            throw new IllegalArgumentException("stage '" + stage + "' returned a null stream");
        }
        if (stage == null || stage.isEmpty()) {
            throw new IllegalArgumentException("a stage must have a name");
        }
        this.stage = stage;
        this.inner = inner;
    }

    @Override
    public int read() throws IOException {
        long t0 = System.nanoTime();
        try {
            int b = in.read();
            if (b != -1) {
                bytes++;
            }
            return b;
        } catch (IOException e) {
            throw attributed(e);
        } finally {
            totalNanos += System.nanoTime() - t0;
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        long t0 = System.nanoTime();
        try {
            int n = in.read(b, off, len);
            if (n > 0) {
                bytes += n;
            }
            return n;
        } catch (IOException e) {
            throw attributed(e);
        } finally {
            totalNanos += System.nanoTime() - t0;
        }
    }

    /**
     * Timed, but <b>not</b> counted towards {@link #bytes()}: skipped bytes were never produced to
     * a consumer. Timing them still matters, or a stage that resumes by skipping forward would
     * appear to take no time at all.
     */
    @Override
    public long skip(long n) throws IOException {
        long t0 = System.nanoTime();
        try {
            return in.skip(n);
        } catch (IOException e) {
            throw attributed(e);
        } finally {
            totalNanos += System.nanoTime() - t0;
        }
    }

    /**
     * Attributes a failure to this stage, unless it already carries an attribution — in which case
     * the innermost stage wins and this returns it unchanged.
     */
    private IOException attributed(IOException e) {
        if (e instanceof StageIOException) {
            return e;
        }
        return new StageIOException(stage, e);
    }

    /** Bytes this stage produced to its consumer. */
    public long bytes() {
        return bytes;
    }

    /** Bytes this stage consumed from below, or {@code 0} at the head of the chain. */
    public long bytesIn() {
        return inner == null ? 0L : inner.bytes();
    }

    /** Time spent in this stage's reads, <b>inclusive</b> of everything below it. */
    public long totalNanos() {
        return totalNanos;
    }

    /**
     * Time attributable to this stage's own work, excluding upstream wait. Wall clock, not CPU —
     * see the class javadoc.
     */
    public long selfNanos() {
        long below = inner == null ? 0L : inner.totalNanos();
        long self = totalNanos - below;
        // Clock granularity and re-entrant reads can make the subtraction go very slightly
        // negative; reporting a negative duration is worse than reporting zero.
        return self < 0L ? 0L : self;
    }

    /** This stage's name. */
    public String stage() {
        return stage;
    }

    /**
     * An {@code IOException} that knows which stage produced it.
     * <p>
     * Nested here, and constructible only from here, because a stage identity is trustworthy only
     * if the wrapper that was executing assigned it. Operators must not construct one — throw a
     * plain {@code IOException} from a stream and let the enclosing wrapper attribute it.
     * <p>
     * Never surfaces to a caller of {@code execute()}: the pipeline converts it to a
     * {@code StreamException} carrying the same stage.
     */
    public static final class StageIOException extends IOException {

        private static final long serialVersionUID = 1L;

        private final String stage;

        private StageIOException(String stage, IOException cause) {
            super("stage '" + stage + "' failed: " + cause.getMessage(), cause);
            this.stage = stage;
        }

        /** The stage that raised this failure. */
        public String getStage() {
            return stage;
        }
    }
}
