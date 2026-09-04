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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Owns every handle a run opens, and closes them last-in-first-out.
 * <p>
 * One scope per run, created by the pipeline and reached through
 * {@link StreamContext#resources()}. No operator can own this itself: a transform does not know
 * whether the sink is still reading from it, and {@code try-with-resources} inside an operator is
 * actively wrong, because the resource must outlive the method that created it — the stream is
 * read long after {@code wrap()} returns.
 *
 * <h2>Why LIFO</h2>
 * Resources form a dependency chain in construction order, so unwinding must run in reverse. A
 * decompressing stream registered after the socket it reads from must close <b>before</b> that
 * socket, or its close may attempt a read on a dead transport. Registration order is construction
 * order, so last-in-first-out is reverse-dependency order for free — nothing has to be declared.
 *
 * <h2>Two kinds of resource</h2>
 * {@link #register} is for resources whose close is <b>cleanup</b>: failing to close leaks, and a
 * close error cannot un-transfer bytes already delivered, so it is logged and swallowed.
 * <p>
 * {@link #registerCommitting} is for resources whose close <b>completes the work</b> — a file that
 * must be flushed and renamed into place. If that fails the transfer failed, even though every
 * byte was read successfully, because the destination is not in the state the caller was told it
 * would be. Those failures propagate.
 * <p>
 * The distinction exists because the two cases need opposite error handling. Swallowing a commit
 * failure reports success on a transfer whose destination was never made durable, which is the
 * worst available outcome since nobody retries it.
 *
 * <h2>Not thread-safe</h2>
 * A run is single-threaded by construction: the sink's read loop drives everything. Concurrency
 * in MFT is across transfers, and each has its own scope.
 */
public class ResourceScope implements Closeable {

    private static final Log log = LogFactory.getLog(ResourceScope.class);

    /** Registration order; iterated in reverse on close. */
    private final Deque<Entry> entries = new ArrayDeque<>();

    private boolean closed;

    /**
     * Registers a resource whose close is cleanup, and returns it so registration need not be a
     * separate statement:
     * <pre>
     *   SftpClient c = ctx.resources().register("sftp-client", new SftpClient(cfg));
     * </pre>
     *
     * @param name     short label used in log messages when closing fails
     * @param resource the resource; may be {@code null}, in which case nothing is registered
     * @param <T>      the resource type
     * @return {@code resource}, unchanged
     * @throws IllegalStateException if this scope is already closed
     */
    public <T extends Closeable> T register(String name, T resource) {
        return add(name, resource, false);
    }

    /**
     * Registers a resource whose close <b>publishes</b> work, so that a close failure fails the
     * transfer and a failed run discards it instead.
     *
     * <p>Requires a {@link StreamCommit} rather than a plain {@code Closeable}, because the scope
     * has to be able to say "release this but publish nothing" when the run failed. See that
     * interface for why the distinction cannot be inferred.
     *
     * @param name     short label used in the failure message
     * @param resource the resource; may be {@code null}, in which case nothing is registered
     * @param <T>      the resource type
     * @return {@code resource}, unchanged
     * @throws IllegalStateException if this scope is already closed
     */
    public <T extends StreamCommit> T registerCommitting(String name, T resource) {
        return add(name, resource, true);
    }

    private <T extends Closeable> T add(String name, T resource, boolean committing) {
        if (closed) {
            throw new IllegalStateException(
                    "cannot register '" + name + "': this resource scope is already closed");
        }
        if (resource != null) {
            entries.push(new Entry(name, resource, committing));
        }
        return resource;
    }

    /**
     * Closes one resource now and forgets it, instead of waiting for the scope to close.
     * <p>
     * Exists for the orphaned upstream a resumed run leaves behind: when a run resumes from a
     * materialised artifact, the operators of earlier segments have still been constructed and
     * their streams will never be read. Releasing them early is what stops a long resumed run
     * holding handles it cannot use.
     * <p>
     * <b>A committing resource is aborted, not closed.</b> It is being abandoned, so publishing its
     * work would be wrong — and a failure to discard is logged and swallowed, because nothing was
     * promised.
     *
     * @param resource the resource to close and deregister; {@code null} and unknown resources
     *                 are ignored
     */
    public void closeNow(Closeable resource) {
        if (resource == null) {
            return;
        }
        Entry found = null;
        for (Entry e : entries) {
            if (e.resource == resource) {
                found = e;
                break;
            }
        }
        if (found == null) {
            return;
        }
        entries.remove(found);
        releaseOne(found);
    }

    /** How many resources are registered. A watermark for {@link #closeNowRegisteredBefore}. */
    public int registered() {
        return entries.size();
    }

    /**
     * Releases every resource registered before a watermark, newest of those first.
     *
     * <p>This is what actually frees a resumed run's orphaned upstream, and it exists because
     * {@link #closeNow} could not. The pipeline holds {@code StageStream} decorators, not the streams
     * operators registered here, so matching by identity found nothing and released nothing — while
     * logging that it had. What the pipeline does know is <b>when</b>: everything registered before the
     * stage that resumed belongs to a segment nothing will read.
     *
     * <p>Committing resources among them are aborted rather than closed, for the reason
     * {@link #closeNow} gives: an abandoned upstream must publish nothing.
     *
     * @param mark a value previously returned by {@link #registered()}
     */
    public void closeNowRegisteredBefore(int mark) {
        if (mark <= 0 || entries.isEmpty()) {
            return;
        }
        int keepCount = Math.max(0, entries.size() - mark);
        Deque<Entry> keep = new ArrayDeque<>();
        List<Entry> release = new ArrayList<>();

        int index = 0;
        for (Entry e : entries) {                       // iterates newest first
            if (index < keepCount) {
                keep.addLast(e);                        // preserving newest-first order
            } else {
                release.add(e);
            }
            index++;
        }

        entries.clear();
        for (Entry e : keep) {
            entries.addLast(e);
        }
        for (Entry e : release) {                       // already newest-first
            releaseOne(e);
        }
    }

    private void releaseOne(Entry entry) {
        try {
            if (entry.committing) {
                // Abandoned, not completed — so discard. Closing would publish: this is the same
                // defect abort() exists for, reached by a different path. An orphaned upstream from
                // a resumed run has produced nothing anybody should read.
                ((StreamCommit) entry.resource).abort();
            } else {
                entry.resource.close();
            }
        } catch (Throwable t) {
            log.warn("Failed to close '" + entry.name + "' early; it is being abandoned", t);
        }
    }

    /**
     * Closes every registered resource, last-registered first.
     * <p>
     * <b>Attempts all of them</b>, even if an earlier close threw — abandoning the remainder on
     * first failure would leak precisely the handles most likely to matter. Failures of
     * {@link #register}ed resources are logged and swallowed. Failures of
     * {@link #registerCommitting} resources are collected; if there are any, the first is thrown
     * with the rest attached as suppressed.
     * <p>
     * <b>Caller obligation on the failure path.</b> When the run is already failing, the caller
     * must not let a commit failure replace the original exception — the original is the
     * diagnosis and the commit failure is a consequence of it. Catch and attach:
     * <pre>
     *   try {
     *       scope.close();
     *   } catch (IOException commitFailure) {
     *       primary.addSuppressed(commitFailure);
     *   }
     * </pre>
     * Only the caller knows what the primary exception is, which is why that rule lives there and
     * not here.
     * <p>
     * Idempotent: closing an already-closed scope does nothing. Mutually exclusive with
     * {@link #abort()} — whichever runs first wins, and the other becomes a no-op.
     *
     * <p><b>This is the success path.</b> A committing resource is closed, which publishes its
     * work. A run that failed must call {@link #abort()} instead, or it publishes a partial
     * artifact under a name that means complete.
     *
     * @throws IOException if any committing resource failed to close
     */
    /**
     * Unwinds after a <b>failed or cancelled</b> run: releases every resource and publishes none.
     *
     * <p>Same LIFO order and same all-or-nothing attempt as {@link #close()}, with one difference
     * that is the whole point — a committing resource gets {@link StreamCommit#abort()} instead of
     * {@code close()}, so a partial artifact is never renamed into place.
     *
     * <p><b>Never throws.</b> This runs while a failure is already being reported, and an
     * exception raised here would compete with the diagnosis the caller needs. Failures to discard
     * are logged, including for committing resources: there is nothing left to fail, since nothing
     * was promised.
     */
    public void abort() {
        if (closed) {
            return;
        }
        closed = true;

        while (!entries.isEmpty()) {
            Entry e = entries.pop();
            try {
                if (e.committing) {
                    ((StreamCommit) e.resource).abort();
                } else {
                    e.resource.close();
                }
            } catch (Throwable t) {
                log.warn("Failed to discard '" + e.name + "' while unwinding a failed run;"
                        + " continuing to unwind", t);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;

        List<Throwable> commitFailures = new ArrayList<>();

        while (!entries.isEmpty()) {
            Entry e = entries.pop();
            try {
                e.resource.close();
            } catch (Throwable t) {
                if (e.committing) {
                    commitFailures.add(t);
                } else {
                    log.warn("Failed to close '" + e.name + "'; continuing to unwind", t);
                }
            }
        }

        if (!commitFailures.isEmpty()) {
            Throwable first = commitFailures.get(0);
            IOException failure = new IOException(
                    "the transfer read every byte but " + commitFailures.size()
                            + " committing resource(s) failed to close, so the destination is not in the"
                            + " promised state", first);
            for (int i = 1; i < commitFailures.size(); i++) {
                failure.addSuppressed(commitFailures.get(i));
            }
            throw failure;
        }
    }

    /**
     * How many resources are currently registered. Intended for tests and diagnostics.
     *
     * @return the number of registered resources
     */
    public int size() {
        return entries.size();
    }

    /** Whether this scope has been closed. */
    public boolean isClosed() {
        return closed;
    }

    /** One registration. */
    private static final class Entry {

        private final String name;
        private final Closeable resource;
        private final boolean committing;

        private Entry(String name, Closeable resource, boolean committing) {
            this.name = name;
            this.resource = resource;
            this.committing = committing;
        }
    }
}
