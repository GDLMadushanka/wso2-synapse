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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.stream.Checkpoint;
import org.apache.synapse.stream.CheckpointStore;
import org.apache.synapse.stream.CheckpointUnit;
import org.apache.synapse.stream.ResourceScope;
import org.apache.synapse.stream.StageArtifact;
import org.apache.synapse.stream.StreamCommit;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/**
 * The framework's side of {@link StageArtifact}: temp-write, {@code fsync}, rename on success,
 * discard on failure, and the checkpoint ordering contract fused into one call.
 * <p>
 * Every operator that materialises or checkpoints goes through this, so the six obligations that
 * used to sit with operator authors are discharged in one place that is reviewed once.
 *
 * <h2>Publishing is the run's decision</h2>
 * The output is registered with the run's {@link ResourceScope} as a {@link StreamCommit}, so
 * {@code close()} renames the partial into place and {@code abort()} deletes it. There is no method
 * an operator can call to publish, which is what makes invariant 5 structural rather than advisory.
 *
 * <h2>Nothing is created until something is written</h2>
 * A stage that declares {@code checkpointed()} without {@code materialises()} calls
 * {@link #unitDone} and never {@link #append}, so no file is opened and none is registered. Its
 * checkpoint simply records an artifact length of zero.
 *
 * <h2>Thread safety</h2>
 * One instance per stage per run, used by the single thread pulling the chain. Not shared, and not
 * synchronised.
 */
public class DefaultStageArtifact implements StageArtifact {

    private static final Log log = LogFactory.getLog(DefaultStageArtifact.class);

    /** Suffix of the file being written. A partial artifact must never be visible under its final name. */
    static final String PARTIAL_SUFFIX = ".part";

    private final String stageName;
    private final Path finalPath;
    private final Path partialPath;

    /** Null when this artifact was not obtained for checkpointing. */
    private final CheckpointStore store;
    private final CheckpointUnit unit;
    private final String formatVersion;

    private final int maxReprocessed;
    private final ResourceScope resources;

    /** Signals the owning context that this stage short-circuited; the pipeline reads it after the build. */
    private final Runnable resumeSignal;

    private boolean started;
    private FileOutputStream fileOut;
    private BufferedOutputStream out;

    /** Bytes in the artifact, durable or buffered, including anything a previous attempt left. */
    private long appendedLength;

    /** Input position this attempt began at, in {@link #unit}. */
    private long resumeInputPosition;

    /** Operator-owned state from the checkpoint this attempt resumed from; null if there was none. */
    private String resumePayload;

    /** Input position of the last checkpoint written or resumed from. */
    private long lastCheckpointPosition;

    /** Monotonicity guard on {@link #unitDone}. */
    private long lastReportedPosition = -1L;

    /** So a coarse reporter is told once, not once per unit. */
    private boolean granularityWarned;

    /**
     * Whether {@link #seal()} is allowed. False for the canonical artifact, whose publish decision
     * belongs to the run's outcome and nobody else — see invariant 5.
     */
    private final boolean sealable;

    /** Set by {@link #seal()}: the file is published, so run-end commit and abort must both stand off. */
    private boolean sealed;

    public DefaultStageArtifact(String stageName, Path stageDir, String artifactName,
                                CheckpointStore store, CheckpointUnit unit, String formatVersion,
                                int maxReprocessed, ResourceScope resources, Runnable resumeSignal,
                                boolean sealable) {
        this.stageName = stageName;
        // Null when no workspace is configured, which is legal for a stage that keeps a position and
        // writes nothing. Appending then fails with a message naming the cause; checkpointing does not.
        this.finalPath = stageDir == null ? null : stageDir.resolve(artifactName);
        this.partialPath = stageDir == null ? null : stageDir.resolve(artifactName + PARTIAL_SUFFIX);
        this.store = store;
        this.unit = unit;
        this.formatVersion = formatVersion;
        this.maxReprocessed = Math.max(1, maxReprocessed);
        this.resources = resources;
        this.resumeSignal = resumeSignal;
        this.sealable = sealable;
    }

    // ---------------------------------------------------------------- resume

    @Override
    public boolean isComplete() throws IOException {
        // Deliberately does not trigger start(): a complete artifact makes the partial irrelevant,
        // and start() would delete it before the caller had decided anything.
        return finalPath != null && Files.exists(finalPath);
    }

    @Override
    public InputStream openComplete() throws IOException {
        if (!isComplete()) {
            throw new IllegalStateException("stage '" + stageName + "' has no completed artifact at '"
                    + finalPath + "'; call isComplete() before openComplete()");
        }
        InputStream in = resources.register(stageName + ":artifact", Files.newInputStream(finalPath));
        // Signalled here rather than left to the operator. Forgetting it holds every upstream handle
        // — a remote connection, typically — open and unread for the length of the run, with nothing
        // to indicate it happened.
        resumeSignal.run();
        return in;
    }

    @Override
    public long resumePosition() throws IOException {
        start();
        return resumeInputPosition;
    }

    @Override
    public String resumePayload() throws IOException {
        start();
        return resumePayload;
    }

    @Override
    public long length() throws IOException {
        start();
        return appendedLength;
    }

    /**
     * Reconciles the stored position against the artifact on disk, once, before anything is written.
     *
     * <p>Takes the checkpoint only when its recorded length {@code L} is no greater than the partial
     * file's actual length {@code A}. A checkpoint ahead of the data describes bytes that are not
     * there; resuming on it would leave a hole no later check could find, so it is discarded and the
     * stage restarts. The mandatory {@code fsync} in {@link #unitDone} means this should be
     * unreachable — it is a net under storage that acknowledges writes it has not committed.
     */
    private void start() throws IOException {
        if (started) {
            return;
        }
        started = true;

        long resumeLength = 0L;
        if (store != null) {
            Checkpoint last = store.lastComplete();
            if (last != null) {
                long actual = partialPath != null && Files.exists(partialPath)
                        ? Files.size(partialPath) : 0L;
                if (last.outputLength() <= actual) {
                    resumeInputPosition = last.position();
                    resumeLength = last.outputLength();
                    resumePayload = last.payload();
                } else {
                    log.warn("stage '" + stageName + "' has a checkpoint recording " + last.outputLength()
                            + " artifact bytes but its partial file holds only " + actual
                            + "; the checkpoint is ahead of the data, so it is discarded and this stage"
                            + " restarts from the beginning of its segment");
                }
            }
        }

        if (resumeLength > 0L) {
            // Cut anything written after the checkpoint. Those bytes were never recorded, so whatever
            // produced them is about to produce them again.
            try (FileChannel channel = FileChannel.open(partialPath, StandardOpenOption.WRITE)) {
                channel.truncate(resumeLength);
            }
            log.info("stage '" + stageName + "' resumed at position " + resumeInputPosition
                    + (unit == null ? "" : " " + unit) + ", artifact truncated to " + resumeLength
                    + " byte(s)");
        } else if (partialPath != null && Files.exists(partialPath)) {
            // No usable position, so the bytes below one cannot be trusted either.
            Files.delete(partialPath);
        }

        appendedLength = resumeLength;
        lastCheckpointPosition = resumeInputPosition;
    }

    // ---------------------------------------------------------------- writing

    @Override
    public void append(int b) throws IOException {
        ensureOpen();
        out.write(b);
        appendedLength++;
    }

    @Override
    public void append(byte[] b, int off, int len) throws IOException {
        ensureOpen();
        out.write(b, off, len);
        appendedLength += len;
    }

    private void ensureOpen() throws IOException {
        if (sealed) {
            throw new IllegalStateException("stage '" + stageName + "' appended to artifact '"
                    + finalPath + "' after sealing it. A sealed file is published and may already have"
                    + " been read back; appending would change what a reader saw");
        }
        start();
        if (out != null) {
            return;
        }
        if (partialPath == null) {
            throw new IllegalStateException("stage '" + stageName + "' tried to write an artifact but"
                    + " has no workspace directory. An operator that appends must declare"
                    + " materialises(), and its pipeline needs '"
                    + org.apache.synapse.SynapseConstants.STREAM_WORKSPACE_ROOT
                    + "' set in synapse.properties");
        }
        // Opened on first write, not in start(), so a checkpoint-only stage never creates a file.
        // Appending, because start() has already truncated to whatever is safe to keep.
        fileOut = new FileOutputStream(partialPath.toFile(), true);
        out = new BufferedOutputStream(fileOut);
        resources.registerCommitting(stageName + ":artifact", new RenameOnCommit());
    }

    // ---------------------------------------------------------------- the ordering contract

    @Override
    public void unitDone(long position) throws IOException {
        unitDone(position, null);
    }

    @Override
    public void unitDone(long position, String payload) throws IOException {
        if (store == null || unit == null) {
            throw new IllegalStateException("stage '" + stageName + "' called unitDone() on an artifact"
                    + " obtained without a checkpoint unit and format version; obtain it with"
                    + " ctx.artifact(unit, formatVersion), and declare checkpointed() on the operator");
        }
        if (position < 0L) {
            throw new IllegalArgumentException("stage '" + stageName + "' reported a negative position: "
                    + position);
        }
        if (position < lastReportedPosition) {
            throw new IllegalArgumentException("stage '" + stageName + "' reported position " + position
                    + " after " + lastReportedPosition + "; a checkpoint position is cumulative and must"
                    + " not move backwards");
        }
        start();
        lastReportedPosition = position;

        long advanced = position - lastCheckpointPosition;
        if (advanced < maxReprocessed) {
            // Not due. The bytes appended since the last checkpoint are deliberately left un-synced:
            // resume truncates them away, so paying for them would buy nothing. This is the whole of
            // why maxReprocessed is the only lever on throughput here.
            return;
        }

        if (advanced > maxReprocessed && !granularityWarned) {
            granularityWarned = true;
            log.warn("stage '" + stageName + "' reports progress every " + advanced + " " + unit
                    + " but maxReprocessed is " + maxReprocessed + ", so a failure may reprocess more"
                    + " than the configured bound. The operator's own reporting granularity is the"
                    + " floor; lower maxReprocessed no further, or report more finely");
        }

        // 2. durable before the position moves. flush() alone reaches the page cache, and a file that
        //    is the right length with unwritten gaps below it passes every length check there is.
        if (out != null) {
            out.flush();
            fileOut.getFD().sync();
        }
        // 3. and only then.
        store.append(new Checkpoint(position, appendedLength, unit, formatVersion, payload));
        lastCheckpointPosition = position;
    }

    @Override
    public void seal() throws IOException {
        if (!sealable) {
            throw new IllegalStateException("stage '" + stageName + "' tried to seal its canonical"
                    + " artifact. Whether that is published is the run's outcome to decide, not the"
                    + " operator's: publishing a partial artifact would let the next attempt read this"
                    + " segment as finished. Only a named working file may be sealed");
        }
        if (sealed) {
            return;
        }
        // An empty working file is a legitimate answer -- zero runs matched a predicate, say -- so
        // this creates one rather than leaving a reader unable to tell empty from absent.
        ensureOpen();
        out.flush();
        fileOut.getFD().sync();
        out.close();
        Files.move(partialPath, finalPath, StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
        sealed = true;
    }

    // ---------------------------------------------------------------- publishing

    /**
     * Renames the partial artifact into place on a successful run, and deletes it on a failed one.
     *
     * <p>Which of the two runs is the run's outcome, decided by {@link ResourceScope}. That is
     * invariant 5: publishing on failure renames a truncated artifact into place, and the next attempt
     * reads its presence as a finished segment — a complete-looking, wrong result that surfaces
     * nowhere near its cause.
     */
    private final class RenameOnCommit implements StreamCommit {

        @Override
        public void close() throws IOException {
            if (sealed) {
                return;                             // already published, by the operator's own call
            }
            out.flush();
            fileOut.getFD().sync();
            out.close();
            Files.move(partialPath, finalPath, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        }

        @Override
        public void abort() throws IOException {
            if (sealed) {
                // Deliberately kept. A sealed working file surviving a failed run is how the next
                // attempt resumes instead of redoing the work that produced it.
                return;
            }
            // Must not throw merely because the write was partial: this runs while a failure is
            // already being reported, and an exception here competes with the diagnosis.
            try {
                out.close();
            } catch (IOException e) {
                log.warn("stage '" + stageName + "' failed to close its partial artifact while"
                        + " aborting; continuing to delete it", e);
            }
            Files.deleteIfExists(partialPath);
        }
    }
}
