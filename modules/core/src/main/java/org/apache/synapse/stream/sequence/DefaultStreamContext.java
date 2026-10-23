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

package org.apache.synapse.stream.sequence;

import org.apache.synapse.MessageContext;
import org.apache.synapse.stream.spi.CheckpointStore;
import org.apache.synapse.stream.JobContext;
import org.apache.synapse.stream.ResourceScope;
import org.apache.synapse.stream.StageArtifact;
import org.apache.synapse.stream.StreamContext;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * What one operator gets for one invocation.
 * <p>
 * Immutable, and one instance per operator per run — {@code stageName} differs between operators,
 * so they cannot share one. That is a deliberate change from an earlier design in which the
 * sequence mutated a shared context as the chain was built: a shared mutable context is a race
 * waiting for the first operator that hands its context to another thread.
 *
 * <h2>Capability gating</h2>
 * {@link #workspace()} and the {@code artifact(...)} accessors are legal only for an operator that
 * declared the matching flag. The sequence passes that entitlement in at construction, and an
 * unentitled call throws {@link IllegalStateException} naming the flag that is missing.
 */
public class DefaultStreamContext implements StreamContext {

    private static final org.apache.commons.logging.Log log =
            org.apache.commons.logging.LogFactory.getLog(DefaultStreamContext.class);

    /**
     * The one artifact name restart looks for. Framework-defined rather than operator-chosen: both
     * the writer and the run that later resumes from it read this constant, so they cannot diverge.
     */
    static final String CANONICAL_ARTIFACT = "artifact.out";

    /** This stage's artifacts, by file name. One instance per name per run — see artifactFor. */
    private final Map<String, Held> artifacts = new HashMap<>();

    private final MessageContext message;
    private final JobContext job;
    private final ResourceScope resources;
    private final String stageName;

    private final boolean mayUseWorkspace;
    private final boolean mayCheckpoint;

    /** This stage's directory, or null when no workspace is configured. */
    private final Path workspace;

    /** Backs the checkpointing artifact; null when this operator does not checkpoint. */
    private final CheckpointStore checkpointStore;

    /**
     * Where this stage's scratch directory should go, or null to fall back to a JVM temporary
     * directory. Computed by the sequence, which is the only thing that knows the roots; not created
     * until {@link #scratch()} is called, so a stage that never asks costs nothing.
     */
    private final Path scratchTarget;

    /** Created and registered on first {@link #scratch()}; null until then. */
    private Path scratchDir;

    /**
     * @param message         the invoking message context
     * @param job             the job context, already defaulted to {@code NOOP} by the sequence
     * @param resources       the run's resource scope
     * @param stageName       this operator's name
     * @param mayUseWorkspace whether this operator declared {@code materialises()} or
     *                        {@code checkpointed()}
     * @param mayCheckpoint   whether this operator declared {@code checkpointed()}
     * @param workspace       this stage's workspace directory, or {@code null} if unsupported
     * @param checkpointStore this stage's checkpoint store, or {@code null} if unsupported
     */
    public DefaultStreamContext(MessageContext message, JobContext job, ResourceScope resources,
                                String stageName, boolean mayUseWorkspace, boolean mayCheckpoint,
                                Path workspace, CheckpointStore checkpointStore,
                                Path scratchTarget) {
        this(message, job, resources, stageName, mayUseWorkspace, mayCheckpoint, workspace,
                checkpointStore, scratchTarget, 0);
    }

    /**
     * As above, carrying this stage's position in the chain.
     *
     * <p>Needed only by {@link #status(String)}, so the row it writes can be ordered before the stage
     * has finished. Defaulted to zero by the constructor above, which tests and programmatically
     * assembled sequences use.
     */
    public DefaultStreamContext(MessageContext message, JobContext job, ResourceScope resources,
                                String stageName, boolean mayUseWorkspace, boolean mayCheckpoint,
                                Path workspace, CheckpointStore checkpointStore,
                                Path scratchTarget, int stageIndex) {
        this.stageIndex = stageIndex;
        this.message = message;
        this.job = job == null ? JobContext.NOOP : job;
        this.resources = resources;
        this.stageName = stageName;
        this.mayUseWorkspace = mayUseWorkspace;
        this.mayCheckpoint = mayCheckpoint;
        this.workspace = workspace;
        this.checkpointStore = checkpointStore;
        this.scratchTarget = scratchTarget;
    }

    @Override
    public long upstreamStartsAt() {
        return upstreamStartsAt;
    }

    /** Sequence-only. See {@link StreamContext#upstreamStartsAt()} for when this may be non-zero. */
    void upstreamStartsAt(long offset) {
        this.upstreamStartsAt = offset;
    }


    @Override
    public MessageContext message() {
        return message;
    }

    @Override
    public JobContext job() {
        return job;
    }

    @Override
    public ResourceScope resources() {
        return resources;
    }

    @Override
    public String stageName() {
        return stageName;
    }

    /** Set by {@link #resumedFromArtifact}; read by the sequence straight after the build call. */
    private boolean resumed;


    /**
     * Byte offset at which this stage's upstream begins. Set by the sequence on a resumed run whose
     * source could seek; zero otherwise. Mutable because it is decided after the context exists — the
     * sink has to be asked where it will resume before the source can be told where to start.
     */
    private volatile long upstreamStartsAt;

    /**
     * Framework-internal. Signalled by {@link DefaultStageArtifact#openComplete()}, never by an
     * operator: it used to be on {@link StreamContext}, and left there it was the one remaining way
     * to orphan an upstream you still needed and then read from a released handle.
     */
    void resumedFromArtifact() {
        this.resumed = true;
    }

    /** Whether this stage said it short-circuited to an existing artifact. */
    public boolean hasResumedFromArtifact() {
        return resumed;
    }

    @Override
    public List<StageArtifact> artifacts(String glob) throws IOException {
        if (!mayUseWorkspace) {
            throw new IllegalStateException("operator '" + stageName + "' asked for its working files"
                    + " but declared neither materialises() nor checkpointed(); declare one, or do not"
                    + " keep durable state");
        }
        if (glob == null || glob.isBlank() || glob.indexOf('/') >= 0 || glob.indexOf('\\') >= 0
                || glob.indexOf('\0') >= 0 || glob.contains("..")) {
            throw new IllegalArgumentException("operator '" + stageName + "' asked for working files"
                    + " matching '" + glob + "', which is not a file-name pattern within its stage"
                    + " directory");
        }
        if (workspace == null || !Files.isDirectory(workspace)) {
            return List.of();           // nothing has been written, so there is nothing to list
        }
        List<StageArtifact> found = new ArrayList<>();
        try (DirectoryStream<Path> listing = Files.newDirectoryStream(workspace, glob)) {
            for (Path entry : listing) {
                String name = entry.getFileName().toString();
                // The canonical artifact is the stage's output and the run decides whether it is
                // published; a .part is a write in progress. Neither is the operator's to enumerate,
                // whatever the glob happens to match.
                if (CANONICAL_ARTIFACT.equals(name)
                        || name.endsWith(DefaultStageArtifact.PARTIAL_SUFFIX)
                        || !Files.isRegularFile(entry)) {
                    continue;
                }
                found.add(artifactFor(name, null, null, 1));
            }
        } catch (DirectoryIteratorException unreadable) {
            throw new IOException("stage '" + stageName + "' could not list its working files",
                    unreadable.getCause());
        }
        return found;
    }

    /**
     * This stage's directory. <b>Deliberately not on {@link StreamContext}</b>: a {@code Path} is an
     * unrestricted handle to the filesystem, and the workspace is a cluster-mounted share, so handing
     * one to connector code — which anyone may write — puts every other run's artifacts, on every
     * node, one {@code resolve("..")} away. Operators reach their files through {@link StageArtifact}
     * and nothing else. See ADR-0064.
     */
    Path workspaceDirectory() {
        if (workspace == null) {
            return null;
        }
        try {
            Files.createDirectories(workspace);
        } catch (IOException e) {
            throw new UncheckedIOException("stage '" + stageName + "' could not create its workspace"
                    + " directory '" + workspace + "'", e);
        }
        return workspace;
    }

    @Override
    public Path scratch() {
        if (scratchDir != null) {
            return scratchDir;
        }
        try {
            if (scratchTarget != null) {
                Files.createDirectories(scratchTarget);
                scratchDir = scratchTarget;
            } else {
                // No workspace and no configured root. Scratch is not durable state, so a JVM
                // temporary directory is a correct home rather than a failure — but it is worth
                // saying, because a large spill landing on the root filesystem is an operational
                // surprise and mft.scratch.root is the fix.
                scratchDir = Files.createTempDirectory("mft-scratch-");
                log.warn("stage '" + stageName + "' asked for scratch space but neither '"
                        + org.apache.synapse.SynapseConstants.STREAM_SCRATCH_ROOT + "' nor '"
                        + org.apache.synapse.SynapseConstants.STREAM_WORKSPACE_ROOT + "' is"
                        + " configured, so it is using '" + scratchDir + "'. Set the former if this"
                        + " stage spills anything large");
            }
        } catch (IOException e) {
            throw new UncheckedIOException("stage '" + stageName + "' could not create its scratch"
                    + " directory", e);
        }
        // Registered rather than deleted by the operator: cleanup then happens on every exit path,
        // including a cancelled run, and cannot be forgotten. Not a committing resource — there is
        // nothing to publish, and both close() and abort() must delete.
        resources.register(stageName + ":scratch", new DeleteOnRelease(scratchDir));
        return scratchDir;
    }

    /** Removes a scratch tree when the run's scope unwinds, whichever way it unwinds. */
    private final class DeleteOnRelease implements Closeable {

        private final Path dir;

        private DeleteOnRelease(Path dir) {
            this.dir = dir;
        }

        @Override
        public void close() {
            if (!Files.exists(dir)) {
                return;
            }
            try (Stream<Path> tree = Files.walk(dir)) {
                tree.sorted(Comparator.reverseOrder()).forEach(entry -> {
                    try {
                        Files.delete(entry);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            } catch (IOException | UncheckedIOException e) {
                // Never fails the run. The bytes are already delivered or already lost; a directory
                // that would not go away is an operational annoyance, not a transfer outcome.
                log.warn("stage '" + stageName + "' could not remove its scratch directory at '" + dir
                        + "'; the transfer is unaffected but the directory is left behind", e);
            }
        }
    }

    // ------------------------------------------------------------------ activity

    /**
     * Minimum gap between database writes for one stage.
     *
     * <p>Three seconds is chosen against a human refreshing a console, not against the operator: an
     * operator may call {@code status} four thousand times and the database still sees one write per
     * three seconds per stage. That independence is the point — see ADR-0069 on why "only report when
     * there is news" bounds redundant updates but not total ones.
     */
    private static final long STATUS_FLUSH_INTERVAL_MILLIS = 3_000L;

    /**
     * The transfer narrative, in its own file.
     *
     * <p>A named logger rather than this class's own, because {@code log4j2.properties} binds it to
     * {@code mft-transfer.log} with {@code additivity = false} — the pattern {@code AUDIT_LOG} and
     * {@code correlation} already use. Four thousand lines from one sort interleaved with framework
     * logging would make both unreadable.
     */
    private static final org.apache.commons.logging.Log TRANSFER_LOG =
            org.apache.commons.logging.LogFactory.getLog("MFT_TRANSFER");

    /** This stage's position in the chain, so a status row can be ordered before the stage ends. */
    private final int stageIndex;

    /** The latest thing this stage said, or null if it has said nothing. */
    private volatile String activity;

    /** The value last written to the store, so the final flush can skip a redundant write. */
    private volatile String activityPushed;

    /** When the store was last written for this stage. */
    private volatile long activityPushedAt;

    @Override
    public void status(String activity) {
        if (activity == null || activity.isBlank()) {
            return;
        }
        String text = activity.trim();
        this.activity = text;

        // Every call, unthrottled: this file IS the history, and a history with gaps is not one.
        if (TRANSFER_LOG.isInfoEnabled()) {
            TRANSFER_LOG.info(job.jobId() + " " + stageName + " | " + text);
        }

        long now = System.currentTimeMillis();
        if (now - activityPushedAt >= STATUS_FLUSH_INTERVAL_MILLIS) {
            pushStatus(text, now);
        }
    }

    /**
     * Writes the latest value even if the interval has not elapsed.
     *
     * <p>Called by the sequence as a run unwinds, so the last thing a stage said survives — which on
     * a failed run is the most useful field there is. Skips a value already written.
     */
    void flushStatus() {
        String text = activity;
        if (text != null && !text.equals(activityPushed)) {
            pushStatus(text, System.currentTimeMillis());
        }
    }

    /**
     * Telemetry on a live transfer, so it swallows everything.
     *
     * <p>The timestamp advances even on failure. A store that is down would otherwise be retried on
     * every single call, turning a broken database into a slow transfer.
     */
    private void pushStatus(String text, long now) {
        activityPushedAt = now;
        activityPushed = text;
        try {
            job.status(stageName, stageIndex, text);
        } catch (Throwable t) {
            if (log.isDebugEnabled()) {
                log.debug("stage '" + stageName + "' could not record its activity; the transfer is"
                        + " unaffected", t);
            }
        }
    }

    @Override
    public StageArtifact artifact() {
        return artifactFor(CANONICAL_ARTIFACT, null, null, 1);
    }

    @Override
    public StageArtifact artifact(String unit, String formatVersion, int checkpointFrequency) {
        if (!mayCheckpoint) {
            throw new IllegalStateException("operator '" + stageName + "' asked for a checkpointing"
                    + " artifact but did not declare checkpointed()");
        }
        if (checkpointStore == null) {
            throw new IllegalStateException("operator '" + stageName + "' declared checkpointed() but"
                    + " no checkpoint store is available; checkpoints live in the MFT datasource, so"
                    + " this needs one configured. A sequence whose operators checkpoint is rejected at"
                    + " deployment for this, so reaching here means it was assembled programmatically");
        }
        if (unit == null) {
            throw new IllegalArgumentException("operator '" + stageName + "' must say what its"
                    + " checkpoint positions count; a position without a unit is ambiguous");
        }
        if (formatVersion == null || formatVersion.isBlank()) {
            throw new IllegalArgumentException("operator '" + stageName + "' must supply a checkpoint"
                    + " format version, so a later version of it can refuse a checkpoint it cannot"
                    + " interpret rather than misreading one");
        }
        return artifactFor(CANONICAL_ARTIFACT, unit, formatVersion, checkpointFrequency);
    }

    @Override
    public StageArtifact artifact(String name) {
        if (name == null || name.isBlank() || name.indexOf('/') >= 0 || name.indexOf('\\') >= 0
                || name.indexOf('\0') >= 0 || ".".equals(name) || "..".equals(name)) {
            throw new IllegalArgumentException("operator '" + stageName + "' asked for an artifact"
                    + " named '" + name + "', which is not a single file name within its stage"
                    + " directory");
        }
        return artifactFor(name, null, null, 1);
    }

    /**
     * Builds this stage's artifact, or returns the one already built.
     *
     * <p>Cached by name because two instances over one file would each hold their own append offset
     * and their own idea of what is durable — a corruption the framework could not detect afterwards.
     * Constructing one performs no I/O, so calling this from {@code open()} or {@code wrap()} is safe.
     */
    private StageArtifact artifactFor(String name, String unit, String formatVersion,
                                      int checkpointFrequency) {
        if (!mayUseWorkspace) {
            throw new IllegalStateException("operator '" + stageName + "' asked for an artifact but"
                    + " declared neither materialises() nor checkpointed(); declare one, or do not"
                    + " keep durable state");
        }
        // A null workspace is not refused here. An operator that declares checkpointed() without
        // materialises() writes no bytes, so it needs no directory, and validate() demands a workspace
        // only for materialises(). Appending without one fails inside the artifact, where the message
        // can say which of the two is missing.

        Held held = artifacts.get(name);
        if (held != null) {
            if (unit != null && held.unit == null) {
                throw new IllegalStateException("operator '" + stageName + "' obtained artifact '"
                        + name + "' without a checkpoint unit and is now asking for one; a stage's"
                        + " artifact must be obtained the same way everywhere, or two callers disagree"
                        + " about whether its position is being recorded");
            }
            return held.artifact;
        }

        StageArtifact artifact = new DefaultStageArtifact(stageName, workspace, name,
                unit == null ? null : checkpointStore, unit, formatVersion, checkpointFrequency,
                resources, this::resumedFromArtifact,
                CANONICAL_ARTIFACT.equals(name));
        artifacts.put(name, new Held(artifact, unit));
        return artifact;
    }

    /**
     * The canonical artifact, if this stage obtained one, else {@code null}.
     *
     * <p>For the sequence, which concatenates the already-produced prefix ahead of a resumed stage's
     * output. Only the canonical name qualifies: a stage's own working files mean nothing about its
     * output, and prepending one would corrupt the stream — see {@link StageArtifact#openPrefix()}.
     */
    StageArtifact canonicalArtifact() {
        Held held = artifacts.get(CANONICAL_ARTIFACT);
        return held == null ? null : held.artifact();
    }

    /** An artifact and how it was obtained, so an inconsistent second request is caught. */
    private record Held(StageArtifact artifact, String unit) {
    }
}
