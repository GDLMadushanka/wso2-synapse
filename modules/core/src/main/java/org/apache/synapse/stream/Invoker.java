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

import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.rest.RESTConstants;

import java.util.ArrayList;
import java.util.List;

/**
 * What caused a run, and the chain of artifacts it came through.
 *
 * <h2>The name is a path, not a name</h2>
 * A synchronous run has one artifact in front of it — the inbound endpoint, API, proxy or sequence that
 * invoked the pipeline — and the path is that one name. A <b>queued</b> run has three, and none of them
 * on its own answers "where did this come from":
 *
 * <pre>
 *   mft-jobs -&gt; mft-queue -&gt; mft-runner
 *   ^ submitted here  ^ waited here  ^ ran from here
 * </pre>
 *
 * <p>They are separated in time as well as in configuration: the API that submitted the transfer
 * returned long before the processor claimed it, and is not on any thread the run can see. So the
 * submitter is captured at submission, carried on the transfer row, and joined to the store and
 * processor by whoever claims it.
 *
 * <h2>Why one column and not three</h2>
 * Because the number of artifacts in front of a run is not fixed, and a schema that fixed it at three
 * would be wrong for the first topology that has two or four. The path is stored as text and read back
 * by {@link #segments}, which is what any filter matches against — filtering on <i>an artifact
 * anywhere in the path</i> is the question people actually ask, and it stays answerable however long
 * the path gets.
 */
public record Invoker(String type, String name) {

    /** What separates one artifact from the next. Chosen to read as a chain in a log and in a table. */
    public static final String ARROW = " -> ";

    /** A run whose invoker could not be identified at all. */
    public static final String DIRECT = "DIRECT";

    /** A run a message processor claimed from a store, rather than one invoked inline. */
    public static final String QUEUED = "QUEUED";

    /** An inbound endpoint. Named here so a caller that has to declare itself uses the same word. */
    public static final String INBOUND = "INBOUND";

    public static final String API = "API";

    public static final String PROXY = "PROXY";

    /**
     * Reads the invoking artifact off a message.
     *
     * <p>Synapse already stamps it on, so this reads the same properties the rest of the product does
     * rather than asking a caller to declare what the runtime already knows.
     *
     * <p><b>Only works for a caller that was mediating.</b> A caller on the seed path — an inbound
     * endpoint handing the pipeline bytes it already holds — never injected a message, and the context
     * it passes is a bare carrier it synthesised for operator configuration. Nothing stamped an
     * artifact name on it, and nothing could have. Such a caller declares itself through
     * {@link JobContext#invokerType} instead, which is why that is checked first.
     *
     * @param msg the message being mediated, or {@code null}
     * @return the invoker, or {@code null} when nothing on the message identifies one — a pipeline run
     *         from a plain sequence, a seed-path caller's carrier, and tests
     */
    public static Invoker of(MessageContext msg) {
        if (msg == null) {
            return null;
        }
        String proxy = trimToNull(asString(msg.getProperty(SynapseConstants.PROXY_SERVICE)));
        if (proxy != null) {
            return new Invoker(PROXY, proxy);
        }
        // Carries "name:vX" for a versioned API, which is that API's real identity and is stored as
        // such -- anything filtering on it later has to match the same string.
        String api = trimToNull(asString(msg.getProperty(RESTConstants.SYNAPSE_REST_API)));
        if (api != null) {
            return new Invoker(API, api);
        }
        String inbound = trimToNull(asString(msg.getProperty(SynapseConstants.INBOUND_ENDPOINT_NAME)));
        if (inbound != null) {
            return new Invoker(INBOUND, inbound);
        }
        return null;
    }

    /**
     * Joins artifacts into a path, in the order a transfer passed through them.
     *
     * @param artifacts the chain; nulls and blanks are dropped rather than becoming empty segments,
     *                  because "we could not identify the submitter" is honestly a shorter path and not
     *                  a path with a hole in it
     * @return the path, or {@code null} when nothing could be named
     */
    public static String path(String... artifacts) {
        StringBuilder path = new StringBuilder();
        for (String artifact : artifacts) {
            String trimmed = trimToNull(artifact);
            if (trimmed == null) {
                continue;
            }
            if (path.length() > 0) {
                path.append(ARROW);
            }
            path.append(trimmed);
        }
        return path.length() == 0 ? null : path.toString();
    }

    /**
     * Splits a path back into the artifacts it names.
     *
     * <p>What a filter matches against. A path is one column, so a database can only pre-filter it
     * with a substring test — and a substring test alone would answer "api1" with rows for "api10".
     * Segment equality is the check that means what the operator asked.
     *
     * @param path a stored invoker name, or {@code null}
     * @return its artifacts in order; empty when there are none
     */
    public static List<String> segments(String path) {
        List<String> segments = new ArrayList<>();
        if (path == null) {
            return segments;
        }
        for (String part : path.split("->")) {
            String trimmed = trimToNull(part);
            if (trimmed != null) {
                segments.add(trimmed);
            }
        }
        return segments;
    }

    /**
     * @param path     a stored invoker name
     * @param artifact the artifact to look for
     * @return whether that artifact is one of the path's segments — not merely a substring of one
     */
    public static boolean passedThrough(String path, String artifact) {
        String wanted = trimToNull(artifact);
        if (wanted == null) {
            return true;
        }
        for (String segment : segments(path)) {
            if (segment.equalsIgnoreCase(wanted)) {
                return true;
            }
        }
        return false;
    }

    private static String asString(Object value) {
        return value == null ? null : value.toString();
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
