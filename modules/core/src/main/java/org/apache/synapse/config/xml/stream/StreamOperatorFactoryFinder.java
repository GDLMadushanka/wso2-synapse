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

package org.apache.synapse.config.xml.stream;

import org.apache.axiom.om.OMElement;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.libraries.LibClassLoader;
import org.apache.synapse.stream.StreamException;
import org.apache.synapse.stream.StreamOperator;

import javax.xml.namespace.QName;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves a {@code <streamPipeline>} child element to a {@link StreamOperator}.
 * <p>
 * Mirrors {@code MediatorFactoryFinder}'s shape — a built-in table plus {@code ServiceLoader} — with
 * one addition that matters more than it looks: it also scans <b>deployed library classloaders</b>,
 * which is the only way an operator shipped inside an MI connector can be seen.
 *
 * <h2>Three passes</h2>
 * <ol>
 *   <li><b>Built-ins</b>, from a table in this class.</li>
 *   <li><b>{@code ServiceLoader}</b> on this class's own loader — covers jars dropped into
 *       {@code $MI_HOME/lib} and anything on the server classpath.</li>
 *   <li><b>Deployed library classloaders</b>, via
 *       {@link SynapseConfiguration#getLibraryClassLoaders()}. This one is <b>lazy</b>, run on a
 *       miss rather than at construction, because connectors deploy after this finder first exists.
 *       {@code GenericProcessor} performs the same walk for inbound endpoint classes.</li>
 * </ol>
 * Pass 3 is why {@code STREAM_PIPELINE_TYPE} must be ordered after {@code SYNAPSE_LIBRARY_TYPE} in
 * the CApp deployment sequence: a pipeline deployed before its operators' connector has nothing to
 * resolve against.
 *
 * <h2>Lookup is by QName, with a localName fallback</h2>
 * A pipeline's children normally inherit the Synapse namespace from the artifact root, but a
 * hand-written artifact may omit it. Rather than fail on something that is obviously intended, an
 * exact-QName miss falls back to matching on local name — and only when exactly one registered
 * factory claims that local name, so the fallback can never silently pick the wrong operator.
 */
public class StreamOperatorFactoryFinder {

    private static final Log log = LogFactory.getLog(StreamOperatorFactoryFinder.class);

    /** Operators shipped with Synapse itself. None yet; operators live in connectors. */
    private static final Class<?>[] BUILT_INS = {};

    private static StreamOperatorFactoryFinder instance;

    private final Map<QName, StreamOperatorFactory> factories = new ConcurrentHashMap<>();

    private StreamOperatorFactoryFinder() {
        loadBuiltIns();
        registerFrom(ServiceLoader.load(StreamOperatorFactory.class), "the server classpath");
    }

    public static synchronized StreamOperatorFactoryFinder getInstance() {
        if (instance == null) {
            instance = new StreamOperatorFactoryFinder();
        }
        return instance;
    }

    /** Discards the singleton. For tests only. */
    static synchronized void reset() {
        instance = null;
    }

    private void loadBuiltIns() {
        for (Class<?> c : BUILT_INS) {
            try {
                register((StreamOperatorFactory) c.getDeclaredConstructor().newInstance(),
                        "built-ins");
            } catch (Exception e) {
                log.error("Could not instantiate built-in stream operator factory " + c, e);
            }
        }
    }

    private void registerFrom(Iterable<StreamOperatorFactory> loaded, String origin) {
        // Iterated one provider at a time, because ServiceLoader throws lazily on the offending
        // entry. Wrapping the whole loop would let a single unusable provider — a class that is not
        // public, say, or one whose dependencies are missing — hide every operator listed after it.
        Iterator<StreamOperatorFactory> it;
        try {
            it = loaded.iterator();
        } catch (Throwable t) {
            log.warn("Could not read stream operator factories from " + origin, t);
            return;
        }
        while (true) {
            StreamOperatorFactory f;
            try {
                if (!it.hasNext()) {
                    break;
                }
                f = it.next();
            } catch (Throwable t) {
                log.error("Skipping an unusable stream operator factory declared in " + origin
                        + "; the remaining entries are still loaded", t);
                continue;
            }
            try {
                register(f, origin);
            } catch (Throwable t) {
                log.error("Could not register stream operator factory "
                        + f.getClass().getName() + " from " + origin, t);
            }
        }
    }

    private void register(StreamOperatorFactory factory, String origin) {
        QName tag = factory.getTagQName();
        if (tag == null) {
            log.error("Ignoring stream operator factory " + factory.getClass().getName()
                    + " because getTagQName() returned null");
            return;
        }
        StreamOperatorFactory previous = factories.put(tag, factory);
        if (log.isDebugEnabled()) {
            log.debug("Registered stream operator factory " + factory.getClass().getName()
                    + " for " + tag + " from " + origin
                    + (previous == null ? "" : " (replacing " + previous.getClass().getName() + ")"));
        }
    }

    /**
     * Scans deployed library classloaders for operator factories.
     * <p>
     * Run on a miss rather than cached, deliberately. A miss happens at deployment time for an
     * unknown element, not per message, so rescanning costs nothing measurable — and it means a
     * connector deployed after this finder was created is still found, with no stale-cache bookkeeping
     * to get wrong.
     */
    private void scanLibraries() {
        Map<String, ClassLoader> loaders = SynapseConfiguration.getLibraryClassLoaders();
        if (loaders == null || loaders.isEmpty()) {
            return;
        }
        for (Map.Entry<String, ClassLoader> entry : loaders.entrySet()) {
            ClassLoader cl = entry.getValue();
            if (cl instanceof LibClassLoader) {
                registerFrom(ServiceLoader.load(StreamOperatorFactory.class, cl),
                        "library '" + entry.getKey() + "'");
            }
        }
    }

    /**
     * Builds the operator for an element.
     *
     * @param elem       the pipeline child element
     * @param properties deployment properties
     * @return the operator; never {@code null}
     * @throws StreamException if no factory handles this element, or the factory rejects it
     */
    /**
     * Whether an operator element could be built right now, without building it.
     *
     * <p>Exists for one reason: a connector's operators are not on any classloader until the library
     * deployer has run, and that happens <b>after</b> the Synapse configuration is parsed. So a pipeline
     * naming {@code <file.streamRead>} cannot be resolved at parse time however hard we try. The factory
     * uses this to notice the case and defer the whole pipeline to {@code init()}, which does run after
     * libraries are deployed.
     *
     * @param tag the element's qualified name
     * @return {@code true} if a factory is registered for it
     */
    public boolean isKnown(QName tag) {
        if (tag == null) {
            return false;
        }
        if (resolve(tag) != null) {
            return true;
        }
        scanLibraries();
        return resolve(tag) != null;
    }

    public StreamOperator getOperator(OMElement elem, Properties properties) throws StreamException {
        if (elem == null) {
            throw new StreamException("cannot resolve a null element to a stream operator");
        }
        QName tag = elem.getQName();

        StreamOperatorFactory factory = resolve(tag);
        if (factory == null) {
            // Might be an operator from a connector deployed since this finder was built.
            scanLibraries();
            factory = resolve(tag);
        }
        if (factory == null) {
            throw new StreamException("'" + tag + "' is not a known stream operator. Operators are"
                    + " registered through META-INF/services/"
                    + StreamOperatorFactory.class.getName() + ", either on the server classpath or"
                    + " inside a deployed connector. Registered operators: " + registeredNames());
        }

        StreamOperator op = factory.createOperator(elem, properties);
        if (op == null) {
            throw new StreamException("factory " + factory.getClass().getName() + " returned no"
                    + " operator for '" + tag + "'");
        }
        return op;
    }

    /** Whether an element is a known operator. Used to tell an operator from a stray child. */
    public boolean isOperator(QName tag) {
        if (tag == null) {
            return false;
        }
        if (resolve(tag) != null) {
            return true;
        }
        scanLibraries();
        return resolve(tag) != null;
    }

    private StreamOperatorFactory resolve(QName tag) {
        StreamOperatorFactory exact = factories.get(tag);
        if (exact != null) {
            return exact;
        }
        // Namespace-insensitive fallback, but only when it is unambiguous.
        List<StreamOperatorFactory> byLocalName = new ArrayList<>(2);
        for (Map.Entry<QName, StreamOperatorFactory> e : factories.entrySet()) {
            if (e.getKey().getLocalPart().equals(tag.getLocalPart())) {
                byLocalName.add(e.getValue());
            }
        }
        return byLocalName.size() == 1 ? byLocalName.get(0) : null;
    }

    private String registeredNames() {
        if (factories.isEmpty()) {
            return "none";
        }
        List<String> names = new ArrayList<>(factories.size());
        for (QName q : factories.keySet()) {
            names.add(q.getLocalPart());
        }
        java.util.Collections.sort(names);
        return String.join(", ", names);
    }
}
