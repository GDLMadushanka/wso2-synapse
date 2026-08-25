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

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.OMNamespace;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.libraries.LibClassLoader;
import org.apache.synapse.stream.StreamException;
import org.apache.synapse.stream.StreamOperator;
import org.junit.After;
import org.junit.Test;

import javax.xml.namespace.QName;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link StreamOperatorFactoryFinder}.
 * <p>
 * The library-classloader pass is the one that matters. It is the only route by which an operator
 * shipped inside an MI connector becomes visible, and it runs lazily — so it is the thing most likely
 * to be silently broken by a change in ordering or classloader parentage. Everything else in the
 * pipeline plumbing depends on it working.
 */
public class StreamOperatorFactoryFinderTest {

    private static final String LIB_NAME = "test-operator-library";

    private File jar;

    @After
    public void cleanUp() {
        SynapseConfiguration.getLibraryClassLoaders().remove(LIB_NAME);
        StreamOperatorFactoryFinder.reset();
        if (jar != null) {
            jar.delete();
        }
    }

    // ---------------------------------------------------------------- pass 2: the classpath

    @Test
    public void findsAFactoryRegisteredThroughTheClasspathServicesFile() throws Exception {
        StreamOperator op = StreamOperatorFactoryFinder.getInstance()
                .getOperator(element(ClasspathTestOperatorFactory.TAG), new Properties());

        assertNotNull(op);
        assertEquals("test.classpathSink", op.name());
    }

    @Test
    public void reportsAnElementAsAKnownOperator() {
        assertTrue(StreamOperatorFactoryFinder.getInstance()
                .isOperator(ClasspathTestOperatorFactory.TAG));
    }

    // ---------------------------------------------------------------- pass 3: a deployed library

    /**
     * The decisive test for this phase. A factory visible <b>only</b> through a
     * {@code LibClassLoader} must resolve — otherwise no connector-shipped operator can ever be used.
     */
    @Test
    public void findsAFactoryVisibleOnlyThroughADeployedLibraryClassLoader() throws Exception {
        // Not registered on the classpath services file, so it must not be found yet.
        assertFalse("precondition: the library factory must not already be registered",
                StreamOperatorFactoryFinder.getInstance().isOperator(LibraryTestOperatorFactory.TAG));

        deployLibrary(LibraryTestOperatorFactory.class.getName());

        StreamOperator op = StreamOperatorFactoryFinder.getInstance()
                .getOperator(element(LibraryTestOperatorFactory.TAG), new Properties());

        assertNotNull(op);
        assertEquals("test.librarySink", op.name());
    }

    /**
     * The scan must be lazy. A connector deploys after this finder is first created, so a finder
     * built before the library existed still has to find it.
     */
    @Test
    public void scansLibrariesLazilySoAConnectorDeployedLaterIsStillFound() throws Exception {
        // Force the finder into existence before the library appears.
        StreamOperatorFactoryFinder finder = StreamOperatorFactoryFinder.getInstance();
        assertFalse(finder.isOperator(LibraryTestOperatorFactory.TAG));

        deployLibrary(LibraryTestOperatorFactory.class.getName());

        assertTrue("the same finder instance must pick up a library deployed afterwards",
                finder.isOperator(LibraryTestOperatorFactory.TAG));
    }

    @Test
    public void ignoresLoadersThatAreNotLibraryClassLoaders() throws Exception {
        // A plain URLClassLoader over the same jar is not a LibClassLoader and must be skipped,
        // so that this finder only ever picks up genuinely deployed libraries.
        jar = writeServicesJar(LibraryTestOperatorFactory.class.getName());
        SynapseConfiguration.addLibraryClassLoader(LIB_NAME,
                new java.net.URLClassLoader(new URL[]{jar.toURI().toURL()},
                        getClass().getClassLoader()));

        assertFalse(StreamOperatorFactoryFinder.getInstance()
                .isOperator(LibraryTestOperatorFactory.TAG));
    }

    // ---------------------------------------------------------------- misses and fallbacks

    @Test
    public void anUnknownElementFailsWithAMessageThatSaysHowToRegisterOne() {
        QName unknown = new QName("http://ws.apache.org/ns/synapse", "nobody.homeSink");

        try {
            StreamOperatorFactoryFinder.getInstance().getOperator(element(unknown), new Properties());
            fail("expected an unknown operator to be rejected");
        } catch (StreamException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("is not a known stream operator"));
            assertTrue("the message should say how to register one",
                    e.getMessage().contains("META-INF/services"));
            assertTrue("and list what is registered, to make a typo obvious",
                    e.getMessage().contains("test.classpathSink"));
        }
    }

    @Test
    public void fallsBackToLocalNameWhenTheNamespaceIsOmitted() throws Exception {
        // A hand-written artifact may leave the default namespace off its children. That is
        // obviously intended, so resolve it rather than failing.
        QName noNamespace = new QName("", "test.classpathSink");

        StreamOperator op = StreamOperatorFactoryFinder.getInstance()
                .getOperator(element(noNamespace), new Properties());

        assertEquals("test.classpathSink", op.name());
    }

    @Test
    public void rejectsANullElement() {
        try {
            StreamOperatorFactoryFinder.getInstance().getOperator(null, new Properties());
            fail("expected a null element to be rejected");
        } catch (StreamException e) {
            assertTrue(e.getMessage().contains("null element"));
        }
    }

    // ---------------------------------------------------------------- helpers

    /**
     * Writes a jar containing only a {@code META-INF/services} entry. The factory class itself comes
     * from the parent loader, which is what a real connector's classloader parentage also gives us —
     * so this exercises the services-file discovery without needing to compile at test time.
     */
    private void deployLibrary(String factoryClassName) throws Exception {
        jar = writeServicesJar(factoryClassName);
        SynapseConfiguration.addLibraryClassLoader(LIB_NAME,
                new LibClassLoader(new URL[]{jar.toURI().toURL()}, getClass().getClassLoader()));
    }

    private File writeServicesJar(String factoryClassName) throws Exception {
        File f = File.createTempFile("stream-operator-lib", ".jar");
        try (JarOutputStream out = new JarOutputStream(new FileOutputStream(f))) {
            out.putNextEntry(new JarEntry(
                    "META-INF/services/" + StreamOperatorFactory.class.getName()));
            out.write(factoryClassName.getBytes(StandardCharsets.UTF_8));
            out.closeEntry();
        }
        return f;
    }

    private static OMElement element(QName tag) {
        OMFactory f = OMAbstractFactory.getOMFactory();
        OMNamespace ns = f.createOMNamespace(tag.getNamespaceURI(), "");
        return f.createOMElement(tag.getLocalPart(), ns);
    }
}
