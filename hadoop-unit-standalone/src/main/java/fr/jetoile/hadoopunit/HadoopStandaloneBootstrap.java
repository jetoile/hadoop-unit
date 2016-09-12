/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.jetoile.hadoopunit;

import fr.jetoile.hadoopunit.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.WriterAppender;
import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.collection.CollectRequest;
import org.eclipse.aether.connector.basic.BasicRepositoryConnectorFactory;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.graph.DependencyFilter;
import org.eclipse.aether.impl.DefaultServiceLocator;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.resolution.ArtifactResult;
import org.eclipse.aether.resolution.DependencyRequest;
import org.eclipse.aether.resolution.DependencyResolutionException;
import org.eclipse.aether.spi.connector.RepositoryConnectorFactory;
import org.eclipse.aether.spi.connector.transport.TransporterFactory;
import org.eclipse.aether.transport.file.FileTransporterFactory;
import org.eclipse.aether.transport.http.HttpTransporterFactory;
import org.eclipse.aether.util.artifact.JavaScopes;
import org.eclipse.aether.util.filter.DependencyFilterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HadoopStandaloneBootstrap {

    final private static Logger LOGGER = LoggerFactory.getLogger(HadoopStandaloneBootstrap.class);
    static private Configuration configuration;
    static private Configuration hadoopUnitConfiguration;
    static private List<Component> componentsToStart = new ArrayList<>();
    static private List<ComponentProperties> componentsToStop = new ArrayList<>();
    static private List<ComponentProperties> componentsProperty = new ArrayList();


    public static RepositorySystem newRepositorySystem() {
        DefaultServiceLocator locator = MavenRepositorySystemUtils.newServiceLocator();
        locator.addService(RepositoryConnectorFactory.class, BasicRepositoryConnectorFactory.class);
        locator.addService(TransporterFactory.class, FileTransporterFactory.class);
        locator.addService(TransporterFactory.class, HttpTransporterFactory.class);

        locator.setErrorHandler(new DefaultServiceLocator.ErrorHandler() {
            @Override
            public void serviceCreationFailed(Class<?> type, Class<?> impl, Throwable exception) {
                exception.printStackTrace();
            }
        });

        return locator.getService(RepositorySystem.class);
    }

    public static DefaultRepositorySystemSession newRepositorySystemSession(RepositorySystem system) {
        DefaultRepositorySystemSession session = MavenRepositorySystemUtils.newSession();

        LocalRepository localRepo = new LocalRepository(hadoopUnitConfiguration.getString("maven.local.repo"));
        session.setLocalRepositoryManager(system.newLocalRepositoryManager(session, localRepo));

//        session.setTransferListener(new ConsoleTransferListener());
//        session.setRepositoryListener(new ConsoleRepositoryListener());

        return session;
    }

    public static List<RemoteRepository> newRepositories(RepositorySystem system, RepositorySystemSession session) {
        return new ArrayList<>(Arrays.asList(newCentralRepository()));
    }

    private static RemoteRepository newCentralRepository() {
        return new RemoteRepository.Builder("central", "default", hadoopUnitConfiguration.getString("maven.central.repo")).build();
    }

    public static void main(String[] args) throws BootstrapException, MalformedURLException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, DependencyResolutionException {
        String homeDirectory = ".";
        if (StringUtils.isNotEmpty(System.getenv("HADOOP_UNIT_HOME"))) {
            homeDirectory = System.getenv("HADOOP_UNIT_HOME");
        }
        LOGGER.debug("is using {} for local directory", homeDirectory);


        try {
            configuration = new PropertiesConfiguration("hadoop.properties");
            hadoopUnitConfiguration = new PropertiesConfiguration("hadoop-unit-default.properties");
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }

        Arrays.asList(Component.values()).stream().forEach(c -> {
            if (configuration.containsKey(c.name().toLowerCase()) && configuration.getBoolean(c.name().toLowerCase())) {
                componentsToStart.add(c);
            }
        });

        RepositorySystem system = newRepositorySystem();
        DefaultRepositorySystemSession session = newRepositorySystemSession(system);
        DependencyFilter classpathFilter = DependencyFilterUtils.classpathFilter(JavaScopes.COMPILE);

        componentsToStart.stream().forEach(c -> {
            Artifact artifact = new DefaultArtifact(hadoopUnitConfiguration.getString(c.getArtifactKey()));
            CollectRequest collectRequest = new CollectRequest();
            collectRequest.setRoot(new Dependency(artifact, JavaScopes.COMPILE));
            collectRequest.setRepositories(newRepositories(system, session));

            DependencyRequest dependencyRequest = new DependencyRequest(collectRequest, classpathFilter);

            List<ArtifactResult> artifactResults = null;
            try {
                artifactResults = system.resolveDependencies(session, dependencyRequest).getArtifactResults();
            } catch (DependencyResolutionException e) {
                e.printStackTrace();
            }

            List<File> artifacts = new ArrayList<>();
            artifactResults.stream().forEach(a ->
                    artifacts.add(a.getArtifact().getFile())
            );
            ComponentProperties componentProperties = loadAndRun(c.getKey(), c.getMainClass(), artifacts);

            componentsProperty.add(componentProperties);
            componentsToStop.add(0, componentProperties);
        });


        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                LOGGER.info("All services are going to be stopped");
                componentsToStop.stream().forEach(c -> {
                    if (c != null) {
                        try {
                            Method main = c.getMainClass().getMethod("stop");
                            main.invoke(c.getInstance());
                        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                            LOGGER.error("unable to reflect main", e);
                        }
                    }
                });
            }
        });


        printBanner();
    }

    private static void printBanner() {
        HadoopUtils.INSTANCE.printBanner(System.out);
        componentsProperty.stream().forEach(c -> {
            Object name = null;
            Object prop = null;

            if (c != null) {
                try {
                    Method main = c.getMainClass().getMethod("getName");
                    name = main.invoke(c.getInstance());

                    main = c.getMainClass().getMethod("getProperties");
                    prop = main.invoke(c.getInstance());
                } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                    LOGGER.error("unable to reflect main", e);
                }

                System.out.println("\t\t - " + name + " " + prop);
            }
        });
        System.out.println();
    }

    private static ComponentProperties loadAndRun(String c, String className, List<File> artifacts) {
        List<URL> urls = new ArrayList();

        urls.add(HadoopStandaloneBootstrap.class.getClassLoader().getResource("log4j.xml"));
        urls.add(HadoopStandaloneBootstrap.class.getClassLoader().getResource("logback.xml"));

        if ("hiveserver2".equalsIgnoreCase(c)) {
            urls.add(WriterAppender.class.getProtectionDomain().getCodeSource().getLocation());
        }

        if ("solrcloud".equalsIgnoreCase(c)) {
            urls.add(HadoopStandaloneBootstrap.class.getClassLoader().getResource("solr"));
        }

        artifacts.forEach(f -> {
            try {
                urls.add(f.toURL());
            } catch (MalformedURLException e) {
                LOGGER.error("unable to find correct url for {}", f, e);
            }
        });

        ClassLoader classloader = new URLClassLoader(
                (URL[]) urls.toArray(new URL[0]),
                ClassLoader.getSystemClassLoader().getParent());

        // relative to that classloader, find the main class
        Class mainClass = null;
        try {
            mainClass = classloader.loadClass(className);
        } catch (ClassNotFoundException e) {
            LOGGER.error("unable to load class", e);
        }
        Method main = null;


        try {
            Thread.currentThread().setContextClassLoader(classloader);

            Object o = mainClass.getConstructor().newInstance();
            main = mainClass.getMethod("start");
            main.invoke(o);
            return new ComponentProperties(o, mainClass);
        } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            LOGGER.error("unable to reflect main", e);
        }
        return null;
    }

    private static class ComponentProperties {
        private Object instance;
        private Class mainClass;

        public ComponentProperties(Object instance, Class mainClass) {
            this.instance = instance;
            this.mainClass = mainClass;
        }

        public Object getInstance() {
            return instance;
        }

        public Class getMainClass() {
            return mainClass;
        }
    }
}
