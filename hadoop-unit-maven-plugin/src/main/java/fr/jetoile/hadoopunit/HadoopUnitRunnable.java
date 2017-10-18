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

import org.apache.commons.lang3.StringUtils;
import org.apache.maven.plugin.logging.Log;
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
import org.eclipse.aether.resolution.ArtifactRequest;
import org.eclipse.aether.resolution.ArtifactResult;
import org.eclipse.aether.resolution.DependencyRequest;
import org.eclipse.aether.resolution.DependencyResolutionException;
import org.eclipse.aether.spi.connector.RepositoryConnectorFactory;
import org.eclipse.aether.spi.connector.transport.TransporterFactory;
import org.eclipse.aether.transport.file.FileTransporterFactory;
import org.eclipse.aether.transport.http.HttpTransporterFactory;
import org.eclipse.aether.util.artifact.JavaScopes;
import org.eclipse.aether.util.filter.DependencyFilterUtils;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

import static org.fusesource.jansi.Ansi.Color.GREEN;

public class HadoopUnitRunnable implements Runnable {

    private final List<ComponentArtifact> components;
    private final Log log;
    private RepositorySystemSession repoSession;
    private List<RemoteRepository> remoteRepos;
    private List<ComponentProperties> componentProperties = new ArrayList<>();
    private BlockingQueue queue = new ArrayBlockingQueue(1);
    private int port;

    public HadoopUnitRunnable(List<ComponentArtifact> components, BlockingQueue queue, Log log, int port, RepositorySystemSession repoSession, List<RemoteRepository> remoteRepos) {
        this.components = components;
        this.log = log;
        this.repoSession = repoSession;
        this.remoteRepos = remoteRepos;
        this.queue = queue;
        this.port = port;
    }

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

    @Override
    public void run() {
        log.info("is going to start hadoop unit");

        RepositorySystem system = newRepositorySystem();
        DefaultRepositorySystemSession session = newRepositorySystemSession(system);
        DependencyFilter classpathFilter = DependencyFilterUtils.classpathFilter(JavaScopes.COMPILE);

        components.stream().forEach(
                c -> {
                    Artifact artifact = new DefaultArtifact(c.getArtifact());

                    ArtifactRequest request = new ArtifactRequest();
                    CollectRequest collectRequest = new CollectRequest();
                    collectRequest.setRoot(new Dependency(artifact, JavaScopes.COMPILE));
                    collectRequest.setRepositories(remoteRepos);

                    log.info("Resolving artifact " + artifact + " from " + remoteRepos.stream().map(r -> r.getId() + "-" + r.getUrl()).collect(Collectors.joining(", ")));

                    DependencyRequest dependencyRequest = new DependencyRequest(collectRequest, classpathFilter);

                    List<ArtifactResult> artifactResults = null;
                    try {
                        artifactResults = system.resolveDependencies(session, dependencyRequest).getArtifactResults();
                    } catch (DependencyResolutionException e) {
                        log.error("an error occured during the dependencies phase: " + e.getMessage());
                        e.printStackTrace();
                    }

                    List<File> artifacts = new ArrayList<>();
                    artifactResults.stream().forEach(a ->
                            artifacts.add(a.getArtifact().getFile())
                    );

                    ComponentProperties componentProperty = loadAndRun(c, artifacts);
                    componentProperties.add(componentProperty);
                }
        );
        log.info("hadoop unit started");

        printBanner();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("All services are going to be stopped");
            stopAll(componentProperties);
        }));


        queue.add(new Object());

        try (ServerSocket serverSocket = new ServerSocket(port);
             Socket client = serverSocket.accept();
             BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
             PrintWriter output = new PrintWriter(client.getOutputStream(), true)) {


            String line = in.readLine();
            log.info("runner received " + line);

            if (StringUtils.containsIgnoreCase(line, "stop")) {
                log.info("is going to shutdown");
                this.stopAll(componentProperties);

                output.println("success");
                log.info("success sent");

                Thread.currentThread().interrupt();

                log.info("interrupt signal sent to hadoop unit");
            }

        } catch (IOException e) {
            log.error("shutdown failed: " + e.getMessage());
        }
    }

    public DefaultRepositorySystemSession newRepositorySystemSession(RepositorySystem system) {
        DefaultRepositorySystemSession session = MavenRepositorySystemUtils.newSession();

        LocalRepository localRepo1 = new LocalRepository(repoSession.getLocalRepository().getBasedir());
        session.setLocalRepositoryManager(system.newLocalRepositoryManager(session, localRepo1));

//        session.setTransferListener(new ConsoleTransferListener());
//        session.setRepositoryListener(new ConsoleRepositoryListener());

        return session;
    }

    private void stopAll(List<ComponentProperties> componentProperties) {
        List<ComponentProperties> componentsToStop = new ArrayList<>(componentProperties);
        Collections.reverse(componentsToStop);

        componentsToStop.stream().forEach(c -> {
            try {
                Method main = null;
                main = c.getMainClass().getMethod("stop");
                main.invoke(c.getInstance());
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                log.error("unable to reflect main", e);
            }
        });
    }

    private void printBanner() {
        HadoopUtils.INSTANCE.printBanner(System.out);
        componentProperties.stream().forEach(c -> {
            Object name = null;
            Object prop = null;

            if (c != null) {
                try {
                    Method main = c.getMainClass().getMethod("getName");
                    name = main.invoke(c.getInstance());

                    main = c.getMainClass().getMethod("getProperties");
                    prop = main.invoke(c.getInstance());
                } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                    log.error("unable to reflect main", e);
                }
                HadoopUtils.printColorLine(System.out, GREEN, "\t\t - " + name + " " + prop);
            }
        });
        System.out.println();
    }

    private ComponentProperties loadAndRun(ComponentArtifact c, List<File> artifacts) {
        Component component = Component.valueOf(c.getComponentName());
        String componentKey = component.getKey();
        String className = component.getMainClass();

        List<URL> urls = new ArrayList();

        Map<String, String> properties = c.getProperties();

        if ("solrcloud".equalsIgnoreCase(componentKey)) {

            String solrDir = properties.get("solr.dir");
            if (StringUtils.isEmpty(solrDir)) {
                log.warn("unable to find solr.dir property");
            }
        }
        if ("alluxio".equalsIgnoreCase(componentKey)) {

            String alluxioWebappDir = properties.get("alluxio.webapp.directory");
            if (StringUtils.isEmpty(alluxioWebappDir)) {
                log.warn("unable to find alluxio.webapp.directory property");
            }
        }

        artifacts.forEach(f -> {
            try {
                urls.add(f.toURL());
            } catch (MalformedURLException e) {
                log.error("unable to find correct url for " + f, e);
            }
        });

        ClassLoader classloader = new URLClassLoader(
                urls.toArray(new URL[0]),
                ClassLoader.getSystemClassLoader().getParent());

        // relative to that classloader, find the main class
        Class mainClass = null;
        try {
            mainClass = classloader.loadClass(className);
        } catch (ClassNotFoundException e) {
            log.error("unable to load class", e);
        }

        Method main;
        try {
            Thread.currentThread().setContextClassLoader(classloader);

            Object o = mainClass.getConstructor().newInstance();

            if (properties != null) {
                main = mainClass.getMethod("loadConfig", Map.class);
                main.invoke(o, properties);
            }

            main = mainClass.getMethod("start");
            main.invoke(o);
            return new ComponentProperties(o, mainClass);
        } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            log.error("unable to reflect main", e);
        }
        return null;
    }
}
