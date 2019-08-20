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

import com.google.inject.AbstractModule;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.cli.logging.Slf4jLoggerManager;
import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.apache.maven.settings.Mirror;
import org.apache.maven.settings.Settings;
import org.apache.maven.settings.building.DefaultSettingsBuildingRequest;
import org.apache.maven.settings.building.SettingsBuilder;
import org.apache.maven.settings.building.SettingsBuildingException;
import org.apache.maven.settings.building.SettingsBuildingRequest;
import org.codehaus.plexus.*;
import org.codehaus.plexus.classworlds.ClassWorld;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.RepositorySystem;
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
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.stream.Collectors;

import static fr.jetoile.hadoopunit.HadoopUnitConfig.DEFAULT_PROPS_FILE;
import static org.fusesource.jansi.Ansi.Color.GREEN;

public class HadoopStandaloneBootstrap {

    final private static Logger LOGGER = LoggerFactory.getLogger(HadoopStandaloneBootstrap.class);
    static private PropertiesConfiguration configuration;
    static private Configuration hadoopUnitConfiguration;
    static private List<ComponentProperties> componentsToStop = new ArrayList<>();
    static private List<ComponentProperties> componentsProperty = new ArrayList<>();

    static private Settings settings = null;

    /**
     * code from org.apache.maven.cli.MavenCli.container(CliRequest)
     * <p>
     * cf also https://github.com/igor-suhorukov/mvn-classloader/blob/master/dropship/src/main/java/com/github/smreed/dropship/ClassLoaderBuilder.java
     */
    private static PlexusContainer mvnContainer() {
        ILoggerFactory slf4jLoggerFactory = LoggerFactory.getILoggerFactory();
        Slf4jLoggerManager plexusLoggerManager = new Slf4jLoggerManager();

        ClassWorld classWorld = new ClassWorld("plexus.core", Thread.currentThread().getContextClassLoader());

        DefaultPlexusContainer container = null;

        ContainerConfiguration cc = new DefaultContainerConfiguration()
                .setClassWorld(classWorld)
                .setClassPathScanning(PlexusConstants.SCANNING_INDEX)
                .setAutoWiring(true)
                .setName("maven");

        try {
            container = new DefaultPlexusContainer(cc, new AbstractModule() {
                protected void configure() {
                    bind(ILoggerFactory.class).toInstance(slf4jLoggerFactory);
                }
            });
        } catch (PlexusContainerException e) {
            LOGGER.error("unable to create PlexusContainer", e);
        }

        // NOTE: To avoid inconsistencies, we'll use the TCCL exclusively for lookups
        container.setLookupRealm(null);

        container.setLoggerManager(plexusLoggerManager);
        Thread.currentThread().setContextClassLoader(container.getContainerRealm());

        return container;
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

    public static DefaultRepositorySystemSession newRepositorySystemSession(RepositorySystem system) throws BootstrapException {
        DefaultRepositorySystemSession session = MavenRepositorySystemUtils.newSession();
        String localRepositoryDir = "";


        String mavenHome = getInstalledMavenHome();

        String userHome = System.getProperty("user.home");
        if (StringUtils.isNotEmpty(mavenHome)) {
            //if MAVEN_HOME or M2_HOME are defined, will read maven's configuration throught settings.xml
            LOGGER.info("is going to use the local maven configuration: {}", mavenHome);
            Settings settings = getLocalSettings(mavenHome);

            localRepositoryDir = settings.getLocalRepository();
            if (localRepositoryDir == null) {
                LOGGER.info("is going to use default maven local repository");
                localRepositoryDir = userHome + "/.m2/repository";
            }
            LOGGER.info("is going to use {} repository", localRepositoryDir);
        } else {
            localRepositoryDir = hadoopUnitConfiguration.getString("maven.local.repo");
            if (localRepositoryDir != null) {
                LOGGER.info("is going to use the maven repository from {} with key {}", DEFAULT_PROPS_FILE, "maven.local.repo");
                localRepositoryDir = HadoopUtils.resolveDir(localRepositoryDir);
            } else {
                throw new BootstrapException("unable to find M2_HOME/MAVEN_HOME or the configuration key maven.local.repo from " + DEFAULT_PROPS_FILE);
            }
        }

        LocalRepository localRepo = new LocalRepository(localRepositoryDir);
        session.setLocalRepositoryManager(system.newLocalRepositoryManager(session, localRepo));

        boolean debug = Boolean.parseBoolean(hadoopUnitConfiguration.getString("maven.debug"));
        if (debug) {
            session.setTransferListener(new ConsoleTransferListener());
            session.setRepositoryListener(new ConsoleRepositoryListener());
        }

        return session;
    }

    private static Settings getLocalSettings(String mavenHome) {
        if (settings == null) {
            File globalSettingsFile = new File(mavenHome + "/conf/settings.xml");
            if (!globalSettingsFile.exists()) {
                LOGGER.error("maven global settings.xml file not found : {}", globalSettingsFile);
            }

            File userSettingsFile = new File(System.getProperty("user.home") + "/.m2/settings.xml");
            if (!userSettingsFile.exists()) {
                LOGGER.info("maven user settings.xml override file not found : {}", userSettingsFile);
            }

            PlexusContainer container = mvnContainer();

            SettingsBuilder defaultSettingsBuilder = null;
            try {
                defaultSettingsBuilder = container.lookup(SettingsBuilder.class);
            } catch (ComponentLookupException e) {
                LOGGER.error("unable to lookup SettingsBuilder", e);
            }
            SettingsBuildingRequest settingsRequest = new DefaultSettingsBuildingRequest();
            settingsRequest.setGlobalSettingsFile(globalSettingsFile);
            settingsRequest.setUserSettingsFile(userSettingsFile);
            try {
                settings = defaultSettingsBuilder.build(settingsRequest).getEffectiveSettings();
            } catch (SettingsBuildingException e) {
                LOGGER.error("unable to get settings", e);
            }
        }
        return settings;
    }

    private static String getInstalledMavenHome() {
        String mavenHome = null;
        String m2_home = System.getenv("M2_HOME");
        if (StringUtils.isNotEmpty(m2_home)) {
            LOGGER.info("is going to use M2_HOME to read configuration");
            mavenHome = m2_home;
        }
        if (mavenHome == null) {
            String maven_home = System.getenv("MAVEN_HOME"); // legacy, for maven 1
            if (StringUtils.isNotEmpty(maven_home)) {
                LOGGER.info("is going to use MAVEN_HOME to read configuration");
                mavenHome = maven_home;
            }
        }
        return mavenHome;
    }

    private static RepositorySystem getRepositorySystem() throws ComponentLookupException {
        if (StringUtils.isNotEmpty(getInstalledMavenHome())) {
            PlexusContainer mvnContainer = mvnContainer();
            return mvnContainer.lookup(RepositorySystem.class);
        } else {
            return newRepositorySystem();
        }
    }

    public static List<RemoteRepository> newRepositories() {
        return new ArrayList<>(Arrays.asList(newCentralRepository()));
    }

    private static RemoteRepository newCentralRepository() {
        return new RemoteRepository.Builder("central", "default", hadoopUnitConfiguration.getString("maven.central.repo")).build();
    }

    public static void main(String[] args) throws BootstrapException {
        String homeDirectory = ".";
        if (StringUtils.isNotEmpty(System.getenv("HADOOP_UNIT_HOME"))) {
            homeDirectory = System.getenv("HADOOP_UNIT_HOME");
        }
        LOGGER.info("is using {} for local directory", homeDirectory);

        List<String> componentsToStart = new ArrayList<>();

        try {
            configuration = new PropertiesConfiguration("hadoop.properties");
            hadoopUnitConfiguration = new PropertiesConfiguration(DEFAULT_PROPS_FILE);

            FileReader fileReader = new FileReader(configuration.getFile());

            Properties properties = new Properties();
            properties.load(fileReader);
            Enumeration<String> componentKeys = (Enumeration<String>) properties.propertyNames();

            componentsToStart = Collections.list(componentKeys).stream().map(String::toString).filter(c -> configuration.getBoolean(c)).collect(Collectors.toList());
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        } catch (IOException e) {
            LOGGER.error("unable to read configuration's file", e);
        }


        Map<String, ComponentDependencies> componentsToStartWithDependencies = loadMavenDependencies(componentsToStart);
        addModuleJarToCurrentClassloader(componentsToStartWithDependencies);
        List<String> componentsNameToStart = computeStartingOrder(componentsToStartWithDependencies);

        for (String c : componentsNameToStart) {
            ComponentProperties componentProperties = loadAndRun(c, hadoopUnitConfiguration.getString(c.toLowerCase() + ".mainClass"), componentsToStartWithDependencies.get(c.toLowerCase()).getDependencies());

            componentsProperty.add(componentProperties);
            componentsToStop.add(0, componentProperties);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
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
        }));

        printBanner();
    }

    private static List<String> computeStartingOrder(Map<String, ComponentDependencies> componentsToStartWithDependencies) {
        Map<String, ComponentMetadata> commands = new HashMap<>();
        componentsToStartWithDependencies.entrySet().stream().forEach(entry -> {
            try {
                ClassLoader currentThreadClassLoader = Thread.currentThread().getContextClassLoader();

                Class<?> componentClass = currentThreadClassLoader.loadClass(hadoopUnitConfiguration.getString(entry.getKey() + ".metadataClass"));
                ComponentMetadata componentInstance = (ComponentMetadata) componentClass.getConstructor().newInstance();
                commands.put(componentInstance.getName(), componentInstance);
            } catch (Exception e) {
                LOGGER.error("unable to instantiate {}", entry.getValue().getName(), e);
            }
        });

        Graph<String, DefaultEdge> dependenciesGraph = generateGraph(commands);
        Map<String, List<String>> dependenciesMapByComponent = commands.values().stream().collect(Collectors.toMap(ComponentMetadata::getName, c -> DependenciesCalculator.calculateParents(dependenciesGraph, c.getName())));
        Map<String, List<String>> transitiveDependenciesMapByComponent = DependenciesCalculator.findTransitiveDependenciesByComponent(dependenciesMapByComponent);

        return DependenciesCalculator.dryRunToDefineCorrectOrder(transitiveDependenciesMapByComponent);
    }

    private static void addModuleJarToCurrentClassloader(Map<String, ComponentDependencies> componentsToStartWithDependencies) {
        componentsToStartWithDependencies.values().forEach(c -> {
            try {
                ClassLoader currentThreadClassLoader = Thread.currentThread().getContextClassLoader();
                URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{c.getDependencies().get(0).toURL()}, currentThreadClassLoader);
                Thread.currentThread().setContextClassLoader(urlClassLoader);
            } catch (MalformedURLException e) {
                LOGGER.error("unable to locate {}", c.getDependencies().get(0));
            }
        });
    }

    private static Map<String, ComponentDependencies> loadMavenDependencies(List<String> componentsToStart) throws BootstrapException {
        Map<String, ComponentDependencies> componentsToStartWithDependencies = new HashMap<>();

        RepositorySystem repositorySystem;
        try {
            repositorySystem = getRepositorySystem();
        } catch (ComponentLookupException e) {
            throw new BootstrapException("unable to get RepositoySystem from external maven", e);
        }
        DefaultRepositorySystemSession session = newRepositorySystemSession(repositorySystem);
        DependencyFilter classpathFilter = DependencyFilterUtils.classpathFilter(JavaScopes.RUNTIME);

        for (String c : componentsToStart) {
            Artifact artifact = new DefaultArtifact(hadoopUnitConfiguration.getString(c + ".artifactId"));
            CollectRequest collectRequest = new CollectRequest();
            collectRequest.setRoot(new Dependency(artifact, JavaScopes.RUNTIME));
            collectRequest.setRepositories(getRemoteRepositories());

            DependencyRequest dependencyRequest = new DependencyRequest(collectRequest, classpathFilter);

            List<ArtifactResult> artifactResults;
            try {
                artifactResults = repositorySystem.resolveDependencies(session, dependencyRequest).getArtifactResults();
            } catch (DependencyResolutionException e) {
                throw new BootstrapException("failed to resolve dependency artifact " + artifact, e);
            }


            List<File> artifacts = new ArrayList<>();
            artifactResults.stream().forEach(a ->
                    artifacts.add(a.getArtifact().getFile())
            );
            ComponentDependencies component = new ComponentDependencies(c, artifacts);
            componentsToStartWithDependencies.put(c, component);

        }
        return componentsToStartWithDependencies;
    }

    private static Graph<String, DefaultEdge> generateGraph(Map<String, ComponentMetadata> commands) {
        Graph<String, DefaultEdge> graph = new SimpleDirectedGraph<>(DefaultEdge.class);

        commands.keySet().stream().forEach(
                c -> graph.addVertex(c)
        );

        commands.entrySet().stream().forEach(entry -> {
                    String key = entry.getKey();
                    entry.getValue().getDependencies().stream().forEach(dependency -> {
                        try {
                            graph.addEdge(key, dependency);
                        } catch (IllegalArgumentException e) {
                            //ignore it : if a dependency is declared in metadata but is not present on runtime
                            LOGGER.warn("{} is not declared into the component's dependencies {}", dependency, key);
                        }
                    });
                }
        );

        return graph;
    }

    private static List<RemoteRepository> getRemoteRepositories() {
        String mavenHome = getInstalledMavenHome();
        if (StringUtils.isEmpty(mavenHome)) {
            return newRepositories();
        } else {
            List<Mirror> mirrors = getLocalSettings(mavenHome).getMirrors();
            if (mirrors.isEmpty()) {
                LOGGER.info("no mirror have been defined into maven's configuration. Is going to use {} from maven.central.repo", hadoopUnitConfiguration.getString("maven.central.repo"));
                return newRepositories();
            }
            List<RemoteRepository> remoteRepositories = mirrors.stream().map(mirror -> new RemoteRepository.Builder(mirror.getId(), "default", mirror.getUrl()).build()).collect(Collectors.toList());
            return remoteRepositories;
        }
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
                HadoopUtils.printColorLine(System.out, GREEN, "\t\t - " + name + " " + prop);
            }
        });
        System.out.println();
    }

    @SuppressWarnings("resource")
    private static ComponentProperties loadAndRun(String c, String className, List<File> artifacts) {
        List<URL> urls = new ArrayList<>();

        urls.add(HadoopStandaloneBootstrap.class.getClassLoader().getResource("log4j.xml"));
        urls.add(HadoopStandaloneBootstrap.class.getClassLoader().getResource("logback.xml"));

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
        Class<?> mainClass;
        try {
            mainClass = classloader.loadClass(className);
        } catch (ClassNotFoundException e) {
            LOGGER.error("unable to load class", e);
            return null;
        }


        try {
            Thread.currentThread().setContextClassLoader(classloader);

            Object o = mainClass.getConstructor(URL.class).newInstance(HadoopStandaloneBootstrap.class.getClassLoader().getResource(DEFAULT_PROPS_FILE));
            Method main = mainClass.getMethod("start");
            main.invoke(o);
            return new ComponentProperties(o, mainClass);
        } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            LOGGER.error("unable to reflect main", e);
        }
        return null;
    }

    private static class ComponentDependencies {
        private String name;
        private List<File> dependencies;

        public ComponentDependencies(String name, List<File> dependencies) {
            this.name = name;
            this.dependencies = dependencies;
        }

        public String getName() {
            return name;
        }

        public List<File> getDependencies() {
            return dependencies;
        }
    }

    private static class ComponentProperties {
        private Object instance;
        private Class<?> mainClass;

        public ComponentProperties(Object instance, Class<?> mainClass) {
            this.instance = instance;
            this.mainClass = mainClass;
        }

        public Object getInstance() {
            return instance;
        }

        public Class<?> getMainClass() {
            return mainClass;
        }
    }
}