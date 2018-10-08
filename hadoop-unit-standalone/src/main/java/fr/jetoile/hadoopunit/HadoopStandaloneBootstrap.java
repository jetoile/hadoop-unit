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

import static org.fusesource.jansi.Ansi.Color.GREEN;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.cli.logging.Slf4jLoggerManager;
import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.apache.maven.settings.Settings;
import org.apache.maven.settings.building.DefaultSettingsBuildingRequest;
import org.apache.maven.settings.building.SettingsBuilder;
import org.apache.maven.settings.building.SettingsBuildingRequest;
import org.codehaus.plexus.ContainerConfiguration;
import org.codehaus.plexus.DefaultContainerConfiguration;
import org.codehaus.plexus.DefaultPlexusContainer;
import org.codehaus.plexus.PlexusConstants;
import org.codehaus.plexus.PlexusContainer;
import org.codehaus.plexus.classworlds.ClassWorld;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.collection.CollectRequest;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.graph.DependencyFilter;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.resolution.ArtifactResult;
import org.eclipse.aether.resolution.DependencyRequest;
import org.eclipse.aether.resolution.DependencyResolutionException;
import org.eclipse.aether.util.artifact.JavaScopes;
import org.eclipse.aether.util.filter.DependencyFilterUtils;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;

import fr.jetoile.hadoopunit.exception.BootstrapException;

public class HadoopStandaloneBootstrap {

    final private static Logger LOGGER = LoggerFactory.getLogger(HadoopStandaloneBootstrap.class);
    static private Configuration configuration;
    static private Configuration hadoopUnitConfiguration;
    static private List<Component> componentsToStart = new ArrayList<>();
    static private List<ComponentProperties> componentsToStop = new ArrayList<>();
    static private List<ComponentProperties> componentsProperty = new ArrayList<>();

    /**
     * code from org.apache.maven.cli.MavenCli.container(CliRequest)
     * 
     * cf also https://github.com/igor-suhorukov/mvn-classloader/blob/master/dropship/src/main/java/com/github/smreed/dropship/ClassLoaderBuilder.java
     * 
     */
    private static PlexusContainer mvnContainer() throws Exception {
		ILoggerFactory slf4jLoggerFactory = LoggerFactory.getILoggerFactory();
		Slf4jLoggerManager plexusLoggerManager = new Slf4jLoggerManager();
		
		ClassWorld classWorld = new ClassWorld("plexus.core", Thread.currentThread().getContextClassLoader());

        DefaultPlexusContainer container = null;

        ContainerConfiguration cc = new DefaultContainerConfiguration()
            .setClassWorld(classWorld)
            // .setRealm( setupContainerRealm( cliRequest ) )
            .setClassPathScanning( PlexusConstants.SCANNING_INDEX )
            .setAutoWiring( true )
            .setName( "maven" );

        container = new DefaultPlexusContainer( cc, new AbstractModule() {
            protected void configure() {
                bind( ILoggerFactory.class ).toInstance( slf4jLoggerFactory );
            }
        });

        // NOTE: To avoid inconsistencies, we'll use the TCCL exclusively for lookups
        container.setLookupRealm( null );

        container.setLoggerManager( plexusLoggerManager );

        Thread.currentThread().setContextClassLoader( container.getContainerRealm() );

        return container;
    }
    
	public static DefaultRepositorySystemSession newRepositorySystemSession(PlexusContainer container, RepositorySystem system) throws Exception {
		DefaultRepositorySystemSession session = MavenRepositorySystemUtils.newSession();

		//?? LocalRepositoryManager localRepoManager = container.lookup(LocalRepositoryManager.class);
		//.. LocalRepository localRepo = container.lookup(LocalRepository.class);
		//?? DefaultRepositorySystemSessionFactory sessionFactory = container.lookup(DefaultRepositorySystemSessionFactory.class);
		//?? LocalRepositoryManagerFactory localRepoFactory = container.lookup(LocalRepositoryManagerFactory.class);
		
		String localRepositoryDir = hadoopUnitConfiguration.getString("maven.local.repo");
		
		if (localRepositoryDir != null && !new File(localRepositoryDir).exists()) {
			LOGGER.info("maven.local.repo: " + localRepositoryDir + " NOT FOUND!! try resolving it..");
			localRepositoryDir = null;
		}
		
		if (localRepositoryDir == null) {
			String mavenHome = System.getenv("MAVEN_HOME");
			if (mavenHome == null) {
				// find in PATH ..
				String path = System.getenv("PATH");
				String pathSep = System.getProperty("path.separator");
				String[] pathElts = path.split(pathSep);
				for(String pathElt : pathElts) {
					File pathDir = new File(pathElt);
					if (new File(pathDir, "mvn").exists()) {
						mavenHome = pathDir.getParentFile().getAbsolutePath();
						LOGGER.info("found mvn in PATH => " + mavenHome);
						break;
					}
				}
			}
			File globalSettingsFile = new File(mavenHome + "/conf/settings.xml");
			if (!globalSettingsFile.exists()) {
				LOGGER.error("maven global settings.xml file not found : " + globalSettingsFile);
			}
			
			File userSettingsFile = new File(System.getProperty("user.home") + "/.m2/settings.xml");
			if (!userSettingsFile.exists()) {
				LOGGER.info("maven user settings.xml override file not found : " + userSettingsFile);
			}
			
			SettingsBuilder defaultSettingsBuilder = container.lookup(SettingsBuilder.class);
			SettingsBuildingRequest settingsRequest = new DefaultSettingsBuildingRequest();
			settingsRequest.setGlobalSettingsFile(globalSettingsFile);
			settingsRequest.setUserSettingsFile(userSettingsFile);
			Settings settings = defaultSettingsBuilder.build(settingsRequest).getEffectiveSettings();
			
			localRepositoryDir = settings.getLocalRepository();
			if (localRepositoryDir == null) {
				// no set .. using default!
				localRepositoryDir = System.getProperty("user.home") + "/.m2/repository";
			}
			LOGGER.info("found mvn localRepository => " + localRepositoryDir);
		}
		
		LocalRepository localRepo = new LocalRepository(localRepositoryDir);
		// LocalRepository localRepo = new LocalRepository(hadoopUnitConfiguration.getString("maven.local.repo"));
		session.setLocalRepositoryManager(system.newLocalRepositoryManager(session, localRepo));

		boolean debug = Boolean.parseBoolean(hadoopUnitConfiguration.getString("maven.debug"));
		if (debug) {
			session.setTransferListener(new ConsoleTransferListener());
			session.setRepositoryListener(new ConsoleRepositoryListener());
		}

		return session;
	}

    
//    public static RepositorySystem newRepositorySystem() {
//        DefaultServiceLocator locator = MavenRepositorySystemUtils.newServiceLocator();
//        locator.addService(RepositoryConnectorFactory.class, BasicRepositoryConnectorFactory.class);
//        locator.addService(TransporterFactory.class, FileTransporterFactory.class);
//        locator.addService(TransporterFactory.class, HttpTransporterFactory.class);
//
//        locator.setErrorHandler(new DefaultServiceLocator.ErrorHandler() {
//            @Override
//            public void serviceCreationFailed(Class<?> type, Class<?> impl, Throwable exception) {
//                exception.printStackTrace();
//            }
//        });
//
//        return locator.getService(RepositorySystem.class);
//    }
//
//    public static DefaultRepositorySystemSession newRepositorySystemSession(RepositorySystem system) {
//        DefaultRepositorySystemSession session = MavenRepositorySystemUtils.newSession();
//
//        LocalRepository localRepo = new LocalRepository(hadoopUnitConfiguration.getString("maven.local.repo"));
//        session.setLocalRepositoryManager(system.newLocalRepositoryManager(session, localRepo));
//
//        boolean debug = Boolean.parseBoolean(hadoopUnitConfiguration.getString("maven.debug"));
//        if (debug) {
//            session.setTransferListener(new ConsoleTransferListener());
//            session.setRepositoryListener(new ConsoleRepositoryListener());
//        }
//
//        return session;
//    }
//
//    public static List<RemoteRepository> newRepositories(RepositorySystem system, RepositorySystemSession session) {
//        return new ArrayList<>(Arrays.asList(newCentralRepository()));
//    }
//
//    private static RemoteRepository newCentralRepository() {
//        return new RemoteRepository.Builder("central", "default", hadoopUnitConfiguration.getString("maven.central.repo")).build();
//    }

    public static void main(String[] args) throws Exception {
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

        PlexusContainer mvnContainer = mvnContainer();
        RepositorySystem system = mvnContainer.lookup(RepositorySystem.class);
        DefaultRepositorySystemSession session = newRepositorySystemSession(mvnContainer, system);
        
//        RepositorySystem system = newRepositorySystem();
//        DefaultRepositorySystemSession session = newRepositorySystemSession(system);
        
        DependencyFilter classpathFilter = DependencyFilterUtils.classpathFilter(JavaScopes.RUNTIME);

        componentsToStart.stream().forEach(c -> {
            Artifact artifact = new DefaultArtifact(hadoopUnitConfiguration.getString(c.getArtifactKey()));
            CollectRequest collectRequest = new CollectRequest();
            collectRequest.setRoot(new Dependency(artifact, JavaScopes.RUNTIME));
//            collectRequest.setRepositories(newRepositories(system, session));

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
                HadoopUtils.printColorLine(System.out, GREEN, "\t\t - " + name + " " + prop);
            }
        });
        System.out.println();
    }

    private static ComponentProperties loadAndRun(String c, String className, List<File> artifacts) {
        List<URL> urls = new ArrayList();

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
        Class mainClass = null;
        try {
            mainClass = classloader.loadClass(className);
        } catch (ClassNotFoundException e) {
            LOGGER.error("unable to load class", e);
        }
        Method main = null;


        try {
            Thread.currentThread().setContextClassLoader(classloader);

            Object o = mainClass.getConstructor(URL.class).newInstance(HadoopStandaloneBootstrap.class.getClassLoader().getResource(HadoopUnitConfig.DEFAULT_PROPS_FILE));
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
