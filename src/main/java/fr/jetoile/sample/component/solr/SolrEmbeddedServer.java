package fr.jetoile.sample.component.solr;

import fr.jetoile.sample.exception.BootstrapException;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SolrEmbeddedServer {

    private static final String SOLR_HOME_SYSTEM_PROPERTY = "solr.solr.home";

    private String solrHome;
    private EmbeddedSolrServer solrServer;
    private String solrCollection;

    protected SolrEmbeddedServer() {

    }


    public SolrEmbeddedServer(String solrHome, String solrCollection) throws ParserConfigurationException, IOException, SAXException {
//        Assert.hasText(solrHome);
        this.solrHome = solrHome;
        this.solrCollection = solrCollection;
    }

    public EmbeddedSolrServer getSolrClient() throws BootstrapException {
        if (this.solrServer == null) {
            initSolrServer();
        }
        return this.solrServer;
    }

    protected void initSolrServer() throws BootstrapException {
        try {
            this.solrServer = createPathConfiguredSolrServer(this.solrHome);
        } catch (ParserConfigurationException e) {
            throw new BootstrapException(EmbeddedSolrServer.class, e.getMessage(), e);
        } catch (IOException e) {
            throw new BootstrapException(EmbeddedSolrServer.class, e.getMessage(), e);
        } catch (SAXException e) {
            throw new BootstrapException(EmbeddedSolrServer.class, e.getMessage(), e);
        }
    }

    public final EmbeddedSolrServer createPathConfiguredSolrServer(String path) throws ParserConfigurationException,
            IOException, SAXException, BootstrapException {
        String solrHomeDirectory = System.getProperty(SOLR_HOME_SYSTEM_PROPERTY);

        if (StringUtils.isBlank(solrHomeDirectory)) {
            solrHomeDirectory = getURL(path).getPath();
            if (System.getProperty("os.name").startsWith("Windows")) {
                solrHomeDirectory = solrHomeDirectory.substring(1);
            }
        }

        solrHomeDirectory = URLDecoder.decode(solrHomeDirectory, "utf-8");
        return new EmbeddedSolrServer(createCoreContainer(solrHomeDirectory), solrCollection);
    }

    public static ClassLoader getDefaultClassLoader() {
        ClassLoader cl = null;

        try {
            cl = Thread.currentThread().getContextClassLoader();
        } catch (Throwable var3) {
            ;
        }

        if(cl == null) {
            cl = SolrEmbeddedServer.class.getClassLoader();
            if(cl == null) {
                try {
                    cl = ClassLoader.getSystemClassLoader();
                } catch (Throwable var2) {
                    ;
                }
            }
        }

        return cl;
    }

    public static URL getURL(String resourceLocation) throws FileNotFoundException {
//        Assert.notNull(resourceLocation, "Resource location must not be null");
        if(resourceLocation.startsWith("classpath:")) {
            String ex = resourceLocation.substring("classpath:".length());
            ClassLoader ex2 = getDefaultClassLoader();
            URL url = ex2 != null?ex2.getResource(ex):ClassLoader.getSystemResource(ex);
            if(url == null) {
                String description = "class path resource [" + ex + "]";
                throw new FileNotFoundException(description + " cannot be resolved to URL because it does not exist");
            } else {
                return url;
            }
        } else {
            try {
                return new URL(resourceLocation);
            } catch (MalformedURLException var6) {
                try {
                    return (new File(resourceLocation)).toURI().toURL();
                } catch (MalformedURLException var5) {
                    throw new FileNotFoundException("Resource location [" + resourceLocation + "] is neither a URL not a well-formed file path");
                }
            }
        }
    }

    public static boolean hasConstructor(Class<?> clazz, Class... paramTypes) {
        return getConstructorIfAvailable(clazz, paramTypes) != null;
    }

    public static <T> Constructor<T> getConstructorIfAvailable(Class<T> clazz, Class... paramTypes) {
//        Assert.notNull(clazz, "Class must not be null");

        try {
            return clazz.getConstructor(paramTypes);
        } catch (NoSuchMethodException var3) {
            return null;
        }
    }

    private CoreContainer createCoreContainer(String solrHomeDirectory) throws BootstrapException {
        File solrXmlFile = new File(solrHomeDirectory + "/solr.xml");
        if (hasConstructor(CoreContainer.class, String.class, File.class)) {
            return createCoreContainerViaConstructor(solrHomeDirectory, solrXmlFile);
        }
        return createCoreContainer(solrHomeDirectory, solrXmlFile);
    }

    /**
     * Create {@link CoreContainer} via its constructor (Solr 3.6.0 - 4.3.1)
     *
     * @param solrHomeDirectory
     * @param solrXmlFile
     * @return
     */
    private CoreContainer createCoreContainerViaConstructor(String solrHomeDirectory, File solrXmlFile) throws BootstrapException {
        Constructor<CoreContainer> constructor = getConstructorIfAvailable(CoreContainer.class, String.class,
                File.class);
        return instantiateClass(constructor, solrHomeDirectory, solrXmlFile);
    }

    public static <T> T instantiateClass(Constructor<T> ctor, Object... args) throws BootstrapException {
//        Assert.notNull(ctor, "Constructor must not be null");

        try {
            makeAccessible(ctor);
            return ctor.newInstance(args);
        } catch (InstantiationException var3) {
            throw new BootstrapException(ctor.getDeclaringClass(), "Is it an abstract class?", var3);
        } catch (IllegalAccessException var4) {
            throw new BootstrapException(ctor.getDeclaringClass(), "Is the constructor accessible?", var4);
        } catch (IllegalArgumentException var5) {
            throw new BootstrapException(ctor.getDeclaringClass(), "Illegal arguments for constructor", var5);
        } catch (InvocationTargetException var6) {
            throw new BootstrapException(ctor.getDeclaringClass(), "Constructor threw exception", var6.getTargetException());
        }
    }

    public static void makeAccessible(Constructor<?> ctor) {
        if((!Modifier.isPublic(ctor.getModifiers()) || !Modifier.isPublic(ctor.getDeclaringClass().getModifiers())) && !ctor.isAccessible()) {
            ctor.setAccessible(true);
        }

    }

    /**
     * Create {@link CoreContainer} for Solr version 4.4+
     *
     * @param solrHomeDirectory
     * @param solrXmlFile
     * @return
     */
    private CoreContainer createCoreContainer(String solrHomeDirectory, File solrXmlFile) {
        return CoreContainer.createAndLoad(solrHomeDirectory, solrXmlFile);
    }

    public void shutdownSolrServer() {
        if (this.solrServer != null && solrServer.getCoreContainer() != null) {
            solrServer.getCoreContainer().shutdown();
        }
    }

    public List<String> getCores() {
        if (this.solrServer != null && solrServer.getCoreContainer() != null) {
            return new ArrayList<>(this.solrServer.getCoreContainer().getCoreNames());
        }
        return Collections.emptyList();
    }

}