package fr.jetoile.sample.component;

import fr.jetoile.sample.HadoopUtils;
import fr.jetoile.sample.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.net.URLDecoder;

public enum SolrBootstrap implements Bootstrap {
    INSTANCE;

    public static final String SOLR_DIR_KEY = "solr.dir";
    public static final String SOLR_COLLECTION_INTERNAL_NAME = "solr.collection.internal.name";
    final private Logger LOGGER = LoggerFactory.getLogger(SolrBootstrap.class);

    private State state = State.STOPPED;

    private Configuration configuration;
    private EmbeddedSolrServer solrServer;
    private String solrDirectory;
    private String solrCollectionInternalName;


    SolrBootstrap() {
        if (solrServer == null) {
            try {
                loadConfig();
            } catch (BootstrapException e) {
                LOGGER.error("unable to load configuration", e);
            }
        }
    }

    private void loadConfig() throws BootstrapException {
        HadoopUtils.setHadoopHome();
        try {
            configuration = new PropertiesConfiguration("default.properties");
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }
        solrDirectory = configuration.getString(SOLR_DIR_KEY);
        solrCollectionInternalName = configuration.getString(SOLR_COLLECTION_INTERNAL_NAME);

    }

    @Override
    public Bootstrap start() {
        if (state == State.STOPPED) {
            state = State.STARTING;
            LOGGER.info("{} is starting", this.getClass().getName());

            try {
                String path = getClass().getClassLoader().getResource(solrDirectory).getFile();
                if (System.getProperty("os.name").startsWith("Windows")) {
                    path = path.substring(1);
                }

                this.solrServer = createPathConfiguredSolrServer(path);
            } catch (ParserConfigurationException | IOException | SAXException | BootstrapException e) {
                LOGGER.error("unable to bootstrap solr", e);
            }
            state = State.STARTED;
            LOGGER.info("{} is started", this.getClass().getName());
        }
        return this;
    }

    @Override
    public Bootstrap stop() {
        if (state == State.STARTED) {
            state = State.STOPPING;
            LOGGER.info("{} is stopping", this.getClass().getName());
            if (this.solrServer != null && solrServer.getCoreContainer() != null) {
                solrServer.getCoreContainer().shutdown();
            }
            state = State.STOPPED;
            LOGGER.info("{} is stopped", this.getClass().getName());
        }
        return this;
    }

    @Override
    public org.apache.hadoop.conf.Configuration getConfiguration() {
        throw new UnsupportedOperationException("the method getConfiguration can not be called on SolrBootstrap");
    }

    public EmbeddedSolrServer getClient() {
        return solrServer;
    }


    private final EmbeddedSolrServer createPathConfiguredSolrServer(String path) throws ParserConfigurationException,
            IOException, SAXException, BootstrapException {

        String solrHomeDirectory = path;
        if (System.getProperty("os.name").startsWith("Windows")) {
            solrHomeDirectory = solrHomeDirectory.substring(1);
        }

        solrHomeDirectory = URLDecoder.decode(solrHomeDirectory, "utf-8");
        return new EmbeddedSolrServer(createCoreContainer(solrHomeDirectory), solrCollectionInternalName);
    }

    private CoreContainer createCoreContainer(String solrHomeDirectory) throws BootstrapException {
        File solrXmlFile = new File(solrHomeDirectory + "/solr.xml");
        return createCoreContainer(solrHomeDirectory, solrXmlFile);
    }


    private CoreContainer createCoreContainer(String solrHomeDirectory, File solrXmlFile) {
        return CoreContainer.createAndLoad(solrHomeDirectory, solrXmlFile);
    }
}
