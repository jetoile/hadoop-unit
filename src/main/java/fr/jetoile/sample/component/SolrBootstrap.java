package fr.jetoile.sample.component;

import fr.jetoile.sample.exception.BootstrapException;
import fr.jetoile.sample.component.solr.SolrEmbeddedServer;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

public enum SolrBootstrap implements Bootstrap {
    INSTANCE;

    public static final String SOLR_DIR_KEY = "solr.dir";
    public static final String SOLR_COLLECTION_KEY = "solr.collection";
    final private Logger LOGGER = LoggerFactory.getLogger(SolrBootstrap.class);

    private Configuration configuration;
    private SolrEmbeddedServer solrLocalCluster;
    private String solrDirectory;
    private String solrCollection;


    SolrBootstrap() {
        if (solrLocalCluster == null) {
            try {
                loadConfig();
            } catch (BootstrapException e) {
                LOGGER.error("unable to load configuration", e);
            }
            build();
        }
    }

    private void build() {
        try {
            String path = getClass().getClassLoader().getResource(solrDirectory).getFile();
            if (System.getProperty("os.name").startsWith("Windows")) {
                path = path.substring(1);
            }
            solrLocalCluster = new SolrEmbeddedServer(path, solrCollection);
        } catch (ParserConfigurationException | IOException | SAXException e) {
            LOGGER.error("unable to bootstrap solr", e);
        }
    }

    private void loadConfig() throws BootstrapException {
        try {
            configuration = new PropertiesConfiguration("default.properties");
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }
        solrDirectory = configuration.getString(SOLR_DIR_KEY);
        solrCollection = configuration.getString(SOLR_COLLECTION_KEY);

    }

    @Override
    public Bootstrap start() {
        return this;
    }

    @Override
    public Bootstrap stop() {
        solrLocalCluster.shutdownSolrServer();
        return this;
    }

    @Override
    public org.apache.hadoop.conf.Configuration getConfiguration() {
        throw new UnsupportedOperationException("the method getConfiguration can not be called on SolrBootstrap");
    }

    public EmbeddedSolrServer getClient() {
        try {
            return solrLocalCluster.getSolrClient();
        } catch (BootstrapException e) {
            LOGGER.error("unable to get Solr Client", e);
        }
        return null;
    }
}
