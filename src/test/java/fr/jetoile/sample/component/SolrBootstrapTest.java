package fr.jetoile.sample.component;


import com.github.sakserv.minicluster.config.ConfigVars;
import fr.jetoile.sample.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static junit.framework.TestCase.assertNotNull;
import static org.fest.assertions.Assertions.assertThat;

public class SolrBootstrapTest {
    static private Logger LOGGER = LoggerFactory.getLogger(SolrBootstrapTest.class);

    static private Configuration configuration;

    private static Bootstrap solr;

    @BeforeClass
    public static void setup() throws Exception {
        try {
            configuration = new PropertiesConfiguration("default.properties");
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }
        solr = SolrBootstrap.INSTANCE.start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        solr.stop();
    }


    @Test
    public void solrShouldStart() throws Exception {

        EmbeddedSolrServer client = ((SolrBootstrap) solr).getClient();

        injectIntoSolr(client);

        SolrDocument collection1 = client.getById(configuration.getString(SolrBootstrap.SOLR_COLLECTION_INTERNAL_NAME), "book-1");

        assertNotNull(collection1);

        assertThat(collection1.getFieldValue("name")).isEqualTo("The Legend of the Hobbit part 1");
    }

    private void injectIntoSolr(EmbeddedSolrServer client) throws SolrServerException, IOException {
        for (int i = 0; i < 1000; ++i) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("cat", "book");
            doc.addField("id", "book-" + i);
            doc.addField("name", "The Legend of the Hobbit part " + i);
            client.add(doc);
            if (i % 100 == 0) client.commit();  // periodically flush
        }
        client.commit();
    }
}
