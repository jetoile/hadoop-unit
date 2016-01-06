package fr.jetoile.sample.component;


import fr.jetoile.sample.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static junit.framework.TestCase.assertNotNull;
import static org.fest.assertions.Assertions.assertThat;

public class SolrCloudBootstrapTest {
    static private Logger LOGGER = LoggerFactory.getLogger(SolrCloudBootstrapTest.class);

    static private Configuration configuration;

    private static Bootstrap solr;
    private static Bootstrap zookeeper;

    @BeforeClass
    public static void setup() throws Exception {
        try {
            configuration = new PropertiesConfiguration("default.properties");
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }
        zookeeper = ZookeeperBootstrap.INSTANCE.start();
        solr = SolrCloudBootstrap.INSTANCE.start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        solr.stop();
        zookeeper.stop();
    }


    @Test
    public void solrCloudShouldStart() throws IOException, SolrServerException, KeeperException, InterruptedException {

        String collectionName = configuration.getString(SolrCloudBootstrap.SOLR_COLLECTION_NAME);

//        String zkHostString = configuration.getString(ConfigVars.ZOOKEEPER_HOST_KEY) + ":" + configuration.getInt(ConfigVars.ZOOKEEPER_PORT_KEY);
//        CloudSolrClient client = new CloudSolrClient(zkHostString);
        CloudSolrClient client = ((SolrCloudBootstrap) solr).getClient();

        for (int i = 0; i < 1000; ++i) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("cat", "book");
            doc.addField("id", "book-" + i);
            doc.addField("name", "The Legend of the Hobbit part " + i);
            client.add(collectionName, doc);
            if (i % 100 == 0) client.commit(collectionName);  // periodically flush
        }
        client.commit("collection1");

        SolrDocument collection1 = client.getById(collectionName, "book-1");

        assertNotNull(collection1);

        assertThat(collection1.getFieldValue("name")).isEqualTo("The Legend of the Hobbit part 1");


        client.close();
    }

}
