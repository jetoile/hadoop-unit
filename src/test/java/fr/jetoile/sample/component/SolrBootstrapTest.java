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
    private static Bootstrap zookeeper;

    @BeforeClass
    public static void setup() throws Exception {
        try {
            configuration = new PropertiesConfiguration("default.properties");
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }

        zookeeper = ZookeeperBootstrap.INSTANCE.start();
        solr = SolrBootstrap.INSTANCE.start();

        String zookeeperConnectionString = configuration.getString(ConfigVars.ZOOKEEPER_HOST_KEY) + ":" + configuration.getInt(ConfigVars.ZOOKEEPER_PORT_KEY);
        System.setProperty("zkHost", zookeeperConnectionString);

        CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperConnectionString, new RetryOneTime(300));
        client.start();

        String solrDirectory = configuration.getString(SolrBootstrap.SOLR_DIR_KEY);
        File schema = new File(zookeeper.getClass().getClassLoader().getResource(solrDirectory + "/collection1/conf/schema.xml").toURI());
        File solrConfig = new File(zookeeper.getClass().getClassLoader().getResource(solrDirectory + "/collection1/conf/solrconfig.xml").toURI());
        File stopwords = new File(zookeeper.getClass().getClassLoader().getResource(solrDirectory + "/collection1/conf/stopwords.txt").toURI());
        File synonyms = new File(zookeeper.getClass().getClassLoader().getResource(solrDirectory + "/collection1/conf/synonyms.txt").toURI());
        File stopwords_en = new File(zookeeper.getClass().getClassLoader().getResource(solrDirectory + "/collection1/conf/lang/stopwords_en.txt").toURI());
        File protwords = new File(zookeeper.getClass().getClassLoader().getResource(solrDirectory + "/collection1/conf/protwords.txt").toURI());
        File currency = new File(zookeeper.getClass().getClassLoader().getResource(solrDirectory + "/collection1/conf/currency.xml").toURI());

        byte[] schemaContent = Files.readAllBytes(schema.toPath());
        byte[] solrConfigContent = Files.readAllBytes(solrConfig.toPath());
        byte[] stopwordsContent = Files.readAllBytes(stopwords.toPath());
        byte[] synonymsContent = Files.readAllBytes(synonyms.toPath());
        byte[] stopwordsEnContent = Files.readAllBytes(stopwords_en.toPath());
        byte[] protwordsContent = Files.readAllBytes(protwords.toPath());
        byte[] currencyContent = Files.readAllBytes(currency.toPath());

        client.create().forPath("/clusterstate.json", "{}".getBytes());
        client.create().forPath("/configs");
        client.create().forPath("/configs/collection1");
        client.create().forPath("/configs/collection1/lang");
        client.create().forPath("/configs/collection1/solrconfig.xml", solrConfigContent);
        client.create().forPath("/configs/collection1/schema.xml", schemaContent);
        client.create().forPath("/configs/collection1/stopwords.txt", stopwordsContent);
        client.create().forPath("/configs/collection1/synonyms.txt", synonymsContent);
        client.create().forPath("/configs/collection1/lang/stopwords_en.txt", stopwordsEnContent);
        client.create().forPath("/configs/collection1/protwords.txt", protwordsContent);
        client.create().forPath("/configs/collection1/currency.xml", currencyContent);

        client.close();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        solr.stop();
        zookeeper.stop();
    }


    @Test
    public void solrShouldStart() throws Exception {

        EmbeddedSolrServer client = ((SolrBootstrap) solr).getClient();

        injectIntoSolr(client);

        SolrDocument collection1 = client.getById("collection1_shard1_replica1", "book-1");

        assertNotNull(collection1);

        assertThat(collection1.getFieldValue("name")).isEqualTo("The Legend of the Hobbit part 1");
    }

    @Test
    @Ignore
    public void test2() throws IOException, SolrServerException {
        String zkHostString = configuration.getString(ConfigVars.ZOOKEEPER_HOST_KEY) + ":" + configuration.getInt(ConfigVars.ZOOKEEPER_PORT_KEY);
        SolrClient client = new CloudSolrClient(zkHostString);

        for (int i = 0; i < 1000; ++i) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("cat", "book");
            doc.addField("id", "book-" + i);
            doc.addField("name", "The Legend of the Hobbit part " + i);
            client.add(doc);
            if (i % 100 == 0) client.commit();  // periodically flush
        }
        client.commit();

        SolrDocument collection1 = client.getById("collection1_shard1_replica1", "book-1");

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
