package fr.jetoile.sample.component;


import com.github.sakserv.minicluster.config.ConfigVars;
import fr.jetoile.sample.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static junit.framework.TestCase.assertNotNull;
import static org.fest.assertions.Assertions.assertThat;

public class SolrCloudBootstrapTest {
    static private Logger LOGGER = LoggerFactory.getLogger(SolrCloudBootstrapTest.class);

    static private Configuration configuration;

    private static Bootstrap solr;
    private static Bootstrap zookeeper;
    private static JettySolrRunner jettySolr;

    @BeforeClass
    public static void setup() throws Exception {
        try {
            configuration = new PropertiesConfiguration("default.properties");
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }
        zookeeper = ZookeeperBootstrap.INSTANCE.start();


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


        File solrXml = new File(configuration.getClass().getClassLoader().getResource(solrDirectory + "/solr.xml").toURI());
        File solrHomeDir = solrXml.getParentFile();

        int port = 8983;
        String context = "/solr";
        jettySolr = new JettySolrRunner(solrHomeDir.getAbsolutePath(), context, port);
//
//        boolean waitUntilTheSolrWebAppHasStarted = true;
        jettySolr.start();

    }

    @AfterClass
    public static void tearDown() throws Exception {
        jettySolr.stop();
        zookeeper.stop();
    }



    @Test
    public void test2() throws IOException, SolrServerException, KeeperException, InterruptedException {

        String zkHostString = configuration.getString(ConfigVars.ZOOKEEPER_HOST_KEY) + ":" + configuration.getInt(ConfigVars.ZOOKEEPER_PORT_KEY);
        CloudSolrClient client = new CloudSolrClient(zkHostString);

        String configName = "collection1";
        String name = "collection1";

        final ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CoreAdminParams.ACTION, CollectionParams.CollectionAction.CREATE.name());
        params.set(CoreAdminParams.NAME, name);
        params.set("numShards", 1);
        params.set("replicationFactor", 1);
        params.set("collection.configName", configName);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("====================================");

        for (int i = 0; i < 1000; ++i) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("cat", "book");
            doc.addField("id", "book-" + i);
            doc.addField("name", "The Legend of the Hobbit part " + i);
//            client.add(configuration.getString(SolrBootstrap.SOLR_COLLECTION_INTERNAL_NAME), doc);
            client.add("collection1", doc);
//            if (i % 100 == 0) client.commit(configuration.getString(SolrBootstrap.SOLR_COLLECTION_INTERNAL_NAME));  // periodically flush
            if (i % 100 == 0) client.commit("collection1");  // periodically flush
        }
        client.commit("collection1");

        SolrDocument collection1 = client.getById("collection1", "book-1");

        assertNotNull(collection1);

        assertThat(collection1.getFieldValue("name")).isEqualTo("The Legend of the Hobbit part 1");
    }

}
