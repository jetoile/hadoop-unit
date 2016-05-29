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
package fr.jetoile.hadoopunit.component;

import fr.jetoile.hadoopunit.Component;
import fr.jetoile.hadoopunit.HadoopBootstrap;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.HadoopUtils;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import fr.jetoile.hadoopunit.exception.NotFoundServiceException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

public class SolrCloudBootstrap implements Bootstrap {
    final public static String NAME = Component.SOLRCLOUD.name();

    public static final String SOLR_DIR_KEY = "solr.dir";
    public static final String SOLR_COLLECTION_NAME = "solr.collection.name";
    public static final String SOLR_PORT = "solr.cloud.port";
    final private Logger LOGGER = LoggerFactory.getLogger(SolrCloudBootstrap.class);

    public static final int TIMEOUT = 10000;

    private State state = State.STOPPED;

    private Configuration configuration;
    private JettySolrRunner solrServer;
    private String solrDirectory;
    private String solrCollectionName;
    private int solrPort;
    private String zkHostString;


    public SolrCloudBootstrap() {
        if (solrServer == null) {
            try {
                loadConfig();
            } catch (BootstrapException e) {
                LOGGER.error("unable to load configuration", e);
            }
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getProperties() {
        return "[" +
                "zh:" + zkHostString +
                ", port:" + solrPort +
                ", collection:" + solrCollectionName +
                "]";
    }

    private void build() {
        File solrXml = null;
        try {
            solrXml = new File(configuration.getClass().getClassLoader().getResource(solrDirectory + "/solr.xml").toURI());
        } catch (URISyntaxException e) {
            LOGGER.error("unable to instanciate SolrCloudBootstrap", e);
        }
        File solrHomeDir = solrXml.getParentFile();

        String context = "/solr";
        solrServer = new JettySolrRunner(solrHomeDir.getAbsolutePath(), context, solrPort);

    }

    private void loadConfig() throws BootstrapException {
        HadoopUtils.setHadoopHome();
        try {
            configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }
        solrDirectory = configuration.getString(SOLR_DIR_KEY);
        solrCollectionName = configuration.getString(SOLR_COLLECTION_NAME);
        solrPort = configuration.getInt(SOLR_PORT);
        zkHostString = configuration.getString(HadoopUnitConfig.ZOOKEEPER_HOST_KEY) + ":" + configuration.getInt(HadoopUnitConfig.ZOOKEEPER_PORT_KEY);

    }

    @Override
    public Bootstrap start() {
        if (state == State.STOPPED) {
            state = State.STARTING;
            LOGGER.info("{} is starting", this.getClass().getName());

            build();
            populateZkWithCollectionInfo();

            try {
                solrServer.start();
            } catch (Exception e) {
                LOGGER.error("unable to add SolrCloudServer", e);
            }

//        final ModifiableSolrParams params = new ModifiableSolrParams();
//        params.set(CoreAdminParams.ACTION, CollectionParams.CollectionAction.CREATE.name());
//        params.set(CoreAdminParams.NAME, solrCollectionName);
//        params.set("numShards", 1);
//        params.set("replicationFactor", 1);
//        params.set("collection.configName", solrCollectionName);
//
//        final QueryRequest request = new QueryRequest(params);
//        request.setPath("/admin/collections");
//        getClient().request(request);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            state = State.STARTED;
            LOGGER.info("{} is started", this.getClass().getName());
        }
        return this;
    }

    private void populateZkWithCollectionInfo() {
        System.setProperty("zkHost", zkHostString);

        try {

            URI solrDirectoryFile = solrServer.getClass().getClassLoader().getResource(solrDirectory + "/collection1/conf").toURI();

            try (SolrZkClient zkClient = new SolrZkClient(zkHostString, TIMEOUT, 45000, null)) {
                ZkConfigManager manager = new ZkConfigManager(zkClient);
                manager.uploadConfigDir(Paths.get(solrDirectoryFile), solrCollectionName);
            }

//            CuratorFramework client = CuratorFrameworkFactory.newClient(zkHostString, new RetryOneTime(300));
//            client.start();
//            File schema = new File(solrServer.getClass().getClassLoader().getResource(solrDirectory + "/collection1/conf/schema.xml").toURI());
//            File solrConfig = new File(solrServer.getClass().getClassLoader().getResource(solrDirectory + "/collection1/conf/solrconfig.xml").toURI());
//            File stopwords = new File(solrServer.getClass().getClassLoader().getResource(solrDirectory + "/collection1/conf/stopwords.txt").toURI());
//            File synonyms = new File(solrServer.getClass().getClassLoader().getResource(solrDirectory + "/collection1/conf/synonyms.txt").toURI());
//            File stopwords_en = new File(solrServer.getClass().getClassLoader().getResource(solrDirectory + "/collection1/conf/lang/stopwords_en.txt").toURI());
//            File protwords = new File(solrServer.getClass().getClassLoader().getResource(solrDirectory + "/collection1/conf/protwords.txt").toURI());
//            File currency = new File(solrServer.getClass().getClassLoader().getResource(solrDirectory + "/collection1/conf/currency.xml").toURI());
//
//            byte[] schemaContent = Files.readAllBytes(schema.toPath());
//            byte[] solrConfigContent = Files.readAllBytes(solrConfig.toPath());
//            byte[] stopwordsContent = Files.readAllBytes(stopwords.toPath());
//            byte[] synonymsContent = Files.readAllBytes(synonyms.toPath());
//            byte[] stopwordsEnContent = Files.readAllBytes(stopwords_en.toPath());
//            byte[] protwordsContent = Files.readAllBytes(protwords.toPath());
//            byte[] currencyContent = Files.readAllBytes(currency.toPath());
//
//            client.create().forPath("/configs");
//            client.create().forPath("/configs/collection1");
//            client.create().forPath("/configs/collection1/lang");
//            client.create().forPath("/configs/collection1/solrconfig.xml", solrConfigContent);
//            client.create().forPath("/configs/collection1/schema.xml", schemaContent);
//            client.create().forPath("/configs/collection1/stopwords.txt", stopwordsContent);
//            client.create().forPath("/configs/collection1/synonyms.txt", synonymsContent);
//            client.create().forPath("/configs/collection1/lang/stopwords_en.txt", stopwordsEnContent);
//            client.create().forPath("/configs/collection1/protwords.txt", protwordsContent);
//            client.create().forPath("/configs/collection1/currency.xml", currencyContent);
//
//            client.close();

        } catch (URISyntaxException | IOException e) {
            LOGGER.error("unable to populate zookeeper", e);
        }
    }

    @Override
    public Bootstrap stop() {
        if (state == State.STARTED) {
            state = State.STOPPING;
            LOGGER.info("{} is stopping", this.getClass().getName());
            try {
                System.clearProperty("zkHost");

                this.solrServer.stop();
            } catch (Exception e) {
                LOGGER.error("unable to stop SolrCloudBootstrap", e);
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

    public CloudSolrClient getClient() {
        return new CloudSolrClient(zkHostString);
    }

    final public static void main(String... args) throws NotFoundServiceException {

        HadoopBootstrap bootstrap = HadoopBootstrap.INSTANCE;

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                bootstrap.stopAll();
            }
        });

        bootstrap.add(Component.SOLRCLOUD).startAll();
    }
}
