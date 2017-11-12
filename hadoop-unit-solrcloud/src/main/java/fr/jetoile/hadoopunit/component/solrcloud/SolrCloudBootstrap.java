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
package fr.jetoile.hadoopunit.component.solrcloud;

import fr.jetoile.hadoopunit.Component;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.HadoopUtils;
import fr.jetoile.hadoopunit.component.Bootstrap;
import fr.jetoile.hadoopunit.component.State;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import org.apache.commons.configuration.*;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Map;

public class SolrCloudBootstrap implements Bootstrap {
    final public static String NAME = Component.SOLRCLOUD.name();

    final private static Logger LOGGER = LoggerFactory.getLogger(SolrCloudBootstrap.class);

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
                configuration = HadoopUtils.INSTANCE.loadConfigFile(null);
                loadConfig();
            } catch (BootstrapException e) {
                LOGGER.error("unable to load configuration", e);
            }
        }
    }

    public SolrCloudBootstrap(URL url) {
        if (solrServer == null) {
            try {
                configuration = HadoopUtils.INSTANCE.loadConfigFile(url);
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
        return "\n \t\t\t zh:" + zkHostString +
                "\n \t\t\t port:" + solrPort +
                "\n \t\t\t collection:" + solrCollectionName;
    }

    private void build() {
        File solrXml = null;
        try {

            URL url = ConfigurationUtils.locate(FileSystem.getDefaultFileSystem(), "", solrDirectory + "/solr.xml");

            if (url == null) {
                try {
                    url = new URL(solrDirectory + "/solr.xml");
                } catch (MalformedURLException e) {
                    LOGGER.error("unable to load solr config", e);
                }
            }
            solrXml = new File(url.toURI());
        } catch (URISyntaxException e) {
            LOGGER.error("unable to instanciate SolrCloudBootstrap", e);
        }
        File solrHomeDir = solrXml.getParentFile();

        String context = "/solr";
        solrServer = new JettySolrRunner(solrHomeDir.getAbsolutePath(), context, solrPort);

    }

    private void loadConfig() throws BootstrapException {
        solrDirectory = configuration.getString(HadoopUnitConfig.SOLR_DIR_KEY);
        solrCollectionName = configuration.getString(HadoopUnitConfig.SOLR_COLLECTION_NAME);
        solrPort = configuration.getInt(HadoopUnitConfig.SOLR_PORT);
        zkHostString = configuration.getString(HadoopUnitConfig.ZOOKEEPER_HOST_KEY) + ":" + configuration.getInt(HadoopUnitConfig.ZOOKEEPER_PORT_KEY);
    }

    @Override
    public void loadConfig(Map<String, String> configs) {
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.SOLR_DIR_KEY))) {
            solrDirectory = configs.get(HadoopUnitConfig.SOLR_DIR_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.SOLR_COLLECTION_NAME))) {
            solrCollectionName = configs.get(HadoopUnitConfig.SOLR_COLLECTION_NAME);
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.SOLR_PORT))) {
            solrPort = Integer.parseInt(configs.get(HadoopUnitConfig.SOLR_PORT));
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.ZOOKEEPER_HOST_KEY)) && StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.ZOOKEEPER_PORT_KEY))) {
            zkHostString = configs.get(HadoopUnitConfig.ZOOKEEPER_HOST_KEY) + ":" + configs.get(HadoopUnitConfig.ZOOKEEPER_PORT_KEY);
        }
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

            URL url = ConfigurationUtils.locate(FileSystem.getDefaultFileSystem(), "", solrDirectory + "/collection1/conf");

            if (url == null) {
                try {
                    url = new URL(solrDirectory + "/collection1/conf");
                } catch (MalformedURLException e) {
                    LOGGER.error("unable to load solr config", e);
                }
            }
            URI solrDirectoryFile = url.toURI();


            try (SolrZkClient zkClient = new SolrZkClient(zkHostString, TIMEOUT, 45000, null)) {
                ZkConfigManager manager = new ZkConfigManager(zkClient);
                manager.uploadConfigDir(Paths.get(solrDirectoryFile), solrCollectionName);
            }

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

    public CloudSolrClient getClient() {
        return new CloudSolrClient(zkHostString);
    }

}
