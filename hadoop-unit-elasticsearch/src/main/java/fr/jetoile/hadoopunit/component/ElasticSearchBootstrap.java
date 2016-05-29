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
import fr.jetoile.hadoopunit.exception.BootstrapException;
import fr.jetoile.hadoopunit.exception.NotFoundServiceException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;

public class ElasticSearchBootstrap implements Bootstrap {
    final public static String NAME = Component.ELASTICSEARCH.name();

    final private Logger LOGGER = LoggerFactory.getLogger(ElasticSearchBootstrap.class);

    private State state = State.STOPPED;

    private Configuration configuration;

    private String ip;
    private int httpPort;
    private int tcpPort;
    private Client client;
    private Node node;
    private String tmpDir;
    private String indexName;
    private String clusterName;

    public ElasticSearchBootstrap() {
        try {
            loadConfig();
        } catch (BootstrapException e) {
            LOGGER.error("unable to load configuration", e);
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getProperties() {
        return "[" +
                "clusterName:" + clusterName +
                ", ip:" + ip +
                ", httpPort:" + httpPort +
                ", tcpPort:" + tcpPort +
                ", indexName:" + indexName +
                ", tmpDir:" + tmpDir +
                "]";
    }

    private void loadConfig() throws BootstrapException {
        try {
            configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }

        httpPort = configuration.getInt(HadoopUnitConfig.ELASTICSEARCH_HTTP_PORT_KEY);
        tcpPort = configuration.getInt(HadoopUnitConfig.ELASTICSEARCH_TCP_PORT_KEY);
        ip = configuration.getString(HadoopUnitConfig.ELASTICSEARCH_IP_KEY);
        tmpDir = configuration.getString(HadoopUnitConfig.ELASTICSEARCH_TEMP_DIR_KEY);
        indexName = configuration.getString(HadoopUnitConfig.ELASTICSEARCH_INDEX_NAME);
        clusterName = configuration.getString(HadoopUnitConfig.ELASTICSEARCH_CLUSTER_NAME);
    }

    private void build() {
        client = createEmbeddedNode().client();
    }

    private Node createEmbeddedNode() {
        // Instancie le client pour inserer
        Settings settings = Settings.builder()
                .put("cluster.name", clusterName)
                .put("path.data", tmpDir + "/data")
                .put("path.logs", tmpDir + "/logs")
                .put("http.port", httpPort)
                .put("transport.tcp.port", tcpPort)
                .put("network.host", ip)
                .put("path.home", tmpDir)
                .build();

        node = new Node(settings);
        node.start();
        LOGGER.debug("an embedded node is started");

        return node;
    }

    @Override
    public Bootstrap start() {
        if (state == State.STOPPED) {
            state = State.STARTING;
            LOGGER.info("{} is starting", this.getClass().getName());
            try {
                build();
                client.admin().indices().prepareCreate(indexName).execute().actionGet();

                client.admin().cluster().prepareHealth().setWaitForYellowStatus().get();
            } catch (Exception e) {
                LOGGER.error("unable to add elastic", e);
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
            try {
                client.close();
                node.close();
                cleanup();
            } catch (Exception e) {
                LOGGER.error("unable to stop elastic", e);
            }
            state = State.STOPPED;
            LOGGER.info("{} is stopped", this.getClass().getName());
        }
        return this;
    }

    @Override
    public org.apache.hadoop.conf.Configuration getConfiguration() {
        throw new UnsupportedOperationException("the method getConfiguration can not be called on ElasticSearchBootstrap");
    }

    private void cleanup() {
        try {
            FileUtils.deleteDirectory(Paths.get(tmpDir).toFile());
        } catch (IOException e) {
            LOGGER.error("unable to delete {}", tmpDir, e);
        }
    }

    public Client getClient() {
        return client;
    }

    final public static void main(String... args) throws NotFoundServiceException {

        HadoopBootstrap bootstrap = HadoopBootstrap.INSTANCE;

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                bootstrap.stopAll();
            }
        });

        bootstrap.add(Component.ELASTICSEARCH).startAll();
    }
}
