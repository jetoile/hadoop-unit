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
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.HadoopUtils;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.configuration.BoltConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Map;

public class Neo4jBootstrap implements Bootstrap {
    final public static String NAME = Component.NEO4J.name();

    static final private Logger LOGGER = LoggerFactory.getLogger(Neo4jBootstrap.class);

    private State state = State.STOPPED;

    private Configuration configuration;

    private int port;
    private String ip;
    private String tmp;

    private GraphDatabaseService graphDb;

    public Neo4jBootstrap() {
        try {
            configuration = HadoopUtils.INSTANCE.loadConfigFile(null);
            loadConfig();
        } catch (BootstrapException e) {
            LOGGER.error("unable to load configuration", e);
        }
    }

    public Neo4jBootstrap(URL url) {
        try {
            configuration = HadoopUtils.INSTANCE.loadConfigFile(url);
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
        return "\n \t\t\t ip:" + ip +
                "\n \t\t\t port:" + port;
    }

    private void loadConfig() throws BootstrapException {
        port = configuration.getInt(HadoopUnitConfig.NEO4J_PORT_KEY);
        ip = configuration.getString(HadoopUnitConfig.NEO4J_IP_KEY);
        tmp = configuration.getString(HadoopUnitConfig.NEO4J_TEMP_DIR_KEY);
    }

    @Override
    public void loadConfig(Map<String, String> configs) {
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.NEO4J_PORT_KEY))) {
            port = Integer.parseInt(configs.get(HadoopUnitConfig.NEO4J_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.NEO4J_IP_KEY))) {
            ip = configs.get(HadoopUnitConfig.NEO4J_IP_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.NEO4J_TEMP_DIR_KEY))) {
            tmp = configs.get(HadoopUnitConfig.NEO4J_TEMP_DIR_KEY);
        }
    }

    private void build() {
        BoltConnector bolt = new BoltConnector("0");

        graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(Paths.get(tmp).toFile())
//                .setConfig(GraphDatabaseSettings.pagecache_memory, "512M")
//                .setConfig(GraphDatabaseSettings.string_block_size, "60")
//                .setConfig(GraphDatabaseSettings.array_block_size, "300")
                .setConfig(bolt.enabled, "true")
                .setConfig(bolt.type, "BOLT")
                .setConfig(bolt.address, ip + ":" + port)
                .newGraphDatabase();
    }

    @Override
    public Bootstrap start() {
        if (state == State.STOPPED) {
            state = State.STARTING;
            LOGGER.info("{} is starting", this.getClass().getName());
            try {
                build();
            } catch (Exception e) {
                LOGGER.error("unable to add neo4j", e);
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
                graphDb.shutdown();
                cleanup();
            } catch (Exception e) {
                LOGGER.error("unable to stop neo4j", e);
            }
            state = State.STOPPED;
            LOGGER.info("{} is stopped", this.getClass().getName());
        }
        return this;
    }

    private void cleanup() {
        try {
            FileUtils.deleteDirectory(Paths.get(configuration.getString(HadoopUnitConfig.NEO4J_TEMP_DIR_KEY)).toFile());
        } catch (IOException e) {
            LOGGER.error("unable to delete {}", configuration.getString(HadoopUnitConfig.NEO4J_TEMP_DIR_KEY), e);
        }
    }

    public GraphDatabaseService getNeo4jGraph() {
        return this.graphDb;
    }

}
