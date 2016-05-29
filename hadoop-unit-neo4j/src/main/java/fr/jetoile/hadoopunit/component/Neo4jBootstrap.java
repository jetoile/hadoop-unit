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
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;

public class Neo4jBootstrap implements Bootstrap {
    final public static String NAME = Component.NEO4J.name();

    final private Logger LOGGER = LoggerFactory.getLogger(Neo4jBootstrap.class);

    private State state = State.STOPPED;

    private Configuration configuration;

    private int port;
    private String ip;
    private String tmp;

    private GraphDatabaseService graphDb;

    public Neo4jBootstrap() {
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
                "ip:" + ip +
                ", port:" + port +
                "]";
    }

    private void loadConfig() throws BootstrapException {
        try {
            configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }

        port = configuration.getInt(HadoopUnitConfig.NEO4J_PORT_KEY);
        ip = configuration.getString(HadoopUnitConfig.NEO4J_IP_KEY);
        tmp = configuration.getString(HadoopUnitConfig.NEO4J_TEMP_DIR_KEY);
    }

    private void build() {
        GraphDatabaseSettings.BoltConnector bolt = GraphDatabaseSettings.boltConnector("0");

        graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(Paths.get(tmp).toFile())
//                .setConfig(GraphDatabaseSettings.pagecache_memory, "512M")
//                .setConfig(GraphDatabaseSettings.string_block_size, "60")
//                .setConfig(GraphDatabaseSettings.array_block_size, "300")
                .setConfig(bolt.enabled, "true")
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
                LOGGER.error("unable to add cassandra", e);
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
                LOGGER.error("unable to stop cassandra", e);
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

    @Override
    public org.apache.hadoop.conf.Configuration getConfiguration() {
        throw new UnsupportedOperationException("the method getConfiguration can not be called on Neo4jBootstrap");
    }

    public GraphDatabaseService getNeo4jGraph() {
        return this.graphDb;
    }

    final public static void main(String... args) throws NotFoundServiceException {

        HadoopBootstrap bootstrap = HadoopBootstrap.INSTANCE;

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                bootstrap.stopAll();
            }
        });

        bootstrap.add(Component.NEO4J).startAll();
    }

}
