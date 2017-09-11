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

import com.datastax.driver.core.Session;
import fr.jetoile.hadoopunit.Component;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;
import info.archinnov.achilles.embedded.CassandraShutDownHook;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

public class CassandraBootstrap implements Bootstrap {
    final public static String NAME = Component.CASSANDRA.name();

    static final private Logger LOGGER = LoggerFactory.getLogger(CassandraBootstrap.class);
    public Session session;
    private State state = State.STOPPED;
    private Configuration configuration;
    private CassandraShutDownHook shutdownHook;
    private int port;
    private String ip;

    public CassandraBootstrap() {
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
        return "\n \t\t\t ip:" + ip +
                "\n \t\t\t port:" + port;
    }

    private void loadConfig() throws BootstrapException {
        try {
            configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }

        port = configuration.getInt(HadoopUnitConfig.CASSANDRA_PORT_KEY);
        ip = configuration.getString(HadoopUnitConfig.CASSANDRA_IP_KEY);
    }

    @Override
    public void loadConfig(Map<String, String> configs) {
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.CASSANDRA_PORT_KEY))) {
            port = Integer.parseInt(configs.get(HadoopUnitConfig.CASSANDRA_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.CASSANDRA_IP_KEY))) {
            ip = configs.get(HadoopUnitConfig.CASSANDRA_IP_KEY);
        }
    }

    private void build() throws InterruptedException, IOException, TTransportException {
        Files.createDirectory(Paths.get(configuration.getString(HadoopUnitConfig.CASSANDRA_TEMP_DIR_KEY)));
        Files.createDirectory(Paths.get(configuration.getString(HadoopUnitConfig.CASSANDRA_TEMP_DIR_KEY) + "/data"));
        Files.createDirectory(Paths.get(configuration.getString(HadoopUnitConfig.CASSANDRA_TEMP_DIR_KEY) + "/commitlog"));
        Files.createDirectory(Paths.get(configuration.getString(HadoopUnitConfig.CASSANDRA_TEMP_DIR_KEY) + "/saved_caches"));
        Files.createDirectory(Paths.get(configuration.getString(HadoopUnitConfig.CASSANDRA_TEMP_DIR_KEY) + "/hints"));

        shutdownHook = new CassandraShutDownHook();

        session = CassandraEmbeddedServerBuilder.builder()
                .withListenAddress(ip)
                .withRpcAddress(ip)
                .withBroadcastAddress(ip)
                .withBroadcastRpcAddress(ip)
                .withCQLPort(port)
                .withDataFolder(configuration.getString(HadoopUnitConfig.CASSANDRA_TEMP_DIR_KEY) + "/data")
                .withCommitLogFolder(configuration.getString(HadoopUnitConfig.CASSANDRA_TEMP_DIR_KEY) + "/commitlog")
                .withSavedCachesFolder(configuration.getString(HadoopUnitConfig.CASSANDRA_TEMP_DIR_KEY) + "/saved_caches")
                .withHintsFolder(configuration.getString(HadoopUnitConfig.CASSANDRA_TEMP_DIR_KEY) + "/hints")
                .cleanDataFilesAtStartup(true)
                .withShutdownHook(shutdownHook)
                .cleanDataFilesAtStartup(true)
                .buildNativeSession();

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
                shutdownHook.shutDownNow();
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
            FileUtils.deleteDirectory(Paths.get(configuration.getString(HadoopUnitConfig.CASSANDRA_TEMP_DIR_KEY)).toFile());
        } catch (IOException e) {
            LOGGER.error("unable to delete {}", configuration.getString(HadoopUnitConfig.CASSANDRA_TEMP_DIR_KEY), e);
        }
    }

    public Session getCassandraSession() {
        return this.session;
    }

}
