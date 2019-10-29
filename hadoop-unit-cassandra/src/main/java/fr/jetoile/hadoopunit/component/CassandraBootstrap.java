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
import fr.jetoile.hadoopunit.ComponentMetadata;
import fr.jetoile.hadoopunit.HadoopUtils;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;
import info.archinnov.achilles.embedded.CassandraShutDownHook;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

public class CassandraBootstrap implements Bootstrap {
    static final private Logger LOGGER = LoggerFactory.getLogger(CassandraBootstrap.class);
    public Session session;
    private State state = State.STOPPED;
    private Configuration configuration;
    private CassandraShutDownHook shutdownHook;
    private int port;
    private String listenAddressIp;
    private String tmpDirPath;
    private String rpcAddressIp;
    private String broadcastAddressIp;
    private String broadcastRpcAddressIp;

    public CassandraBootstrap() {
        try {
            configuration = HadoopUtils.INSTANCE.loadConfigFile(null);
            loadConfig();
        } catch (BootstrapException e) {
            LOGGER.error("unable to load configuration", e);
        }
    }

    public CassandraBootstrap(URL url) {
        try {
            configuration = HadoopUtils.INSTANCE.loadConfigFile(url);
            loadConfig();
        } catch (BootstrapException e) {
            LOGGER.error("unable to load configuration", e);
        }
    }

    @Override
    public ComponentMetadata getMetadata() {
        return new CassandraMetadata();
    }

    @Override
    public String getProperties() {
        return "\n \t\t\t listenAddressIp:" + listenAddressIp +
                "\n \t\t\t rpcAddressIp:" + rpcAddressIp +
                "\n \t\t\t broadcastAddressIp:" + broadcastAddressIp +
                "\n \t\t\t broadcastRpcAddressIp:" + broadcastRpcAddressIp +
                "\n \t\t\t port:" + port;
    }

    private void loadConfig() {
        port = configuration.getInt(CassandraConfig.CASSANDRA_PORT_KEY);
        listenAddressIp = configuration.getString(CassandraConfig.CASSANDRA_LISTEN_ADDRESS_IP_KEY);
        rpcAddressIp = configuration.getString(CassandraConfig.CASSANDRA_RPC_ADDRESS_IP_KEY);
        broadcastAddressIp = configuration.getString(CassandraConfig.CASSANDRA_BROADCAST_ADDRESS_IP_KEY);
        broadcastRpcAddressIp = configuration.getString(CassandraConfig.CASSANDRA_BROADCAST_RPC_ADDRESS_IP_KEY);
        tmpDirPath = getTmpDirPath(configuration, CassandraConfig.CASSANDRA_TEMP_DIR_KEY);
    }

    @Override
    public void loadConfig(Map<String, String> configs) {
        if (StringUtils.isNotEmpty(configs.get(CassandraConfig.CASSANDRA_PORT_KEY))) {
            port = Integer.parseInt(configs.get(CassandraConfig.CASSANDRA_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(CassandraConfig.CASSANDRA_LISTEN_ADDRESS_IP_KEY))) {
            listenAddressIp = configs.get(CassandraConfig.CASSANDRA_LISTEN_ADDRESS_IP_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(CassandraConfig.CASSANDRA_RPC_ADDRESS_IP_KEY))) {
            rpcAddressIp = configs.get(CassandraConfig.CASSANDRA_RPC_ADDRESS_IP_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(CassandraConfig.CASSANDRA_BROADCAST_ADDRESS_IP_KEY))) {
            broadcastAddressIp = configs.get(CassandraConfig.CASSANDRA_BROADCAST_ADDRESS_IP_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(CassandraConfig.CASSANDRA_BROADCAST_RPC_ADDRESS_IP_KEY))) {
            broadcastRpcAddressIp = configs.get(CassandraConfig.CASSANDRA_BROADCAST_RPC_ADDRESS_IP_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(CassandraConfig.CASSANDRA_TEMP_DIR_KEY))) {
            tmpDirPath = getTmpDirPath(configs, CassandraConfig.CASSANDRA_TEMP_DIR_KEY);
        }
    }

    private void build() throws IOException {
        Files.createDirectory(Paths.get(tmpDirPath));
        Files.createDirectory(Paths.get(tmpDirPath + "/data"));
        Files.createDirectory(Paths.get(tmpDirPath + "/commitlog"));
        Files.createDirectory(Paths.get(tmpDirPath + "/saved_caches"));
        Files.createDirectory(Paths.get(tmpDirPath + "/hints"));

        shutdownHook = new CassandraShutDownHook();

        session = CassandraEmbeddedServerBuilder.builder()
                .withListenAddress(listenAddressIp)
                .withRpcAddress(rpcAddressIp)
                .withBroadcastAddress(broadcastAddressIp)
                .withBroadcastRpcAddress(broadcastRpcAddressIp)
                .withCQLPort(port)
                .withDataFolder(tmpDirPath + "/data")
                .withCommitLogFolder(tmpDirPath + "/commitlog")
                .withSavedCachesFolder(tmpDirPath + "/saved_caches")
                .withHintsFolder(tmpDirPath + "/hints")
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
            FileUtils.deleteDirectory(Paths.get(tmpDirPath).toFile());
        } catch (IOException e) {
            LOGGER.error("unable to delete {}", tmpDirPath, e);
        }
    }

    public Session getCassandraSession() {
        return this.session;
    }

}
