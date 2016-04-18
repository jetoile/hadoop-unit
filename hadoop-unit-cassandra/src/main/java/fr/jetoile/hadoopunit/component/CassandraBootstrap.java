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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import fr.jetoile.hadoopunit.Component;
import fr.jetoile.hadoopunit.Config;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.dataset.CQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;

public class CassandraBootstrap implements Bootstrap {
    final public static String NAME = Component.CASSANDRA.name();

    final private Logger LOGGER = LoggerFactory.getLogger(CassandraBootstrap.class);

    private State state = State.STOPPED;

    private Configuration configuration;

    private int port;
    private String ip;

    protected int readTimeoutMillis = 12000;

    private CQLDataSet dataSet;

    public Session session;
    public Cluster cluster;

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
        return "[" +
                "ip:" + ip +
                ", port:" + port +
                "]";
    }

    private void loadConfig() throws BootstrapException {
        try {
            configuration = new PropertiesConfiguration(Config.DEFAULT_PROPS_FILE);
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }

        port = configuration.getInt(Config.CASSANDRA_PORT_KEY);
        ip = configuration.getString(Config.CASSANDRA_IP_KEY);
        editCassandraConfFile();
    }

    private void build() throws InterruptedException, IOException, TTransportException {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra("cu-cassandra.yaml", configuration.getString(Config.CASSANDRA_TEMP_DIR_KEY), EmbeddedCassandraServerHelper.DEFAULT_STARTUP_TIMEOUT);
        cluster = new Cluster.Builder().addContactPoints(ip).withPort(port).withSocketOptions(getSocketOptions())
                .build();
        session = cluster.connect();
    }

    protected SocketOptions getSocketOptions() {
        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setReadTimeoutMillis(this.readTimeoutMillis);
        return socketOptions;
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
                session.close();
                cluster.close();
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
            FileUtils.deleteDirectory(Paths.get(configuration.getString(Config.CASSANDRA_TEMP_DIR_KEY)).toFile());
        } catch (IOException e) {
            LOGGER.error("unable to delete {}", configuration.getString(Config.CASSANDRA_TEMP_DIR_KEY), e);
        }
    }

    @Override
    public org.apache.hadoop.conf.Configuration getConfiguration() {
        throw new UnsupportedOperationException("the method getConfiguration can not be called on CassandraBootstrap");
    }

    public Session getCassandraSession() {
        return this.session;
    }

    private void editCassandraConfFile() {
        try {
            Path cassandraPropertiesBackupPath = Paths.get(configuration.getString(Config.CASSANDRA_TEMP_DIR_KEY) + "/cu-cassandra.yaml.old");
            Path cassandraPropertiesPath = Paths.get(CassandraBootstrap.class.getClassLoader().getResource("cu-cassandra.yaml").toURI());
            if (cassandraPropertiesBackupPath.toFile().exists() && cassandraPropertiesBackupPath.toFile().canWrite()) {
                cassandraPropertiesBackupPath.toFile().delete();
            }

            Yaml yaml = new Yaml();

            Map<String, Object> params = (Map<String, Object>) yaml.load(new FileInputStream(cassandraPropertiesPath.toString()));

            params.put("hints_directory", configuration.getString(Config.CASSANDRA_TEMP_DIR_KEY));
            params.put("data_file_directories", Arrays.asList(configuration.getString(Config.CASSANDRA_TEMP_DIR_KEY) + "/embeddedCassandra/data"));
            params.put("commitlog_directory", Arrays.asList(configuration.getString(Config.CASSANDRA_TEMP_DIR_KEY) + "/embeddedCassandra/commitlog"));
            params.put("saved_caches_directory", Arrays.asList(configuration.getString(Config.CASSANDRA_TEMP_DIR_KEY) + "/embeddedCassandra/saved_caches"));
            params.put("native_transport_port", configuration.getInt(Config.CASSANDRA_PORT_KEY));

            cassandraPropertiesPath.toFile().renameTo(cassandraPropertiesBackupPath.toFile());

            FileWriter writer = new FileWriter(cassandraPropertiesPath.toFile());
            yaml.dump(params, writer);

        } catch (IOException | URISyntaxException e) {
            LOGGER.error("unable to find or modifying cu-cassandra.yaml. Check user rights", e);
        }
    }

}
