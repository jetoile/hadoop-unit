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

import com.github.sakserv.minicluster.impl.KafkaLocalBroker;
import fr.jetoile.hadoopunit.Component;
import fr.jetoile.hadoopunit.HadoopBootstrap;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.HadoopUtils;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import fr.jetoile.hadoopunit.exception.NotFoundServiceException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Map;
import java.util.Properties;

public class KafkaBootstrap implements Bootstrap {
    final public static String NAME = Component.KAFKA.name();

    static final private Logger LOGGER = LoggerFactory.getLogger(KafkaBootstrap.class);

    private KafkaLocalBroker kafkaLocalCluster;

    private State state = State.STOPPED;

    private Configuration configuration;
    private String zookeeperConnectionString;
    private String host;
    private int port;
    private int brokerId;
    private String tmpDirectory;


    public KafkaBootstrap() {
        if (kafkaLocalCluster == null) {
            try {
                configuration = HadoopUtils.INSTANCE.loadConfigFile(null);
                loadConfig();
            } catch (BootstrapException e) {
                LOGGER.error("unable to load configuration", e);
            }

        }
    }

    public KafkaBootstrap(URL url) {
        if (kafkaLocalCluster == null) {
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
        return "\n \t\t\t host:" + host +
                "\n \t\t\t port:" + port;
    }

    private void init() {

    }

    private void build() {
        kafkaLocalCluster = new KafkaLocalBroker.Builder()
                .setKafkaHostname(host)
                .setKafkaPort(port)
                .setKafkaBrokerId(brokerId)
                .setKafkaProperties(new Properties())
                .setKafkaTempDir(tmpDirectory)
                .setZookeeperConnectionString(zookeeperConnectionString)
                .build();

        Properties kafkaProperties = kafkaLocalCluster.getKafkaProperties();
        kafkaProperties.put("default.replication.factor", "1");
        kafkaProperties.put("offsets.topic.replication.factor", "1");
    }

    private void loadConfig() throws BootstrapException {
        host = configuration.getString(HadoopUnitConfig.KAFKA_HOSTNAME_KEY);
        port = configuration.getInt(HadoopUnitConfig.KAFKA_PORT_KEY);
        brokerId = configuration.getInt(HadoopUnitConfig.KAFKA_TEST_BROKER_ID_KEY);
        tmpDirectory = configuration.getString(HadoopUnitConfig.KAFKA_TEST_TEMP_DIR_KEY);
        zookeeperConnectionString = configuration.getString(HadoopUnitConfig.ZOOKEEPER_HOST_KEY) + ":" + configuration.getInt(HadoopUnitConfig.ZOOKEEPER_PORT_KEY);
    }

    @Override
    public void loadConfig(Map<String, String> configs) {
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.KAFKA_HOSTNAME_KEY))) {
            host = configs.get(HadoopUnitConfig.KAFKA_HOSTNAME_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.KAFKA_PORT_KEY))) {
            port = Integer.parseInt(configs.get(HadoopUnitConfig.KAFKA_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.KAFKA_TEST_BROKER_ID_KEY))) {
            brokerId = Integer.parseInt(configs.get(HadoopUnitConfig.KAFKA_TEST_BROKER_ID_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.KAFKA_TEST_TEMP_DIR_KEY))) {
            tmpDirectory = configs.get(HadoopUnitConfig.KAFKA_TEST_TEMP_DIR_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.ZOOKEEPER_HOST_KEY)) && StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.ZOOKEEPER_PORT_KEY))) {
            zookeeperConnectionString = configs.get(HadoopUnitConfig.ZOOKEEPER_HOST_KEY) + ":" + configs.get(HadoopUnitConfig.ZOOKEEPER_PORT_KEY);
        }
    }

    @Override
    public Bootstrap start() {
        if (state == State.STOPPED) {
            state = State.STARTING;
            LOGGER.info("{} is starting", this.getClass().getName());

            init();
            build();
            try {
                kafkaLocalCluster.start();
            } catch (Exception e) {
                LOGGER.error("unable to add kafka", e);
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
                kafkaLocalCluster.stop(true);
            } catch (Exception e) {
                LOGGER.error("unable to stop kafka", e);
            }
            state = State.STOPPED;
            LOGGER.info("{} is stopped", this.getClass().getName());
        }

        return this;

    }
}
