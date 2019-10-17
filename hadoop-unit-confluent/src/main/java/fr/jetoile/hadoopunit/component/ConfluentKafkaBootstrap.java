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

import fr.jetoile.hadoopunit.ComponentMetadata;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.HadoopUtils;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConversions;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ConfluentKafkaBootstrap implements Bootstrap {
    static final private Logger LOGGER = LoggerFactory.getLogger(ConfluentKafkaBootstrap.class);

    private State state = State.STOPPED;

    private Configuration configuration;

    private Map<String, String> kafkaConfig = new HashMap<>();

    private KafkaServer kafkaServer;


    public ConfluentKafkaBootstrap() {
        try {
            configuration = HadoopUtils.INSTANCE.loadConfigFile(null);
            loadConfig();
        } catch (BootstrapException e) {
            LOGGER.error("unable to load configuration", e);
        }
    }

    public ConfluentKafkaBootstrap(URL url) {
        try {
            configuration = HadoopUtils.INSTANCE.loadConfigFile(url);
            loadConfig();
        } catch (BootstrapException e) {
            LOGGER.error("unable to load configuration", e);
        }
    }

    @Override
    public ComponentMetadata getMetadata() {
        return new ConfluentKafkaMetadata();
    }

    @Override
    public String getProperties() {
        return "\n \t\t\t kafka host:" + configuration.getString(ConfluentConfig.CONFLUENT_KAFKA_HOST_KEY) +
                "\n \t\t\t kafka port:" + configuration.getString(ConfluentConfig.CONFLUENT_KAFKA_PORT_KEY);
    }

    public void loadConfig() {
        kafkaConfig.put("zookeeper.connect", configuration.getString(ZookeeperConfig.ZOOKEEPER_HOST_KEY) + ":" + configuration.getString(ZookeeperConfig.ZOOKEEPER_PORT_KEY));
        kafkaConfig.put("log.dirs", getTmpDirPath(configuration, ConfluentConfig.CONFLUENT_KAFKA_LOG_DIR_KEY));
        kafkaConfig.put("broker.id", configuration.getString(ConfluentConfig.CONFLUENT_KAFKA_BROKER_ID_KEY));
//        kafkaConfig.put("advertised.listeners", "PLAINTEXT://localhost:22222");
        kafkaConfig.put("advertised.host.name", configuration.getString(ConfluentConfig.CONFLUENT_KAFKA_HOST_KEY));
        kafkaConfig.put("port", configuration.getString(ConfluentConfig.CONFLUENT_KAFKA_PORT_KEY));
        kafkaConfig.put("confluent.support.metrics.enable", "false");
        kafkaConfig.put("offsets.topic.replication.factor", "1");
        kafkaConfig.put("num.network.threads", "3");
    }

    @Override
    public void loadConfig(Map<String, String> configs) {
        if (StringUtils.isNotEmpty(configs.get(ZookeeperConfig.ZOOKEEPER_HOST_KEY)) && StringUtils.isNotEmpty(configs.get(ZookeeperConfig.ZOOKEEPER_PORT_KEY))) {
            kafkaConfig.put("zookeeper.connect", configs.get(ZookeeperConfig.ZOOKEEPER_HOST_KEY) + ":" + configs.get(ZookeeperConfig.ZOOKEEPER_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(ConfluentConfig.CONFLUENT_KAFKA_LOG_DIR_KEY))) {
            kafkaConfig.put("log.dirs", getTmpDirPath(configs, ConfluentConfig.CONFLUENT_KAFKA_LOG_DIR_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(ConfluentConfig.CONFLUENT_KAFKA_BROKER_ID_KEY))) {
            kafkaConfig.put("broker.id", configs.get(ConfluentConfig.CONFLUENT_KAFKA_BROKER_ID_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(ConfluentConfig.CONFLUENT_KAFKA_HOST_KEY))) {
            kafkaConfig.put("advertised.host.name", configs.get(ConfluentConfig.CONFLUENT_KAFKA_HOST_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(ConfluentConfig.CONFLUENT_KAFKA_PORT_KEY))) {
            kafkaConfig.put("port", configs.get(ConfluentConfig.CONFLUENT_KAFKA_PORT_KEY));
        }
    }

    private void build() {
        KafkaConfig kf = new KafkaConfig(kafkaConfig);
        Option<String> threadPrefixName = Option.apply("kafka-mini-cluster");
        kafkaServer = new KafkaServer(kf, Time.SYSTEM, threadPrefixName, JavaConversions.asScalaBuffer(Collections.EMPTY_LIST).toSeq());
    }

    @Override
    public Bootstrap start() {
        if (state == State.STOPPED) {
            state = State.STARTING;
            LOGGER.info("{} is starting", this.getClass().getName());
            try {
                build();
                kafkaServer.startup();

            } catch (Exception e) {
                LOGGER.error("unable to add confluent kafka", e);
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
                kafkaServer.shutdown();
                cleanup();
            } catch (Exception e) {
                LOGGER.error("unable to stop confluent kafka", e);
            }
            state = State.STOPPED;
            LOGGER.info("{} is stopped", this.getClass().getName());
        }
        return this;
    }

    private void cleanup() {
        try {
            FileUtils.deleteDirectory(Paths.get(getTmpDirPath(configuration, ConfluentConfig.CONFLUENT_KAFKA_LOG_DIR_KEY)).toFile());
        } catch (IOException e) {
            LOGGER.error("unable to delete {}", configuration.getString(ConfluentConfig.CONFLUENT_KAFKA_LOG_DIR_KEY), e);
        }
    }
}
