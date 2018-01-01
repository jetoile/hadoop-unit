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
import fr.jetoile.hadoopunit.HadoopUtils;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import io.confluent.kafkarest.KafkaRestApplication;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.ksql.rest.server.KsqlRestApplication;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.version.metrics.KsqlVersionCheckerAgent;
import io.confluent.ksql.version.metrics.collector.KsqlModuleType;
import io.confluent.rest.RestConfigException;
import io.confluent.support.metrics.BaseSupportConfig;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static fr.jetoile.hadoopunit.HadoopUnitConfig.*;
import static io.confluent.support.metrics.BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_CONFIG;

public class ConfluentKsqlRestBootstrap implements Bootstrap {
    final public static String NAME = Component.CONFLUENT_KSQL_REST.name();

    static final private Logger LOGGER = LoggerFactory.getLogger(ConfluentKsqlRestBootstrap.class);

    private State state = State.STOPPED;

    private Configuration configuration;

    private Map<String, String> ksqlConfig = new HashMap<>();

    private KsqlRestApplication restServer;


    public ConfluentKsqlRestBootstrap() {
        try {
            configuration = HadoopUtils.INSTANCE.loadConfigFile(null);
            loadConfig();
        } catch (BootstrapException e) {
            LOGGER.error("unable to load configuration", e);
        }
    }

    public ConfluentKsqlRestBootstrap(URL url) {
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
        return "\n \t\t\t ksql rest host:" + configuration.getString(CONFLUENT_KSQL_HOST_KEY) +
                "\n \t\t\t ksql rest port:" + configuration.getString(CONFLUENT_KSQL_PORT_KEY);
    }

    public void loadConfig() {
        ksqlConfig.put(KsqlRestConfig.LISTENERS_CONFIG, "http://" + configuration.getString(CONFLUENT_KSQL_HOST_KEY) + ":" + configuration.getString(CONFLUENT_KSQL_PORT_KEY));
//    props.put(KsqlRestConfig.PORT_CONFIG, String.valueOf(portNumber));
        ksqlConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getString(CONFLUENT_KAFKA_HOST_KEY) + ":" + configuration.getString(CONFLUENT_KAFKA_PORT_KEY));
        ksqlConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "ksql_config_test");
        ksqlConfig.put(KsqlRestConfig.COMMAND_TOPIC_SUFFIX_CONFIG, "commands");
    }

    @Override
    public void loadConfig(Map<String, String> configs) {
        if (StringUtils.isNotEmpty(configs.get(CONFLUENT_KSQL_HOST_KEY)) && StringUtils.isNotEmpty(configs.get(CONFLUENT_KSQL_PORT_KEY))) {
            ksqlConfig.put(KsqlRestConfig.LISTENERS_CONFIG, "http://" + configs.get(CONFLUENT_KSQL_HOST_KEY) + ":" + configs.get(CONFLUENT_KSQL_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(CONFLUENT_KAFKA_HOST_KEY)) && StringUtils.isNotEmpty(configs.get(CONFLUENT_KAFKA_PORT_KEY))) {
            ksqlConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, configs.get(CONFLUENT_KAFKA_HOST_KEY) + ":" + configs.get(CONFLUENT_KAFKA_PORT_KEY));
        }
    }

    private void build() {
        try {
            KsqlRestConfig config = new KsqlRestConfig(ksqlConfig);

            Properties versionCheckProps = new Properties();
            versionCheckProps.setProperty(CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_CONFIG, "false");
//            versionCheckProps.setProperty(BaseSupportConfig.CONFLUENT_SUPPORT_PROXY_CONFIG,"http://localhost:" + proxyPort);

            KsqlVersionCheckerAgent versionCheckerAgent = new KsqlVersionCheckerAgent();
            versionCheckerAgent.start(KsqlModuleType.REMOTE_CLI, versionCheckProps);

            restServer = KsqlRestApplication.buildApplication(config, true, versionCheckerAgent);
        } catch (RestConfigException e) {
            LOGGER.error("Server configuration failed: ", e);
        } catch (Exception e) {
            LOGGER.error("Server died unexpectedly: ", e);
        }


    }

    @Override
    public Bootstrap start() {
        if (state == State.STOPPED) {
            state = State.STARTING;
            LOGGER.info("{} is starting", this.getClass().getName());
            try {
                build();
                restServer.start();

            } catch (Exception e) {
                LOGGER.error("unable to add confluent kafka rest", e);
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
                restServer.stop();
            } catch (Exception e) {
                LOGGER.error("unable to stop confluent kafka rest", e);
            }
            state = State.STOPPED;
            LOGGER.info("{} is stopped", this.getClass().getName());
        }
        return this;
    }

}
