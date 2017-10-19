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
import io.confluent.rest.RestConfigException;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Properties;

import static fr.jetoile.hadoopunit.HadoopUnitConfig.*;

public class ConfluentKafkaRestBootstrap implements Bootstrap {
    final public static String NAME = Component.CONFLUENT_KAFKA_REST.name();

    static final private Logger LOGGER = LoggerFactory.getLogger(ConfluentKafkaRestBootstrap.class);

    private State state = State.STOPPED;

    private Configuration configuration;

    private Properties restConfig = new Properties();

    private KafkaRestApplication restServer;


    public ConfluentKafkaRestBootstrap() {
        try {
            configuration = HadoopUtils.INSTANCE.loadConfigFile(null);
            loadConfig();
        } catch (BootstrapException e) {
            LOGGER.error("unable to load configuration", e);
        }
    }

    public ConfluentKafkaRestBootstrap(URL url) {
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
        return "\n \t\t\t rest host:" + configuration.getString(CONFLUENT_REST_HOST_KEY) +
                "\n \t\t\t rest port:" + configuration.getString(CONFLUENT_REST_PORT_KEY);
    }

    public void loadConfig() {
        restConfig.put("schema.registry.url", configuration.getString(CONFLUENT_SCHEMAREGISTRY_HOST_KEY) + ":" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_PORT_KEY));
        restConfig.put("zookeeper.connect", configuration.getString(ZOOKEEPER_HOST_KEY) + ":" + configuration.getString(ZOOKEEPER_PORT_KEY));
        restConfig.put("listeners", "http://" + configuration.getString(CONFLUENT_REST_HOST_KEY) + ":" + configuration.getString(CONFLUENT_REST_PORT_KEY));
    }

    @Override
    public void loadConfig(Map<String, String> configs) {

    }

    private void build() {
        try {
            KafkaRestConfig config = new KafkaRestConfig(restConfig);

            restServer = new KafkaRestApplication(config);
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
