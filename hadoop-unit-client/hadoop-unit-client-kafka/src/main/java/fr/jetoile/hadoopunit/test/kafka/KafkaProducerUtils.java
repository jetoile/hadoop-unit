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
package fr.jetoile.hadoopunit.test.kafka;

import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.exception.ConfigException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public enum KafkaProducerUtils {
    INSTANCE;

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerUtils.class);

    private static final String KAFKA_HOSTNAME_KEY = "kafka.hostname";
    private static final String KAFKA_PORT_KEY = "kafka.port";


    private String kafkaHostname;
    private Integer kafkaPort;
    private Properties props;

    private Configuration configuration;

    KafkaProducerUtils() {
        try {
            loadConfig();
            props = new Properties();
            props.put("bootstrap.servers", kafkaHostname + ":" + kafkaPort);
            props.put("acks", "all");
            props.put("retries", 0);
            props.put("batch.size", 10);
            props.put("linger.ms", 1);
            props.put("buffer.memory", 33554432);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        } catch (ConfigException e) {
            System.exit(-1);
        }
    }

    public void produceMessages(String topic, String key, String message) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        producer.send(new ProducerRecord<>(topic, key, message));
        producer.close();
    }

    private void loadConfig() throws ConfigException {
        try {
            configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
        } catch (ConfigurationException e) {
            throw new ConfigException("bad config", e);
        }

        kafkaHostname = configuration.getString(KAFKA_HOSTNAME_KEY);
        kafkaPort = configuration.getInt(KAFKA_PORT_KEY);
    }

}
