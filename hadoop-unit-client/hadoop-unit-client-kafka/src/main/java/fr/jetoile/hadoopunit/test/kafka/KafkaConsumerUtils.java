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
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Properties;

public enum KafkaConsumerUtils {
    INSTANCE;

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerUtils.class);

    private static final String KAFKA_HOSTNAME_KEY = "kafka.hostname";
    private static final String KAFKA_PORT_KEY = "kafka.port";


    private String kafkaHostname;
    private Integer kafkaPort;

    private Configuration configuration;

    long numRead = 0;

    KafkaConsumerUtils() {
        try {
            loadConfig();
        } catch (ConfigException e) {
            System.exit(-1);
        }
    }


    public void consumeMessagesWithOldApi(String topic, int nbMessageToRead) throws UnsupportedEncodingException {
        SimpleConsumer simpleConsumer = new SimpleConsumer(kafkaHostname,
                kafkaPort,
                30000,
                2,
                "test");

        System.out.println("Testing single fetch");
        kafka.api.FetchRequest req = new FetchRequestBuilder()
                .clientId("test")
                .addFetch(topic, 0, 0L, 100)
                .build();
        while (numRead != nbMessageToRead) {
            FetchResponse fetchResponse = simpleConsumer.fetch(req);
            printMessages(fetchResponse.messageSet(topic, 0));
            numRead++;
        }
    }


    public void consumeMessagesWithNewApi(String topic, int nbMessageToRead) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaHostname + ":" + kafkaPort);
        props.put("group.id", "test");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        try {
            while (numRead != nbMessageToRead) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    numRead++;
//                    LOG.debug("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    LOG.debug(record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }

    public long getNumRead() {
        return numRead;
    }

    private void printMessages(ByteBufferMessageSet messageSet) throws UnsupportedEncodingException {
        for (MessageAndOffset messageAndOffset : messageSet) {
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            LOG.debug(new String(bytes, "UTF-8"));
        }
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
