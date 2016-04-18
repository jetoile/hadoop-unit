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


import fr.jetoile.hadoopunit.Config;
import fr.jetoile.hadoopunit.HadoopBootstrap;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import fr.jetoile.hadoopunit.kafka.consumer.KafkaTestConsumer;
import fr.jetoile.hadoopunit.kafka.producer.KafkaTestProducer;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class KafkaBootstrapTest {
    static private Logger LOGGER = LoggerFactory.getLogger(KafkaBootstrapTest.class);

    static private Configuration configuration;


    @BeforeClass
    public static void setup() throws Exception {
        try {
            configuration = new PropertiesConfiguration(Config.DEFAULT_PROPS_FILE);
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }

        HadoopBootstrap.INSTANCE.startAll();

    }

    @AfterClass
    public static void tearDown() throws Exception {
        HadoopBootstrap.INSTANCE.stopAll();

    }


    @Test
    public void kafkaShouldStart() throws Exception {

        // Producer
        KafkaTestProducer kafkaTestProducer = new KafkaTestProducer.Builder()
                .setKafkaHostname(configuration.getString(Config.KAFKA_HOSTNAME_KEY))
                .setKafkaPort(configuration.getInt(Config.KAFKA_PORT_KEY))
                .setTopic(configuration.getString(Config.KAFKA_TEST_TOPIC_KEY))
                .setMessageCount(configuration.getInt(Config.KAFKA_TEST_MESSAGE_COUNT_KEY))
                .build();
        kafkaTestProducer.produceMessages();


        // Consumer
        List<String> seeds = new ArrayList<String>();
        seeds.add(configuration.getString(Config.KAFKA_HOSTNAME_KEY));
        KafkaTestConsumer kafkaTestConsumer = new KafkaTestConsumer();
        kafkaTestConsumer.consumeMessages2(
                configuration.getInt(Config.KAFKA_TEST_MESSAGE_COUNT_KEY),
                configuration.getString(Config.KAFKA_TEST_TOPIC_KEY),
                0,
                seeds,
                configuration.getInt(Config.KAFKA_PORT_KEY));

        // Assert num of messages produced = num of message consumed
        Assert.assertEquals(configuration.getLong(Config.KAFKA_TEST_MESSAGE_COUNT_KEY),
                kafkaTestConsumer.getNumRead());
    }
}
