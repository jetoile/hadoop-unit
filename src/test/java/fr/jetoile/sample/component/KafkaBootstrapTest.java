package fr.jetoile.sample.component;


import com.github.sakserv.minicluster.config.ConfigVars;
import fr.jetoile.sample.BootstrapException;
import fr.jetoile.sample.kafka.consumer.KafkaTestConsumer;
import fr.jetoile.sample.kafka.producer.KafkaTestProducer;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class KafkaBootstrapTest {
    static private Logger LOGGER = LoggerFactory.getLogger(KafkaBootstrapTest.class);

    static private Bootstrap zookeeper;
    static private Bootstrap kafka;
    static private Configuration configuration;


    @BeforeClass
    public static void setup() throws Exception {
        try {
            configuration = new PropertiesConfiguration("default.properties");
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }

        zookeeper = ZookeeperBootstrap.INSTANCE.start();
        kafka = KafkaBootstrap.INSTANCE.start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        kafka.stop();
        zookeeper.stop();
    }


    @Test
    public void kafkaShouldStart() throws Exception {

        // Producer
        KafkaTestProducer kafkaTestProducer = new KafkaTestProducer.Builder()
                .setKafkaHostname(configuration.getString(ConfigVars.KAFKA_HOSTNAME_KEY))
                .setKafkaPort(configuration.getInt(ConfigVars.KAFKA_PORT_KEY))
                .setTopic(configuration.getString(ConfigVars.KAFKA_TEST_TOPIC_KEY))
                .setMessageCount(configuration.getInt(ConfigVars.KAFKA_TEST_MESSAGE_COUNT_KEY))
                .build();
        kafkaTestProducer.produceMessages();


        // Consumer
        List<String> seeds = new ArrayList<String>();
        seeds.add(configuration.getString(ConfigVars.KAFKA_HOSTNAME_KEY));
        KafkaTestConsumer kafkaTestConsumer = new KafkaTestConsumer();
        kafkaTestConsumer.consumeMessages(
                configuration.getInt(ConfigVars.KAFKA_TEST_MESSAGE_COUNT_KEY),
                configuration.getString(ConfigVars.KAFKA_TEST_TOPIC_KEY),
                0,
                seeds,
                configuration.getInt(ConfigVars.KAFKA_PORT_KEY));

        // Assert num of messages produced = num of message consumed
        Assert.assertEquals(configuration.getLong(ConfigVars.KAFKA_TEST_MESSAGE_COUNT_KEY),
                kafkaTestConsumer.getNumRead());
    }
}
