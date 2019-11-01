package fr.jetoile.hadoopunit.sample.pulsar;

import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pulsar.client.api.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class PulsarIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarIntegrationTest.class);
    private static final String TOPIC = "hello";
    private static final int NUM_OF_MESSAGES = 100;
    private static final String PULSAR_IP_CLIENT_KEY = "pulsar.client.ip";
    private static final String PULSAR_PORT_KEY = "pulsar.port";
    private static final String PULSAR_HTTP_PORT_KEY = "pulsar.http.port";
    static private Configuration configuration;

    @BeforeClass
    public static void setup() throws BootstrapException {
        try {
            configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }
    }


    @AfterClass
    public static void tearDown() throws BootstrapException {
    }

    @Test
    public void pulsarShouldStart() throws PulsarClientException {
        final PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://" + configuration.getString(PULSAR_IP_CLIENT_KEY) + ":" + configuration.getInt(PULSAR_PORT_KEY))
                .build();

        final Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(TOPIC)
                .enableBatching(false)
                .create();

        final Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(TOPIC)
                .subscriptionName("test-subs-1")
                .ackTimeout(10, TimeUnit.SECONDS)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();

        for (int i = 1; i <= NUM_OF_MESSAGES; ++i) {
            producer.send("Hello_" + i);
        }

        for (int i = 1; i <= NUM_OF_MESSAGES; ++i) {
            final Message<String> message = consumer.receive(1, TimeUnit.SECONDS);
            LOGGER.info("Message received : {}", message.getValue());
            assertThat(message.getValue()).isEqualTo("Hello_" + i);
        }

        producer.close();
        consumer.close();
        client.close();
    }

    @Test
    public void functionWorker_should_response() {
        Client client = ClientBuilder.newClient();

        String response = client.target("http://" + configuration.getString(PULSAR_IP_CLIENT_KEY) + ":" + configuration.getInt(PULSAR_HTTP_PORT_KEY) + "/admin/v2/worker/cluster").request().get(String.class);

        assertThat(response).isEqualTo("[{\"workerId\":\"c-pulsar-cluster-1-fw-127.0.0.1-22023\",\"workerHostname\":\"127.0.0.1\",\"port\":22023}]");
    }
}
