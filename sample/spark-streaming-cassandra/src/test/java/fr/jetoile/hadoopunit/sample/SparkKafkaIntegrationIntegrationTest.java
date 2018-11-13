package fr.jetoile.hadoopunit.sample;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.exception.NotFoundServiceException;
import fr.jetoile.hadoopunit.test.kafka.KafkaProducerUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static fr.jetoile.hadoopunit.client.commons.HadoopUnitClientConfig.*;

public class SparkKafkaIntegrationIntegrationTest {

    private static Configuration configuration;

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkKafkaIntegrationIntegrationTest.class);
    private static final FastDateFormat DATE_FORMATTER = FastDateFormat.getInstance("MM/dd/yyyy HH:mm:ss.SSS");

    @BeforeClass
    public static void setup() throws NotFoundServiceException, ConfigurationException {
        configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
    }

    @AfterClass
    public static void tearDown() throws NotFoundServiceException {
    }

    @Before
    public void setUp() throws NotFoundServiceException {
        Cluster cluster = Cluster.builder()
                .addContactPoints(configuration.getString(CASSANDRA_IP_KEY)).withPort(configuration.getInt(CASSANDRA_PORT_KEY)).build();
        Session session = cluster.connect();

        session.execute("create KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy' , 'replication_factor': '1' }");
        session.execute("CREATE TABLE test.orders (orderId text, clientId text, creationTime text, side text, quantity text, orderType text, PRIMARY KEY (orderId))");
        session.close();
        cluster.close();
    }

    @After
    public void teardown() throws NotFoundServiceException {
        Cluster cluster = Cluster.builder()
                .addContactPoints(configuration.getString(CASSANDRA_IP_KEY)).withPort(configuration.getInt(CASSANDRA_PORT_KEY)).build();
        Session session = cluster.connect();

        session.execute("drop table test.orders");
        session.execute("drop keyspace test");
        session.close();
        cluster.close();
    }


    @Test
    public void endToEndStreaming() throws InterruptedException {

        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .set("spark.cassandra.connection.host", configuration.getString(CASSANDRA_IP_KEY))
                .set("spark.cassandra.connection.port", configuration.getString(CASSANDRA_PORT_KEY))
                .setAppName("Integration Test");

        JavaStreamingContext scc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        SparkKafkaJob sparkKafkaJob = new SparkKafkaJob(scc);
        sparkKafkaJob.setTopic("testtopic");
        sparkKafkaJob.setZkString(configuration.getString(ZOOKEEPER_HOST_KEY) + ":" + configuration.getInt(ZOOKEEPER_PORT_KEY));
        sparkKafkaJob.run();
        scc.start();

        final Thread producerThread = new Thread(
                () -> {
                    for (int i = 1; i <= 100; i++) {
                        try {
                            Thread.sleep(200);
                        } catch (Exception e) {
                            //DO SOMETHING
                        }

                        String payload = generateMessage(i);
                        LOGGER.debug("Payload from producer: " + payload);

                        KafkaProducerUtils.INSTANCE.produceMessages(configuration.getString(KAFKA_TEST_TOPIC_KEY), String.valueOf(i), payload);
                    }
                });

        producerThread.start();

        producerThread.join();
        scc.awaitTerminationOrTimeout(5000);

        Cluster cluster = Cluster.builder()
                .addContactPoints(configuration.getString(CASSANDRA_IP_KEY)).withPort(configuration.getInt(CASSANDRA_PORT_KEY)).build();
        Session session = cluster.connect();

        System.out.println("===================================");
        List<Row> rows = session.execute("SELECT * FROM test.orders").all();
        rows.stream().forEach(
                System.out::println
        );
        session.close();
        cluster.close();
        System.out.println("===================================");
    }

    private String generateMessage(int i) {
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        JSONObject obj = new JSONObject();

        try {
            obj.put("orderid", String.valueOf(i));
            obj.put("clientid", String.valueOf(i));
            obj.put("creationtime", DATE_FORMATTER.format(new Date()));
            obj.put("side", rand.nextBoolean() ? "BUY" : "SELL");
            obj.put("quantity", rand.nextInt(1, 1000));
            obj.put("ordertype", rand.nextBoolean() ? "MARKET" : "LIMIT");
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return obj.toString();
    }

}