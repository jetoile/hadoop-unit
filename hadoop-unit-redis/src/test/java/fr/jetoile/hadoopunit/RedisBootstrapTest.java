package fr.jetoile.hadoopunit;

import fr.jetoile.hadoopunit.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;

import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class RedisBootstrapTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisBootstrapTest.class);

    static private Configuration configuration;


    @BeforeClass
    public static void setup() throws BootstrapException {
        HadoopBootstrap.INSTANCE.startAll();

        try {
            configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }
    }


    @AfterClass
    public static void tearDown() throws BootstrapException {
        HadoopBootstrap.INSTANCE.stopAll();
    }


    @Test
    public void testStartAndStopServerMode() throws InterruptedException {
        Jedis jedis = new Jedis("127.0.0.1", configuration.getInt(HadoopUnitConfig.REDIS_PORT_KEY));
        Assert.assertNotNull(jedis.info());
        System.out.println(jedis.info());
        jedis.close();
    }

    @Test
    @Ignore
    public void testStartAndStopClusterMode() throws InterruptedException {
        Jedis jedis = new Jedis("127.0.0.1", configuration.getInt(HadoopUnitConfig.REDIS_PORT_KEY));

        Assert.assertNotNull(jedis.clusterInfo());
        Assert.assertNotNull(jedis.clusterInfo().contains("cluster_state:ok"));
        Assert.assertNotNull(jedis.clusterInfo().contains("cluster_known_nodes:3"));
        Assert.assertNotNull(jedis.clusterInfo().contains("cluster_size:3"));

        jedis.close();
    }

    @Test
    @Ignore
    public void testStartAndStopMasterSlaveMode() throws InterruptedException {

        Jedis master = new Jedis("127.0.0.1", configuration.getInt(HadoopUnitConfig.REDIS_PORT_KEY));
        Assert.assertTrue(master.info("Replication").contains("role:master"));
        master.close();

        configuration.getList(HadoopUnitConfig.REDIS_SLAVE_PORT_KEY).stream().forEach(s -> {

            Jedis slave = new Jedis("127.0.0.1", Integer.valueOf((String) s));
            Assert.assertTrue(slave.info("Replication").contains("role:slave"));
            Assert.assertTrue(slave.info("Replication").contains("master_host:127.0.0.1"));
            Assert.assertTrue(slave.info("Replication").contains("master_port:" + configuration.getInt(HadoopUnitConfig.REDIS_PORT_KEY)));
            slave.close();
        });
    }

    @Test
    @Ignore
    public void testStartAndStopSentinelMode() throws InterruptedException {

        Set<String> sentinels = new HashSet<>();

        List<Object> sentinelPorts = configuration.getList(HadoopUnitConfig.REDIS_SENTINEL_PORT_KEY);
        sentinelPorts.stream().forEach(s -> {
            sentinels.add(new HostAndPort("127.0.0.1", Integer.valueOf((String) s)).toString());
        });

        Jedis connectToSentinel = new Jedis("127.0.0.1", Integer.valueOf((String) sentinelPorts.get(0)));
        Assert.assertNotNull(connectToSentinel.info("Sentinel"));
        Assert.assertTrue(connectToSentinel.info("Sentinel").contains("slaves=1"));
        Assert.assertTrue(connectToSentinel.info("Sentinel").contains("sentinels=3"));
        connectToSentinel.close();

        JedisSentinelPool sentinelPool = new JedisSentinelPool("mymaster", sentinels);
        Jedis connectToMaster = sentinelPool.getResource();

        Assert.assertNotNull(connectToMaster.info());
        Assert.assertTrue(connectToMaster.info("Replication").contains("role:master"));
        connectToMaster.close();
        sentinelPool.close();

    }
}
