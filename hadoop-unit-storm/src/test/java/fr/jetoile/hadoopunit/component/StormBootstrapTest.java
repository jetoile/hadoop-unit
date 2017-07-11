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

import com.github.sakserv.minicluster.impl.StormLocalCluster;
import fr.jetoile.hadoopunit.Component;
import fr.jetoile.hadoopunit.HadoopBootstrap;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.component.storm.bolt.PrinterBolt;
import fr.jetoile.hadoopunit.component.storm.spout.RandomSentenceSpout;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.NimbusClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.JarURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.jar.JarFile;

import static org.junit.Assert.assertTrue;

public class StormBootstrapTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(StormBootstrapTest.class);

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

//    @Before
//    public void setUp() throws NotFoundServiceException {
//        Bootstrap storm = HadoopBootstrap.INSTANCE.getService(Component.STORM);
//    }
//
//    @After
//    public void teardown() throws NotFoundServiceException {
//        Bootstrap storm = HadoopBootstrap.INSTANCE.getService(Component.STORM);
//    }

    @Test
    public void stormCluster_should_submitTopology() throws Exception {
        Bootstrap storm = HadoopBootstrap.INSTANCE.getService(Component.STORM);
        StormLocalCluster stormLocalCluster = ((StormBootstrap) storm).getStormLocalCluster();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("randomsentencespout", new RandomSentenceSpout(), 1);
        builder.setBolt("print", new PrinterBolt(), 1).shuffleGrouping("randomsentencespout");
        stormLocalCluster.submitTopology(configuration.getString(HadoopUnitConfig.STORM_TOPOLOGY_NAME_KEY),
                stormLocalCluster.getStormConf(), builder.createTopology());

        try {
            Thread.sleep(5000L);
        } catch (InterruptedException e) {
            LOGGER.info("SUCCESSFULLY COMPLETED");
        }
    }

    @Test
    public void stormCluster_should_have_a_client() throws Exception {
        Bootstrap storm = HadoopBootstrap.INSTANCE.getService(Component.STORM);
        StormLocalCluster stormLocalCluster = ((StormBootstrap) storm).getStormLocalCluster();

        Config conf = stormLocalCluster.getStormConf();
        NimbusClient nimbusClient = NimbusClient.getConfiguredClient(conf);
        assertTrue(nimbusClient.getClient().getNimbusConf().length() > 0);
    }


    @Test
    public void testStormCluster() throws Exception {
        Config stormConf = new Config();
        stormConf.put("nimbus-daemon", true);
        List<String> stormNimbusSeeds = new ArrayList<>();
        stormNimbusSeeds.add(configuration.getString(HadoopUnitConfig.STORM_HOST_KEY));
        stormConf.put(Config.NIMBUS_SEEDS, stormNimbusSeeds);
        stormConf.put(Config.NIMBUS_THRIFT_PORT, configuration.getInt(HadoopUnitConfig.STORM_PORT_KEY));
        stormConf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN, "org.apache.storm.security.auth.SimpleTransportPlugin");
        stormConf.put(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING, 60000);
        stormConf.put(Config.STORM_NIMBUS_RETRY_TIMES, 5);
        stormConf.put(Config.STORM_NIMBUS_RETRY_INTERVAL, 2000);
        stormConf.put(Config.NIMBUS_THRIFT_MAX_BUFFER_SIZE, 1048576);
        stormConf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(configuration.getString(HadoopUnitConfig.ZOOKEEPER_HOST_KEY)));
        stormConf.put(Config.STORM_ZOOKEEPER_PORT, configuration.getInt(HadoopUnitConfig.ZOOKEEPER_PORT_KEY));

        stormConf.put(Config.STORM_LOCAL_DIR, "/tmp/storm1");
        stormConf.put(Config.STORM_LOCAL_HOSTNAME, "localhost");
        stormConf.put(Config.NIMBUS_THRIFT_PORT, configuration.getInt(HadoopUnitConfig.STORM_PORT_KEY));

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("randomsentencespout", new RandomSentenceSpout(), 1);
        builder.setBolt("print", new PrinterBolt(), 1).shuffleGrouping("randomsentencespout");

        URL url = StormBootstrapTest.class.getClassLoader().getResource("org/apache/storm/bolt/JoinBolt.class");
        JarURLConnection connection = (JarURLConnection) url.openConnection();
        JarFile file = connection.getJarFile();
        String jarPath = file.getName();
        System.setProperty("storm.jar", jarPath);

        StormSubmitter submitter = new StormSubmitter();
        submitter.submitTopology(configuration.getString(HadoopUnitConfig.STORM_TOPOLOGY_NAME_KEY)+"_", stormConf, builder.createTopology());

        try {
            Thread.sleep(5000L);
        } catch (InterruptedException e) {
            LOGGER.info("SUCCESSFULLY COMPLETED");
        }
    }

    @Test
    public void testStormNimbusClient() throws Exception {


        Config stormConf = new Config();
        stormConf.put("nimbus-daemon", true);
        List<String> stormNimbusSeeds = new ArrayList<>();
        stormNimbusSeeds.add(configuration.getString(HadoopUnitConfig.STORM_HOST_KEY));
        stormConf.put(Config.NIMBUS_SEEDS, stormNimbusSeeds);
        stormConf.put(Config.NIMBUS_THRIFT_PORT, configuration.getInt(HadoopUnitConfig.STORM_PORT_KEY));
        stormConf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN, "org.apache.storm.security.auth.SimpleTransportPlugin");
        stormConf.put(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING, 60000);
        stormConf.put(Config.STORM_NIMBUS_RETRY_TIMES, 5);
        stormConf.put(Config.STORM_NIMBUS_RETRY_INTERVAL, 2000);
        stormConf.put(Config.NIMBUS_THRIFT_MAX_BUFFER_SIZE, 1048576);
        stormConf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(configuration.getString(HadoopUnitConfig.ZOOKEEPER_HOST_KEY)));
        stormConf.put(Config.STORM_ZOOKEEPER_PORT, configuration.getInt(HadoopUnitConfig.ZOOKEEPER_PORT_KEY));

        NimbusClient nimbusClient = NimbusClient.getConfiguredClient(stormConf);
        assertTrue(nimbusClient.getClient().getNimbusConf().length() > 0);
    }
}