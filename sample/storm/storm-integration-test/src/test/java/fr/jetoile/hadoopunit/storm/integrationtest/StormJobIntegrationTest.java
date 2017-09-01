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

package fr.jetoile.hadoopunit.storm.integrationtest;


import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import fr.jetoile.hadoopunit.storm.bolt.PrinterBolt;
import fr.jetoile.hadoopunit.storm.spout.RandomSentenceSpout;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.shade.org.json.simple.JSONValue;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.JarURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.jar.JarFile;

import static org.junit.Assert.assertTrue;

@Ignore
public class StormJobIntegrationTest {

    static private Configuration configuration;
    static private Logger LOGGER = LoggerFactory.getLogger(StormJobIntegrationTest.class);

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
    public void testStormCluster() throws Exception {
        org.apache.storm.Config stormConf = new org.apache.storm.Config();
        stormConf.setDebug(true);
        stormConf.put("nimbus-daemon", true);
        List<String> stormNimbusSeeds = new ArrayList<>();
        stormNimbusSeeds.add("localhost");
        stormConf.put(org.apache.storm.Config.NIMBUS_SEEDS, stormNimbusSeeds);
        stormConf.put(org.apache.storm.Config.NIMBUS_THRIFT_PORT, 6627);
        stormConf.put(org.apache.storm.Config.STORM_THRIFT_TRANSPORT_PLUGIN, "org.apache.storm.security.auth.SimpleTransportPlugin");
        stormConf.put(org.apache.storm.Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING, 60000);
        stormConf.put(org.apache.storm.Config.STORM_NIMBUS_RETRY_TIMES, 5);
        stormConf.put(org.apache.storm.Config.STORM_NIMBUS_RETRY_INTERVAL, 2000);
        stormConf.put(org.apache.storm.Config.NIMBUS_THRIFT_MAX_BUFFER_SIZE, 1048576);
        stormConf.put(org.apache.storm.Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(configuration.getString(HadoopUnitConfig.ZOOKEEPER_HOST_KEY)));
        stormConf.put(org.apache.storm.Config.STORM_ZOOKEEPER_PORT, configuration.getInt(HadoopUnitConfig.ZOOKEEPER_PORT_KEY));
//        stormConf.put(org.apache.storm.Config.STORM_ZOOKEEPER_PORT, 2181);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("randomsentencespout", new RandomSentenceSpout(), 1);
        builder.setBolt("print", new PrinterBolt(), 1).shuffleGrouping("randomsentencespout");

//        URL url = ManualIntegrationBootstrapTest.class.getClassLoader().getResource("fr/jetoile/hadoopunit/storm/bolt/PrinterBolt.class");
//        URL url = StormJobIntegrationTest.class.getClassLoader().getResource("org/apache/storm/bolt/JoinBolt.class");
//        JarURLConnection connection = (JarURLConnection) url.openConnection();
//        JarFile file = connection.getJarFile();
//        String jarPath = file.getName();
//        System.setProperty("storm.jar", jarPath);



        StormSubmitter submitter = new StormSubmitter();

        String s = StormSubmitter.submitJar(stormConf, "/home/khanh/github/hadoop-bootstrap/sample/storm/storm-job/target/storm-job-2.3-SNAPSHOT.jar");
        NimbusClient nimbusClient = NimbusClient.getConfiguredClient(stormConf);
        String jsonConf = JSONValue.toJSONString(stormConf);
        nimbusClient.getClient().submitTopology("testtopology", s, jsonConf, builder.createTopology());


//        submitter.submitTopology(configuration.getString(HadoopUnitConfig.STORM_TOPOLOGY_NAME_KEY) + "_", stormConf, builder.createTopology());
//        submitter.submitTopology(configuration.getString(HadoopUnitConfig.STORM_TOPOLOGY_NAME_KEY) + "_", stormConf, builder.createTopology());

        try {
            Thread.sleep(5000L);
        } catch (InterruptedException e) {
            LOGGER.info("SUCCESSFULLY COMPLETED");
        }



////        TopologyBuilder builder = new TopologyBuilder();
//        Config conf = new Config();
//        conf.put(Config.NIMBUS_HOST, "localhost");
//        conf.setDebug(true);
//        Map storm_conf = Utils.readStormConfig();
//        storm_conf.put("nimbus.host", "localhost");
////        Nimbus.Client client = NimbusClient.getConfiguredClient(storm_conf).getClient();
//        String inputJar = "/home/khanh/github/hadoop-bootstrap/sample/storm/storm-job/target/storm-job-2.3-SNAPSHOT.jar";
//        NimbusClient nimbus = new NimbusClient(storm_conf, "localhost", 6627);
//        // upload topology jar to Cluster using StormSubmitter
//        String uploadedJarLocation = StormSubmitter.submitJar(storm_conf, inputJar);
//        try {
//            String jsonConf = JSONValue.toJSONString(storm_conf);
//            nimbus.getClient().submitTopology("testtopology", uploadedJarLocation, jsonConf, builder.createTopology());
//        } catch (AlreadyAliveException ae) {
//            ae.printStackTrace();
//        }
//        Thread.sleep(60000);
    }

    @Test
    public void testStormNimbusClient() throws Exception {
        org.apache.storm.Config stormConf = new org.apache.storm.Config();
        stormConf.put("nimbus-daemon", true);
        List<String> stormNimbusSeeds = new ArrayList<>();
        stormNimbusSeeds.add("localhost");
        stormConf.put(org.apache.storm.Config.NIMBUS_SEEDS, stormNimbusSeeds);
        stormConf.put(org.apache.storm.Config.NIMBUS_THRIFT_PORT, 6627);
        stormConf.put(org.apache.storm.Config.STORM_THRIFT_TRANSPORT_PLUGIN, "org.apache.storm.security.auth.SimpleTransportPlugin");
        stormConf.put(org.apache.storm.Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING, 60000);
        stormConf.put(org.apache.storm.Config.STORM_NIMBUS_RETRY_TIMES, 5);
        stormConf.put(org.apache.storm.Config.STORM_NIMBUS_RETRY_INTERVAL, 2000);
        stormConf.put(org.apache.storm.Config.NIMBUS_THRIFT_MAX_BUFFER_SIZE, 1048576);
        stormConf.put(org.apache.storm.Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(configuration.getString(HadoopUnitConfig.ZOOKEEPER_HOST_KEY)));
        stormConf.put(org.apache.storm.Config.STORM_ZOOKEEPER_PORT, configuration.getInt(HadoopUnitConfig.ZOOKEEPER_PORT_KEY));

        NimbusClient nimbusClient = NimbusClient.getConfiguredClient(stormConf);
        assertTrue(nimbusClient.getClient().getNimbusConf().length() > 0);
    }

}

