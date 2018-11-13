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

package fr.jetoile.hadoopunit.sample.kafka;

import fr.jetoile.hadoopunit.test.kafka.KafkaProducerUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.Serializable;

import static fr.jetoile.hadoopunit.client.commons.HadoopUnitClientConfig.*;

public class SparkKafkaIntegrationTest implements Serializable {

    private static Configuration configuration;

    @BeforeClass
    public static void setUp() throws ConfigurationException {
        configuration = new PropertiesConfiguration(DEFAULT_PROPS_FILE);
    }

    @Ignore
    @Test
    public void stark_should_read_kafka() throws InterruptedException {

        for (int i = 0; i < 10; i++) {
            String payload = generateMessge(i);
            KafkaProducerUtils.INSTANCE.produceMessages(configuration.getString(KAFKA_TEST_TOPIC_KEY), String.valueOf(i), payload);
        }

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("test");
        JavaStreamingContext scc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        SparkKafkaJob sparkKafkaJob = new SparkKafkaJob(scc);
        sparkKafkaJob.setTopic("testtopic");
        sparkKafkaJob.setZkString(configuration.getString(ZOOKEEPER_HOST_KEY) + ":" + configuration.getInt(ZOOKEEPER_PORT_KEY));

        sparkKafkaJob.run();

        scc.start();
        scc.awaitTermination();
    }

    private String generateMessge(int i) {
        JSONObject obj = new JSONObject();
        try {
            obj.put("id", String.valueOf(i));
            obj.put("msg", "test-message" + i);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return obj.toString();
    }
}
