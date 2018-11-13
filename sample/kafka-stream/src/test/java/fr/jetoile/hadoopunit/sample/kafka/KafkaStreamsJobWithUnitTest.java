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

import fr.jetoile.hadoopunit.HadoopBootstrap;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.component.KafkaConfig;
import fr.jetoile.hadoopunit.test.kafka.KafkaProducerUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static fr.jetoile.hadoopunit.client.commons.HadoopUnitClientConfig.DEFAULT_PROPS_FILE;
import static fr.jetoile.hadoopunit.client.commons.HadoopUnitClientConfig.KAFKA_HOSTNAME_KEY;
import static fr.jetoile.hadoopunit.client.commons.HadoopUnitClientConfig.KAFKA_PORT_KEY;

@Ignore
public class KafkaStreamsJobWithUnitTest {
    private static Configuration configuration;

    @BeforeClass
    public static void setUp() throws ConfigurationException {
        configuration = new PropertiesConfiguration(DEFAULT_PROPS_FILE);
        HadoopBootstrap.INSTANCE.startAll();
    }

    @AfterClass
    public static void tearDown() {
        HadoopBootstrap.INSTANCE.stopAll();
    }

    @Test
    public void stark_should_read_kafka() throws InterruptedException {

        for (int i = 0; i < 10; i++) {
            String payload = generateMessge(i);
            KafkaProducerUtils.INSTANCE.produceMessages("testtopic", String.valueOf(i), payload);
        }

        KafkaStreamsJob kafkaStreamJob = new KafkaStreamsJob();
        kafkaStreamJob.setBroker(configuration.getString(KAFKA_HOSTNAME_KEY) + ":" + configuration.getString(KAFKA_PORT_KEY));
        kafkaStreamJob.setTopic("testtopic");

        kafkaStreamJob.run();
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
