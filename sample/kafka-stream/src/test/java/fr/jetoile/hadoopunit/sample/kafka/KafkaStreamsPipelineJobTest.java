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

import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.test.kafka.KafkaConsumerUtils;
import fr.jetoile.hadoopunit.test.kafka.KafkaProducerUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class KafkaStreamsPipelineJobTest {

    private static Configuration configuration;

    @BeforeClass
    public static void setUp() throws ConfigurationException {
        configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
    }

    @Test
    public void stark_should_read_kafka() throws InterruptedException {

        for (int i = 0; i < 10; i++) {
            String payload = generateMessge(i);
            KafkaProducerUtils.INSTANCE.produceMessages("testtopic", String.valueOf(i), payload);
        }

        KafkaStreamsPipelineJob kafkaStreamJob = new KafkaStreamsPipelineJob();
        kafkaStreamJob.setBroker(configuration.getString(HadoopUnitConfig.KAFKA_HOSTNAME_KEY) + ":" + configuration.getString(HadoopUnitConfig.KAFKA_PORT_KEY));
        kafkaStreamJob.setInputTopic("testtopic");
        kafkaStreamJob.setOutputTopic("output");

        kafkaStreamJob.run();

        // Consumer
        KafkaConsumerUtils.INSTANCE.consumeMessagesWithNewApi("output", 10);

        // Assert num of messages produced = num of message consumed
        Assert.assertEquals(10, KafkaConsumerUtils.INSTANCE.getNumRead());
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