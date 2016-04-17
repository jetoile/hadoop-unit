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


import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.HadoopBootstrap;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import fr.jetoile.hadoopunit.test.kafka.KafkaConsumerUtils;
import fr.jetoile.hadoopunit.test.kafka.KafkaProducerUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class KafkaBootstrapTest {
    static private Logger LOGGER = LoggerFactory.getLogger(KafkaBootstrapTest.class);

    static private Configuration configuration;


    @BeforeClass
    public static void setup() throws Exception {
        try {
            configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
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
        for (int i = 0; i < 10; i++) {
            String payload = generateMessage(i);
            KafkaProducerUtils.INSTANCE.produceMessages(configuration.getString(HadoopUnitConfig.KAFKA_TEST_TOPIC_KEY), String.valueOf(i), payload);
        }



        // Consumer
//        KafkaConsumerUtils.INSTANCE.consumeMessagesWithOldApi(configuration.getString(HadoopUnitConfig.KAFKA_TEST_TOPIC_KEY), 10);
        KafkaConsumerUtils.INSTANCE.consumeMessagesWithNewApi(configuration.getString(HadoopUnitConfig.KAFKA_TEST_TOPIC_KEY), 10);

        // Assert num of messages produced = num of message consumed
        Assert.assertEquals(configuration.getLong(HadoopUnitConfig.KAFKA_TEST_MESSAGE_COUNT_KEY), KafkaConsumerUtils.INSTANCE.getNumRead());
    }

    private String generateMessage(int i) {
        JSONObject obj = new JSONObject();
        try {
            obj.put("id", String.valueOf(i));
            obj.put("msg", "test-message" + 1);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return obj.toString();
    }
}
