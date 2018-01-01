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

import fr.jetoile.hadoopunit.HadoopBootstrap;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;

import static fr.jetoile.hadoopunit.HadoopUnitConfig.*;
import static org.fest.assertions.Assertions.assertThat;

public class ConfluentBootstrapTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfluentBootstrapTest.class);

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
    public void schemaRegistry_should_be_ok() {
        Client client = ClientBuilder.newClient();
        String response = client.target("http://" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_HOST_KEY) + ":" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_PORT_KEY) + "/subjects/Kafka-key/versions")
                .request("application/vnd.schemaregistry.v1+json")
                .post(Entity.entity("{\"schema\": \"{\\\"type\\\": \\\"string\\\"}\"}", "application/vnd.schemaregistry.v1+json"), String.class);
        assertThat(response).isEqualToIgnoringCase("{\"id\":1}");

        response = client.target("http://" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_HOST_KEY) + ":" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_PORT_KEY) + "/subjects/Kafka-value/versions")
                .request("application/vnd.schemaregistry.v1+json")
                .post(Entity.entity("{\"schema\": \"{\\\"type\\\": \\\"string\\\"}\"}", "application/vnd.schemaregistry.v1+json"), String.class);
        assertThat(response).isEqualToIgnoringCase("{\"id\":1}");

        response = client.target("http://" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_HOST_KEY) + ":" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_PORT_KEY) + "/subjects")
                .request("application/vnd.schemaregistry.v1+json")
                .get(String.class);
        assertThat(response).isEqualToIgnoringCase("[\"Kafka-value\",\"Kafka-key\"]");

        response = client.target("http://" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_HOST_KEY) + ":" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_PORT_KEY) + "/schemas/ids/1")
                .request("application/vnd.schemaregistry.v1+json")
                .get(String.class);
        assertThat(response).isEqualToIgnoringCase("{\"schema\":\"\\\"string\\\"\"}");

        response = client.target("http://" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_HOST_KEY) + ":" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_PORT_KEY) + "/subjects/Kafka-value/versions")
                .request("application/vnd.schemaregistry.v1+json")
                .get(String.class);
        assertThat(response).isEqualToIgnoringCase("[1]");

        response = client.target("http://" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_HOST_KEY) + ":" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_PORT_KEY) + "/subjects/Kafka-value/versions/1")
                .request("application/vnd.schemaregistry.v1+json")
                .get(String.class);
        assertThat(response).isEqualToIgnoringCase("{\"subject\":\"Kafka-value\",\"version\":1,\"id\":1,\"schema\":\"\\\"string\\\"\"}");

        response = client.target("http://" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_HOST_KEY) + ":" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_PORT_KEY) + "/subjects/Kafka-value/versions/1")
                .request("application/vnd.schemaregistry.v1+json")
                .delete(String.class);
        assertThat(response).isEqualToIgnoringCase("1");

        response = client.target("http://" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_HOST_KEY) + ":" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_PORT_KEY) + "/subjects/Kafka-value/versions")
                .request("application/vnd.schemaregistry.v1+json")
                .post(Entity.entity("{\"schema\": \"{\\\"type\\\": \\\"string\\\"}\"}", "application/vnd.schemaregistry.v1+json"), String.class);
        assertThat(response).isEqualToIgnoringCase("{\"id\":1}");

        response = client.target("http://" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_HOST_KEY) + ":" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_PORT_KEY) + "/subjects/Kafka-value/versions/latest")
                .request("application/vnd.schemaregistry.v1+json")
                .delete(String.class);
        assertThat(response).isEqualToIgnoringCase("2");

        response = client.target("http://" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_HOST_KEY) + ":" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_PORT_KEY) + "/subjects/Kafka-value/versions")
                .request("application/vnd.schemaregistry.v1+json")
                .post(Entity.entity("{\"schema\": \"{\\\"type\\\": \\\"string\\\"}\"}", "application/vnd.schemaregistry.v1+json"), String.class);
        assertThat(response).isEqualToIgnoringCase("{\"id\":1}");

        response = client.target("http://" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_HOST_KEY) + ":" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_PORT_KEY) + "/schemas/ids/1")
                .request("application/vnd.schemaregistry.v1+json")
                .get(String.class);
        assertThat(response).isEqualToIgnoringCase("{\"schema\":\"\\\"string\\\"\"}");

        response = client.target("http://" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_HOST_KEY) + ":" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_PORT_KEY) + "/subjects/Kafka-key")
                .request("application/vnd.schemaregistry.v1+json")
                .post(Entity.entity("{\"schema\": \"{\\\"type\\\": \\\"string\\\"}\"}", "application/vnd.schemaregistry.v1+json"), String.class);
        assertThat(response).isEqualToIgnoringCase("{\"subject\":\"Kafka-key\",\"version\":1,\"id\":1,\"schema\":\"\\\"string\\\"\"}");

        response = client.target("http://" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_HOST_KEY) + ":" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_PORT_KEY) + "/compatibility/subjects/Kafka-value/versions/latest")
                .request("application/vnd.schemaregistry.v1+json")
                .post(Entity.entity("{\"schema\": \"{\\\"type\\\": \\\"string\\\"}\"}", "application/vnd.schemaregistry.v1+json"), String.class);
        assertThat(response).isEqualToIgnoringCase("{\"is_compatible\":true}");

        response = client.target("http://" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_HOST_KEY) + ":" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_PORT_KEY) + "/config")
                .request("application/vnd.schemaregistry.v1+json")
                .get(String.class);
        assertThat(response).isEqualToIgnoringCase("{\"compatibilityLevel\":\"BACKWARD\"}");

        response = client.target("http://" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_HOST_KEY) + ":" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_PORT_KEY) + "/config")
                .request("application/vnd.schemaregistry.v1+json")
                .put(Entity.entity("{\"compatibility\": \"NONE\"}", "application/vnd.schemaregistry.v1+json"), String.class);
        assertThat(response).isEqualToIgnoringCase("{\"compatibility\":\"NONE\"}");

        response = client.target("http://" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_HOST_KEY) + ":" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_PORT_KEY) + "/config/Kafka-value")
                .request("application/vnd.schemaregistry.v1+json")
                .put(Entity.entity("{\"compatibility\": \"BACKWARD\"}", "application/vnd.schemaregistry.v1+json"), String.class);
        assertThat(response).isEqualToIgnoringCase("{\"compatibility\":\"BACKWARD\"}");

        response = client.target("http://" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_HOST_KEY) + ":" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_PORT_KEY) + "/subjects/Kafka-value")
                .request("application/vnd.schemaregistry.v1+json")
                .delete(String.class);
        assertThat(response).isEqualToIgnoringCase("[3]");

        response = client.target("http://" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_HOST_KEY) + ":" + configuration.getString(CONFLUENT_SCHEMAREGISTRY_PORT_KEY) + "/subjects")
                .request("application/vnd.schemaregistry.v1+json")
                .get(String.class);
        assertThat(response).isEqualToIgnoringCase("[\"Kafka-key\"]");
    }

    @Test
    public void kafkaRest_should_be_ok() {

        Client client = ClientBuilder.newClient();
        String response = client.target("http://" + configuration.getString(CONFLUENT_REST_HOST_KEY) + ":" + configuration.getString(CONFLUENT_REST_PORT_KEY) + "/topics/jsontest")
                .request("application/vnd.kafka.v2+json")
                .accept("application/vnd.kafka.v2+json")
                .post(Entity.entity("{\"records\":[{\"value\":{\"foo\":\"bar\"}}]}", "application/vnd.kafka.json.v2+json"), String.class);
        assertThat(response).isEqualToIgnoringCase("{\"offsets\":[{\"partition\":0,\"offset\":0,\"error_code\":null,\"error\":null}],\"key_schema_id\":null,\"value_schema_id\":null}");

        response = client.target("http://" + configuration.getString(CONFLUENT_REST_HOST_KEY) + ":" + configuration.getString(CONFLUENT_REST_PORT_KEY) + "/consumers/my_json_consumer")
                .request("application/vnd.kafka.v2+json")
                .post(Entity.entity("{\"name\": \"my_consumer_instance\", \"format\": \"json\", \"auto.offset.reset\": \"earliest\"}", "application/vnd.kafka.json.v2+json"), String.class);
        assertThat(response).isEqualToIgnoringCase("{\"instance_id\":\"my_consumer_instance\",\"base_uri\":\"http://127.0.0.1:8082/consumers/my_json_consumer/instances/my_consumer_instance\"}");

        client.target("http://" + configuration.getString(CONFLUENT_REST_HOST_KEY) + ":" + configuration.getString(CONFLUENT_REST_PORT_KEY) + "/consumers/my_json_consumer/instances/my_consumer_instance/subscription")
                .request("application/vnd.kafka.v2+json")
                .post(Entity.entity("{\"topics\":[\"jsontest\"]}", "application/vnd.kafka.json.v2+json"), String.class);

        client.target("http://" + configuration.getString(CONFLUENT_REST_HOST_KEY) + ":" + configuration.getString(CONFLUENT_REST_PORT_KEY) + "/consumers/my_json_consumer/instances/my_consumer_instance/records")
                .request("application/vnd.kafka.json.v2+json")
                .get(String.class);

        response = client.target("http://" + configuration.getString(CONFLUENT_REST_HOST_KEY) + ":" + configuration.getString(CONFLUENT_REST_PORT_KEY) + "/consumers/my_json_consumer/instances/my_consumer_instance/records")
                .request("application/vnd.kafka.json.v2+json")
                .get(String.class);
        assertThat(response).isEqualToIgnoringCase("[{\"topic\":\"jsontest\",\"key\":null,\"value\":{\"foo\":\"bar\"},\"partition\":0,\"offset\":0}]");

        client.target("http://" + configuration.getString(CONFLUENT_REST_HOST_KEY) + ":" + configuration.getString(CONFLUENT_REST_PORT_KEY) + "/consumers/my_json_consumer/instances/my_consumer_instance")
                .request("application/vnd.kafka.v2+json")
                .delete(String.class);
    }
}