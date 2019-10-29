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

public class ConfluentConfig {

    //Confluent
    public static final String CONFLUENT_SCHEMAREGISTRY_PORT_KEY = "confluent.schemaregistry.port";
    public static final String CONFLUENT_SCHEMAREGISTRY_HOST_KEY = "confluent.schemaregistry.host";
    public static final String CONFLUENT_SCHEMAREGISTRY_TOPIC_KEY = "confluent.schemaregistry.kafkastore.topic";
    public static final String CONFLUENT_SCHEMAREGISTRY_DEBUG_KEY = "confluent.schemaregistry.debug";

    public static final String CONFLUENT_KAFKA_LOG_DIR_KEY = "confluent.kafka.log.dirs";
    public static final String CONFLUENT_KAFKA_BROKER_ID_KEY = "confluent.kafka.broker.id";
    public static final String CONFLUENT_KAFKA_PORT_KEY = "confluent.kafka.port";
    public static final String CONFLUENT_KAFKA_HOST_KEY = "confluent.kafka.host";

    public static final String CONFLUENT_REST_HOST_KEY = "confluent.rest.host";
    public static final String CONFLUENT_REST_PORT_KEY = "confluent.rest.port";

    public static final String CONFLUENT_KSQL_HOST_KEY = "confluent.ksql.host";
    public static final String CONFLUENT_KSQL_PORT_KEY = "confluent.ksql.port";

    public static final String CONFLUENT_SCHEMAREGISTRY_HOST_CLIENT_KEY = "confluent.schemaregistry.client.host";
    public static final String CONFLUENT_KAFKA_HOST_CLIENT_KEY = "confluent.kafka.client.host";
    public static final String CONFLUENT_KSQL_HOST_CLIENT_KEY = "confluent.ksql.client.host";
    public static final String CONFLUENT_REST_HOST_CLIENT_KEY = "confluent.rest.client.host";

    private ConfluentConfig() {}
}
