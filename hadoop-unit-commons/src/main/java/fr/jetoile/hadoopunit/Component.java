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
package fr.jetoile.hadoopunit;

import java.util.Arrays;

/**
 * List of component which can be bootstrap.
 * Warning : this list should be sorted
 */
public enum Component {
    HDFS("hdfs", "fr.jetoile.hadoopunit.component.HdfsBootstrap", "hdfs.artifact"),
    ZOOKEEPER("zookeeper", "fr.jetoile.hadoopunit.component.ZookeeperBootstrap", "zookeeper.artifact"),
    YARN("yarn", "fr.jetoile.hadoopunit.component.YarnBootstrap", "yarn.artifact"),
    ALLUXIO("alluxio", "fr.jetoile.hadoopunit.component.AlluxioBootstrap", "alluxio.artifact"),
    HIVEMETA("hivemeta", "fr.jetoile.hadoopunit.component.HiveMetastoreBootstrap", "hivemeta.artifact"),
    HIVESERVER2("hiveserver2", "fr.jetoile.hadoopunit.component.HiveServer2Bootstrap", "hiveserver2.artifact"),
    HIVEMETA_2("hivemeta_2", "fr.jetoile.hadoopunit.component.HiveMetastore2Bootstrap", "hivemeta_2.artifact"),
    HIVESERVER2_2("hiveserver2_2", "fr.jetoile.hadoopunit.component.HiveServer22Bootstrap", "hiveserver2_2.artifact"),
    HIVELLAP_2("hivellap_2", "fr.jetoile.hadoopunit.component.HiveLlap2Bootstrap", "hivellap_2.artifact"),
    HIVEMETA_3("hivemeta_3", "fr.jetoile.hadoopunit.component.HiveMetastore3Bootstrap", "hivemeta_3.artifact"),
    HIVESERVER2_3("hiveserver2_3", "fr.jetoile.hadoopunit.component.HiveServer23Bootstrap", "hiveserver2_3.artifact"),
    KAFKA("kafka", "fr.jetoile.hadoopunit.component.KafkaBootstrap", "kafka.artifact"),
    HBASE("hbase", "fr.jetoile.hadoopunit.component.HBaseBootstrap", "hbase.artifact"),
    OOZIE("oozie", "fr.jetoile.hadoopunit.component.OozieBootstrap", "oozie.artifact"),
    SOLRCLOUD("solrcloud", "fr.jetoile.hadoopunit.component.SolrCloudBootstrap", "solrcloud.artifact"),
    SOLR("solr", "fr.jetoile.hadoopunit.component.SolrBootstrap", "solr.artifact"),
    CASSANDRA("cassandra", "fr.jetoile.hadoopunit.component.CassandraBootstrap", "cassandra.artifact"),
    MONGODB("mongodb", "fr.jetoile.hadoopunit.component.MongoDbBootstrap", "mongodb.artifact"),
    ELASTICSEARCH("elasticsearch", "fr.jetoile.hadoopunit.component.ElasticSearchBootstrap", "elasticsearch.artifact"),
    NEO4J("neo4j", "fr.jetoile.hadoopunit.component.Neo4jBootstrap", "neo4j.artifact"),
    KNOX("knox", "fr.jetoile.hadoopunit.component.KnoxBootstrap", "knox.artifact"),
    REDIS("redis", "fr.jetoile.hadoopunit.component.RedisBootstrap", "redis.artifact"),
    CONFLUENT_KAFKA("confluent_kafka", "fr.jetoile.hadoopunit.component.ConfluentKafkaBootstrap", "confluent.kafka.artifact"),
    CONFLUENT_SCHEMAREGISTRY("confluent_schemaregistry", "fr.jetoile.hadoopunit.component.ConfluentSchemaRegistryBootstrap", "confluent.schemaregistry.artifact"),
    CONFLUENT_KAFKA_REST("confluent_kafka_rest", "fr.jetoile.hadoopunit.component.ConfluentKafkaRestBootstrap", "confluent.kafka_rest.artifact"),
    CONFLUENT_KSQL_REST("confluent_ksql_rest", "fr.jetoile.hadoopunit.component.ConfluentKsqlRestBootstrap", "confluent.ksql_rest.artifact");

    private String key;
    private String mainClass;
    private String artifactKey;

    Component(String key, String mainClass, String artifactKey) {
        this.key = key;
        this.mainClass = mainClass;
        this.artifactKey = artifactKey;
    }

    public String getKey() {
        return key;
    }

    public String getMainClass() {
        return mainClass;
    }

    public String getArtifactKey() {
        return artifactKey;
    }

    public static boolean isComponent(String key) {
        return Arrays.stream(values())
                .anyMatch(enumValue -> enumValue.key.equalsIgnoreCase(key));
    }

    public static int getOrdinal(String key) {
        return valueOf(key).ordinal();
    }
}

