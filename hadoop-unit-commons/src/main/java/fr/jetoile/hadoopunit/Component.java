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

/**
 * List of component which can be bootstrap.
 * Warning : this list should be sorted
 */
public enum Component {
    HDFS("hdfs", "fr.jetoile.hadoopunit.component.HdfsBootstrap", "hdfs.artifact"),
    ZOOKEEPER("zookeeper", "fr.jetoile.hadoopunit.component.ZookeeperBootstrap", "zookeeper.artifact"),
    HIVEMETA("hivemeta", "fr.jetoile.hadoopunit.component.HiveMetastoreBootstrap", "hivemeta.artifact"),
    HIVESERVER2("hiveserver2", "fr.jetoile.hadoopunit.component.HiveServer2Bootstrap", "hiveserver2.artifact"),
    KAFKA("kafka", "fr.jetoile.hadoopunit.component.KafkaBootstrap", "kafka.artifact"),
    HBASE("hbase", "fr.jetoile.hadoopunit.component.HBaseBootstrap", "hbase.artifact"),
    OOZIE("oozie", "fr.jetoile.hadoopunit.component.OozieBootstrap", "oozie.artifact"),
    SOLRCLOUD("solrcloud", "fr.jetoile.hadoopunit.component.SolrCloudBootstrap", "solrcloud.artifact"),
    SOLR("solr", "fr.jetoile.hadoopunit.component.SolrBootstrap", "solr.artifact"),
    CASSANDRA("cassandra", "fr.jetoile.hadoopunit.component.CassandraBootstrap", "cassandra.artifact"),
    MONGODB("mongodb", "fr.jetoile.hadoopunit.component.MongoDbBootstrap", "mongodb.artifact"),
    ELASTICSEARCH("elasticsearch", "fr.jetoile.hadoopunit.component.ElasticSearchBootstrap", "elasticsearch.artifact"),
    NEO4J("neo4j", "fr.jetoile.hadoopunit.component.Neo4jBootstrap", "neo4j.artifact"),
    KNOX("knox", "fr.jetoile.hadoopunit.component.KnoxBootstrap", "knox.artifact");

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
}

