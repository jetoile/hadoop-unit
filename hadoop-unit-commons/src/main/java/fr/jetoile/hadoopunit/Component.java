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
    ZOOKEEPER("zookeeper", "fr.jetoile.hadoopunit.component.ZookeeperBootstrap"),
    HDFS("hdfs", "fr.jetoile.hadoopunit.component.HdfsBootstrap"),
    HIVEMETA("hivemeta", "fr.jetoile.hadoopunit.component.HiveMetastoreBootstrap"),
    HIVESERVER2("hiveserver2", "fr.jetoile.hadoopunit.component.HiveServer2Bootstrap"),
    KAFKA("kafka", "fr.jetoile.hadoopunit.component.KafkaBootstrap"),
    HBASE("hbase", "fr.jetoile.hadoopunit.component.HBaseBootstrap"),
    OOZIE("oozie", "fr.jetoile.hadoopunit.component.OozieBootstrap"),
    SOLRCLOUD("solrcloud", "fr.jetoile.hadoopunit.component.SolrCloudBootstrap"),
    SOLR("solr", "fr.jetoile.hadoopunit.component.SolrBootstrap"),
    CASSANDRA("cassandra", "fr.jetoile.hadoopunit.component.CassandraBootstrap"),
    MONGODB("mongodb", "fr.jetoile.hadoopunit.component.MongoDbBootstrap"),
    ELASTICSEARCH("elasticsearch", "fr.jetoile.hadoopunit.component.ElasticSearchBootstrap"),
    NEO4J("neo4j", "fr.jetoile.hadoopunit.component.Neo4jBootstrap");

    private String key;
    private String mainClass;

    Component(String key, String mainClass) {
        this.key = key;
        this.mainClass = mainClass;
    }

    public String getKey() {
        return key;
    }

    public String getMainClass() {
        return mainClass;
    }
}

