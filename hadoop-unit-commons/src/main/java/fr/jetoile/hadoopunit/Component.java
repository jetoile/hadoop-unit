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
    ZOOKEEPER("zookeeper"),
    HDFS("hdfs"),
    HIVEMETA("hivemeta"),
    HIVESERVER2("hiveserver2"),
    KAFKA("kafka"),
    HBASE("hbase"),
    OOZIE("oozie"),
    SOLRCLOUD("solrcloud"),
    SOLR("solr"),
    CASSANDRA("cassandra"),
    MONGODB("mongodb"),
    ELASTICSEARCH("elastic");

    private String key;

    Component(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

}

