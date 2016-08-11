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

public class HadoopUnitConfig {
    // Props file
    public static final String DEFAULT_PROPS_FILE = "hadoop-unit-default.properties";

    // Zookeeper
    public static final String ZOOKEEPER_PORT_KEY = "zookeeper.port";
    public static final String ZOOKEEPER_HOST_KEY = "zookeeper.host";
    public static final String ZOOKEEPER_TEMP_DIR_KEY = "zookeeper.temp.dir";
    public static final String ZOOKEEPER_ELECTION_PORT_KEY = "zookeeper.election.port";
    public static final String ZOOKEEPER_QUORUM_PORT_KEY = "zookeeper.quorum.port";
    public static final String ZOOKEEPER_DELETE_DATA_DIRECTORY_ON_CLOSE_KEY = "zookeeper.delete.data.directory.on.close";
    public static final String ZOOKEEPER_SERVER_ID_KEY = "zookeeper.server.id";
    public static final String ZOOKEEPER_TICKTIME_KEY = "zookeeper.ticktime";
    public static final String ZOOKEEPER_MAX_CLIENT_CNXNS_KEY = "zookeeper.max.client.cnxns";
    public static final String ZOOKEEPER_CONNECTION_STRING_KEY = "zookeeper.connection.string";

    // Hive
    public static final String HIVE_SCRATCH_DIR_KEY = "hive.scratch.dir";
    public static final String HIVE_WAREHOUSE_DIR_KEY = "hive.warehouse.dir";

    // Hive Metastore
    public static final String HIVE_METASTORE_HOSTNAME_KEY = "hive.metastore.hostname";
    public static final String HIVE_METASTORE_PORT_KEY = "hive.metastore.port";
    public static final String HIVE_METASTORE_DERBY_DB_DIR_KEY = "hive.metastore.derby.db.dir";

    // Hive Server2
    public static final String HIVE_SERVER2_HOSTNAME_KEY = "hive.server2.hostname";
    public static final String HIVE_SERVER2_PORT_KEY = "hive.server2.port";

    // Hive Test
    public static final String HIVE_TEST_DATABASE_NAME_KEY = "hive.test.database.name";
    public static final String HIVE_TEST_TABLE_NAME_KEY = "hive.test.table.name";

    // MongoDB
    public static final String MONGO_IP_KEY = "mongo.ip";
    public static final String MONGO_PORT_KEY = "mongo.port";
    public static final String MONGO_DATABASE_NAME_KEY = "mongo.database.name";
    public static final String MONGO_COLLECTION_NAME_KEY = "mongo.collection.name";

    // Cassandra
    public static final String CASSANDRA_IP_KEY = "cassandra.ip";
    public static final String CASSANDRA_PORT_KEY = "cassandra.port";
    public static final String CASSANDRA_TEMP_DIR_KEY = "cassandra.temp.dir";

    // Neo4j
    public static final String NEO4J_IP_KEY = "neo4j.ip";
    public static final String NEO4J_PORT_KEY = "neo4j.port";
    public static final String NEO4J_TEMP_DIR_KEY = "neo4j.temp.dir";

    // ElasticSearch
    public static final String ELASTICSEARCH_IP_KEY = "elasticsearch.ip";
    public static final String ELASTICSEARCH_HTTP_PORT_KEY = "elasticsearch.http.port";
    public static final String ELASTICSEARCH_TCP_PORT_KEY = "elasticsearch.tcp.port";
    public static final String ELASTICSEARCH_TEMP_DIR_KEY = "elasticsearch.temp.dir";
    public static final String ELASTICSEARCH_INDEX_NAME = "elasticsearch.index.name";
    public static final String ELASTICSEARCH_CLUSTER_NAME = "elasticsearch.cluster.name";

    // Kafka
    public static final String KAFKA_HOSTNAME_KEY = "kafka.hostname";
    public static final String KAFKA_PORT_KEY = "kafka.port";

    // Kafka Test
    public static final String KAFKA_TEST_TOPIC_KEY = "kafka.test.topic";
    public static final String KAFKA_TEST_MESSAGE_COUNT_KEY = "kafka.test.message.count";
    public static final String KAFKA_TEST_BROKER_ID_KEY = "kafka.test.broker.id";
    public static final String KAFKA_TEST_TEMP_DIR_KEY = "kafka.test.temp.dir";

    //HDFS
    public static final String HDFS_NAMENODE_PORT_KEY = "hdfs.namenode.port";
    public static final String HDFS_NAMENODE_HTTP_PORT_KEY = "hdfs.namenode.http.port";
    public static final String HDFS_TEMP_DIR_KEY = "hdfs.temp.dir";
    public static final String HDFS_NUM_DATANODES_KEY = "hdfs.num.datanodes";
    public static final String HDFS_ENABLE_PERMISSIONS_KEY = "hdfs.enable.permissions";
    public static final String HDFS_FORMAT_KEY = "hdfs.format";
    public static final String HDFS_ENABLE_RUNNING_USER_AS_PROXY_USER = "hdfs.enable.running.user.as.proxy.user";

    // HDFS Test
    public static final String HDFS_TEST_FILE_KEY = "hdfs.test.file";
    public static final String HDFS_TEST_STRING_KEY = "hdfs.test.string";

    // YARN
    public static final String YARN_NUM_NODE_MANAGERS_KEY = "yarn.num.node.managers";
    public static final String YARN_NUM_LOCAL_DIRS_KEY = "yarn.num.local.dirs";
    public static final String YARN_NUM_LOG_DIRS_KEY = "yarn.num.log.dirs";
    public static final String YARN_RESOURCE_MANAGER_ADDRESS_KEY = "yarn.resource.manager.address";
    public static final String YARN_RESOURCE_MANAGER_HOSTNAME_KEY = "yarn.resource.manager.hostname";
    public static final String YARN_RESOURCE_MANAGER_SCHEDULER_ADDRESS_KEY = "yarn.resource.manager.scheduler.address";
    public static final String YARN_RESOURCE_MANAGER_WEBAPP_ADDRESS_KEY = "yarn.resource.manager.webapp.address";
    public static final String YARN_RESOURCE_MANAGER_RESOURCE_TRACKER_ADDRESS_KEY =
            "yarn.resource.manager.resource.tracker.address";
    public static final String YARN_USE_IN_JVM_CONTAINER_EXECUTOR_KEY = "yarn.use.in.jvm.container.executor";

    // MR
    public static final String MR_JOB_HISTORY_ADDRESS_KEY = "mr.job.history.address";

    // HBase
    public static final String HBASE_MASTER_PORT_KEY = "hbase.master.port";
    public static final String HBASE_MASTER_INFO_PORT_KEY = "hbase.master.info.port";
    public static final String HBASE_NUM_REGION_SERVERS_KEY = "hbase.num.region.servers";
    public static final String HBASE_ROOT_DIR_KEY = "hbase.root.dir";
    public static final String HBASE_ZNODE_PARENT_KEY = "hbase.znode.parent";
    public static final String HBASE_WAL_REPLICATION_ENABLED_KEY = "hbase.wal.replication.enabled";

    // HBase Test
    public static final String HBASE_TEST_TABLE_NAME_KEY = "hbase.test.table.name";
    public static final String HBASE_TEST_COL_FAMILY_NAME_KEY = "hbase.test.col.family.name";
    public static final String HBASE_TEST_COL_QUALIFIER_NAME_KEY = "hbase.test.col.qualifier.name";
    public static final String HBASE_TEST_NUM_ROWS_TO_PUT_KEY = "hbase.test.num.rows.to.put";

    //Oozie
    public static final String OOZIE_TMP_DIR_KEY = "oozie.tmp.dir";
    public static final String OOZIE_TEST_DIR_KEY = "oozie.test.dir";
    public static final String OOZIE_HOME_DIR_KEY = "oozie.home.dir";
    public static final String OOZIE_USERNAME_KEY = "oozie.username";
    public static final String OOZIE_GROUPNAME_KEY = "oozie.groupname";
    public static final String OOZIE_HDFS_SHARE_LIB_DIR_KEY = "oozie.hdfs.share.lib.dir";
    public static final String OOZIE_SHARE_LIB_CREATE_KEY = "oozie.share.lib.create";
    public static final String OOZIE_LOCAL_SHARE_LIB_CACHE_DIR_KEY = "oozie.local.share.lib.cache.dir";
    public static final String OOZIE_PURGE_LOCAL_SHARE_LIB_CACHE_KEY = "oozie.purge.local.share.lib.cache";
}
