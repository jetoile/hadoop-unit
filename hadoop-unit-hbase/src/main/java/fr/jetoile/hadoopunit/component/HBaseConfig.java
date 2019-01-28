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

public class HBaseConfig {

    // HBase
    public static final String HBASE_MASTER_PORT_KEY = "hbase.master.port";
    public static final String HBASE_MASTER_INFO_PORT_KEY = "hbase.master.info.port";
    public static final String HBASE_NUM_REGION_SERVERS_KEY = "hbase.num.region.servers";
    public static final String HBASE_ROOT_DIR_KEY = "hbase.root.dir";
    public static final String HBASE_ZNODE_PARENT_KEY = "hbase.znode.parent";
    public static final String HBASE_WAL_REPLICATION_ENABLED_KEY = "hbase.wal.replication.enabled";

    // HBase Rest
    public static final String HBASE_REST_PORT_KEY = "hbase.rest.port";
    public static final String HBASE_REST_READONLY_KEY="hbase.rest.readonly";
    public static final String HBASE_REST_INFO_PORT_KEY="hbase.rest.info.port";
    public static final String HBASE_REST_HOST_KEY="hbase.rest.host";
    public static final String HBASE_REST_THREADMAX_KEY="hbase.rest.threads.max";
    public static final String HBASE_REST_THREADMIN_KEY="hbase.rest.threads.min";

    // HBase Test
    public static final String HBASE_TEST_TABLE_NAME_KEY = "hbase.test.table.name";
    public static final String HBASE_TEST_COL_FAMILY_NAME_KEY = "hbase.test.col.family.name";
    public static final String HBASE_TEST_COL_QUALIFIER_NAME_KEY = "hbase.test.col.qualifier.name";
    public static final String HBASE_TEST_NUM_ROWS_TO_PUT_KEY = "hbase.test.num.rows.to.put";

    private HBaseConfig() {}
}
