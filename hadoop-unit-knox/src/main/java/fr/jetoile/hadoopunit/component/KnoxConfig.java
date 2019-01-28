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

public class KnoxConfig {

    //Knox
    public static final String KNOX_HOST_KEY = "knox.host";
    public static final String KNOX_PORT_KEY = "knox.port";
    public static final String KNOX_PATH_KEY = "knox.path";
    public static final String KNOX_CLUSTER_KEY = "knox.cluster";
    public static final String KNOX_HOME_DIR_KEY = "knox.home.dir";
    public static final String KNOX_SERVICE_KEY = "knox.service";

    //HDFS
    public static final String HDFS_NAMENODE_HOST_KEY = "hdfs.namenode.host";
    public static final String HDFS_NAMENODE_PORT_KEY = "hdfs.namenode.port";
    public static final String HDFS_NAMENODE_HTTP_PORT_KEY = "hdfs.namenode.http.port";

    // HDFS Test
    public static final String HDFS_TEST_FILE_KEY = "hdfs.test.file";
    public static final String HDFS_TEST_STRING_KEY = "hdfs.test.string";

    // HBase Rest
    public static final String HBASE_REST_PORT_KEY = "hbase.rest.port";
    public static final String HBASE_REST_HOST_KEY="hbase.rest.host";

    // HBase Test
    public static final String HBASE_TEST_TABLE_NAME_KEY = "hbase.test.table.name";
    public static final String HBASE_TEST_COL_FAMILY_NAME_KEY = "hbase.test.col.family.name";
    public static final String HBASE_TEST_COL_QUALIFIER_NAME_KEY = "hbase.test.col.qualifier.name";
    public static final String HBASE_TEST_NUM_ROWS_TO_PUT_KEY = "hbase.test.num.rows.to.put";

    //Oozie
    public static final String OOZIE_PORT = "oozie.port";
    public static final String OOZIE_HOST = "oozie.host";

    private KnoxConfig() {}
}
