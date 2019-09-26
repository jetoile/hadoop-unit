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

public class Hdfs3Config {

    //HDFS
    public static final String HDFS3_NAMENODE_HOST_KEY = "hdfs3.namenode.host";
    public static final String HDFS3_NAMENODE_PORT_KEY = "hdfs3.namenode.port";
    public static final String HDFS3_NAMENODE_HTTP_PORT_KEY = "hdfs3.namenode.http.port";
    public static final String HDFS3_TEMP_DIR_KEY = "hdfs3.temp.dir";
    public static final String HDFS3_NUM_DATANODES_KEY = "hdfs3.num.datanodes";
    public static final String HDFS3_ENABLE_PERMISSIONS_KEY = "hdfs3.enable.permissions";
    public static final String HDFS3_FORMAT_KEY = "hdfs3.format";
    public static final String HDFS3_ENABLE_RUNNING_USER_AS_PROXY_USER = "hdfs3.enable.running.user.as.proxy.user";
    public static final String HDFS3_REPLICATION_KEY = "hdfs3.replication";

    public static final String HDFS3_DATANODE_ADDRESS_KEY = "hdfs3.datanode.address";
    public static final String HDFS3_DATANODE_HTTP_ADDRESS_KEY = "hdfs3.datanode.http.address";
    public static final String HDFS3_DATANODE_IPC_ADDRESS_KEY = "hdfs3.datanode.ipc.address";

    // HDFS Test
    public static final String HDFS3_TEST_FILE_KEY = "hdfs3.test.file";
    public static final String HDFS3_TEST_STRING_KEY = "hdfs3.test.string";

    private Hdfs3Config() {}
}
