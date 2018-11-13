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

public class ZookeeperConfig {

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

    private ZookeeperConfig() {}
}
