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

public class CassandraConfig {

    // Cassandra
    public static final String CASSANDRA_LISTEN_ADDRESS_IP_KEY = "cassandra.listen.address.ip";
    public static final String CASSANDRA_RPC_ADDRESS_IP_KEY = "cassandra.rpc.address.ip";
    public static final String CASSANDRA_BROADCAST_ADDRESS_IP_KEY = "cassandra.broadcast.address.ip";
    public static final String CASSANDRA_BROADCAST_RPC_ADDRESS_IP_KEY = "cassandra.broadcast.rpc.address.ip";
    public static final String CASSANDRA_PORT_KEY = "cassandra.port";
    public static final String CASSANDRA_TEMP_DIR_KEY = "cassandra.temp.dir";

    public static final String CASSANDRA_LISTEN_ADDRESS_IP_CLIENT_KEY = "cassandra.listen.address.client.ip";

    private CassandraConfig() {}
}
