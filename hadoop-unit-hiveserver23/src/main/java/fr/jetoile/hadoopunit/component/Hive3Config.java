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

public class Hive3Config {

    // Hive
    public static final String HIVE3_SCRATCH_DIR_KEY = "hive3.scratch.dir";
    public static final String HIVE3_WAREHOUSE_DIR_KEY = "hive3.warehouse.dir";

    // Hive Metastore
    public static final String HIVE3_METASTORE_HOSTNAME_KEY = "hive3.metastore.hostname";
    public static final String HIVE3_METASTORE_PORT_KEY = "hive3.metastore.port";
    public static final String HIVE3_METASTORE_DERBY_DB_DIR_KEY = "hive3.metastore.derby.db.dir";

    // Hive Server2
    public static final String HIVE3_SERVER2_HOSTNAME_KEY = "hive3.server2.hostname";
    public static final String HIVE3_SERVER2_PORT_KEY = "hive3.server2.port";

    // Hive Test
    public static final String HIVE3_TEST_DATABASE_NAME_KEY = "hive3.test.database.name";
    public static final String HIVE3_TEST_TABLE_NAME_KEY = "hive3.test.table.name";

    public static final String HIVE3_METASTORE_HOSTNAME_CLIENT_KEY = "hive3.metastore.client.hostname";
    public static final String HIVE3_SERVER2_HOSTNAME_CLIENT_KEY = "hive3.server2.client.hostname";

    private Hive3Config() {}
}
