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
package fr.jetoile.hadoopunit.test.alluxio;

import alluxio.PropertyKey;
import alluxio.client.file.FileSystem;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public enum AlluxioUtils {
    INSTANCE;

    private final Logger LOGGER = LoggerFactory.getLogger(AlluxioUtils.class);

    public static final String ALLUXIO_WORK_DIR = "alluxio.work.dir";
    public static final String ALLUXIO_HOSTNAME = "alluxio.hostname";
    public static final String ALLUXIO_MASTER_RPC_PORT = "alluxio.master.port";
    public static final String ALLUXIO_MASTER_WEB_PORT = "alluxio.master.web.port";
    public static final String ALLUXIO_PROXY_WEB_PORT = "alluxio.proxy.web.port";
    public static final String ALLUXIO_WORKER_RPC_PORT = "alluxio.worker.port";
    public static final String ALLUXIO_WORKER_DATA_PORT = "alluxio.worker.data.port";
    public static final String ALLUXIO_WORKER_WEB_PORT = "alluxio.worker.web.port";
    public static final String ALLUXIO_WEBAPP_DIRECTORY = "alluxio.webapp.directory";


    private FileSystem fileSystem = null;

    private Map<PropertyKey, String> configMap = new HashMap<>();
    private org.apache.commons.configuration.Configuration configuration;

    AlluxioUtils() {
        try {
            loadConfig();
        } catch (BootstrapException e) {
            e.printStackTrace();
        }

        this.fileSystem = FileSystem.Factory.get();
    }

    private void loadConfig() throws BootstrapException {
        try {
            configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }

        String workDirectory = configuration.getString(ALLUXIO_WORK_DIR);
        String hostname = configuration.getString(ALLUXIO_HOSTNAME);
        int masterRpcPort = configuration.getInt(ALLUXIO_MASTER_RPC_PORT);
        int masterWebPort = configuration.getInt(ALLUXIO_MASTER_WEB_PORT);
        int proxyWebPort = configuration.getInt(ALLUXIO_PROXY_WEB_PORT);
        int workerRpcPort = configuration.getInt(ALLUXIO_WORKER_RPC_PORT);
        int workerDataPort = configuration.getInt(ALLUXIO_WORKER_DATA_PORT);
        int workerWebPort = configuration.getInt(ALLUXIO_WORKER_WEB_PORT);

//        configMap.put(PropertyKey.WORK_DIR, workDirectory);
        configMap.put(PropertyKey.MASTER_HOSTNAME, hostname);
//        configMap.put(PropertyKey.MASTER_BIND_HOST, hostname);
//        configMap.put(PropertyKey.MASTER_WEB_BIND_HOST, hostname);
//        configMap.put(PropertyKey.WORKER_BIND_HOST, hostname);
//        configMap.put(PropertyKey.WORKER_DATA_BIND_HOST, hostname);
//        configMap.put(PropertyKey.WORKER_WEB_BIND_HOST, hostname);

        configMap.put(PropertyKey.MASTER_RPC_PORT, String.valueOf(masterRpcPort));
        configMap.put(PropertyKey.USER_FILE_WRITE_LOCATION_POLICY, "alluxio.client.file.policy.MostAvailableFirstPolicy");
//        configMap.put(PropertyKey.MASTER_WEB_PORT, String.valueOf(masterWebPort));
//        configMap.put(PropertyKey.PROXY_WEB_PORT, String.valueOf(proxyWebPort));
//        configMap.put(PropertyKey.WORKER_RPC_PORT, String.valueOf(workerRpcPort));
//        configMap.put(PropertyKey.WORKER_DATA_PORT, String.valueOf(workerDataPort));
//        configMap.put(PropertyKey.WORKER_WEB_PORT, String.valueOf(workerWebPort));

        for (Map.Entry<PropertyKey, String> entry : configMap.entrySet()) {
            alluxio.Configuration.set(entry.getKey(), entry.getValue());
        }
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }
}
