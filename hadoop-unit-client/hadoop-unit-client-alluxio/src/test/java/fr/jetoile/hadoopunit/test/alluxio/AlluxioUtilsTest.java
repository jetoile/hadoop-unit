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

import alluxio.AlluxioURI;
import alluxio.PropertyKey;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.master.LocalAlluxioCluster;
import alluxio.security.LoginUserTestUtils;
import alluxio.security.authentication.AuthenticatedClientUser;
import fr.jetoile.hadoopunit.HadoopBootstrap;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

@Ignore
public class AlluxioUtilsTest {

    static private final Logger LOGGER = LoggerFactory.getLogger(AlluxioUtilsTest.class);

    public static final int NB_FILE = 3;
    public static final String PATH = "/fooDirectory";

    static private LocalAlluxioCluster alluxioLocalCluster;
    static private Map<PropertyKey, String> configMap = new HashMap<>();

    static private Configuration configuration;


    @BeforeClass
    public static void setup() throws Exception {

        try {
            configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }

        String workDirectory = configuration.getString(HadoopUnitConfig.ALLUXIO_WORK_DIR);
        String hostname = configuration.getString(HadoopUnitConfig.ALLUXIO_HOSTNAME);
        int masterRpcPort = configuration.getInt(HadoopUnitConfig.ALLUXIO_MASTER_RPC_PORT);
        int masterWebPort = configuration.getInt(HadoopUnitConfig.ALLUXIO_MASTER_WEB_PORT);
        int proxyWebPort = configuration.getInt(HadoopUnitConfig.ALLUXIO_PROXY_WEB_PORT);
        int workerRpcPort = configuration.getInt(HadoopUnitConfig.ALLUXIO_WORKER_RPC_PORT);
        int workerDataPort = configuration.getInt(HadoopUnitConfig.ALLUXIO_WORKER_DATA_PORT);
        int workerWebPort = configuration.getInt(HadoopUnitConfig.ALLUXIO_WORKER_WEB_PORT);
        String webappDirectory = configuration.getString(HadoopUnitConfig.ALLUXIO_WEBAPP_DIRECTORY);

        configMap.put(PropertyKey.WORK_DIR, workDirectory);
        configMap.put(PropertyKey.MASTER_HOSTNAME, hostname);
        configMap.put(PropertyKey.MASTER_BIND_HOST, hostname);
        configMap.put(PropertyKey.MASTER_WEB_BIND_HOST, hostname);
        configMap.put(PropertyKey.WORKER_BIND_HOST, hostname);
        configMap.put(PropertyKey.WORKER_DATA_BIND_HOST, hostname);
        configMap.put(PropertyKey.WORKER_WEB_BIND_HOST, hostname);

        configMap.put(PropertyKey.MASTER_RPC_PORT, String.valueOf(masterRpcPort));
        configMap.put(PropertyKey.MASTER_WEB_PORT, String.valueOf(masterWebPort));
        configMap.put(PropertyKey.PROXY_WEB_PORT, String.valueOf(proxyWebPort));
        configMap.put(PropertyKey.WORKER_RPC_PORT, String.valueOf(workerRpcPort));
        configMap.put(PropertyKey.WORKER_DATA_PORT, String.valueOf(workerDataPort));
        configMap.put(PropertyKey.WORKER_WEB_PORT, String.valueOf(workerWebPort));

        configMap.put(PropertyKey.WEB_RESOURCES, webappDirectory);


        AuthenticatedClientUser.remove();
        LoginUserTestUtils.resetLoginUser();

        alluxioLocalCluster = new LocalAlluxioCluster(1);
        try {
            alluxioLocalCluster.initConfiguration();

            for (Map.Entry<PropertyKey, String> entry : configMap.entrySet()) {
                alluxio.Configuration.set(entry.getKey(), entry.getValue());
            }
        } catch (IOException e) {
            LOGGER.error("unable to init configuration for alluxio", e);
        }

        alluxioLocalCluster.start();
    }


    @AfterClass
    public static void tearDown() throws Exception {
        alluxioLocalCluster.stop();
    }

    @Test
    public void write_and_read_on_hdfs_should_success() throws IOException, AlluxioException {
        FileSystem fs = AlluxioUtils.INSTANCE.getFileSystem();
        writeFile(fs);

        assertTrue(readFile(fs));
    }

    private boolean readFile(FileSystem fs) throws IOException, AlluxioException {
        boolean pass = true;
        for (int i = 0; i < NB_FILE; i++) {
            AlluxioURI filePath = new AlluxioURI(PATH + "/part-" + i);
            LOGGER.debug("Reading data from {}", filePath);
            FileInStream is = fs.openFile(filePath);
            URIStatus status = fs.getStatus(filePath);
            ByteBuffer buf = ByteBuffer.allocate((int) status.getBlockSizeBytes());
            is.read(buf.array());
            buf.order(ByteOrder.nativeOrder());
            for (int k = 0; k < NB_FILE; k++) {
                pass = pass && (buf.getInt() == k);
            }
            is.close();
        }
        return pass;
    }

    private void writeFile(FileSystem fs) throws IOException, AlluxioException {
        for (int i = 0; i < NB_FILE; i++) {
            ByteBuffer buf = ByteBuffer.allocate(80);
            buf.order(ByteOrder.nativeOrder());
            for (int k = 0; k < NB_FILE; k++) {
                buf.putInt(k);
            }
            buf.flip();
            AlluxioURI filePath = new AlluxioURI(PATH + "/part-" + i);
            LOGGER.debug("Writing data to {}", filePath);
            OutputStream os = fs.createFile(filePath);
            os.write(buf.array());
            os.close();
        }
    }

}