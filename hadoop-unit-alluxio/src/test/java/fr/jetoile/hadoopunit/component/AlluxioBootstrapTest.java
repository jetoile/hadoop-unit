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

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.AlluxioException;
import fr.jetoile.hadoopunit.HadoopBootstrap;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import fr.jetoile.hadoopunit.exception.NotFoundServiceException;
import fr.jetoile.hadoopunit.test.alluxio.AlluxioUtils;
import fr.jetoile.hadoopunit.test.hdfs.HdfsUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.Assert.assertTrue;

public class AlluxioBootstrapTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(AlluxioBootstrapTest.class);
    public static final int NB_FILE = 3;
    public static final String PATH = "/fooDirectory";

    static private Configuration configuration;


    @BeforeClass
    public static void setup() throws BootstrapException {
        HadoopBootstrap.INSTANCE.startAll();

        try {
            configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }
    }


    @AfterClass
    public static void tearDown() throws BootstrapException {
        HadoopBootstrap.INSTANCE.stopAll();
    }

    @Test
    public void alluxioShouldStart() throws NotFoundServiceException, IOException, AlluxioException {
        FileSystemMasterClient fsMasterClient = new FileSystemMasterClient(new InetSocketAddress(configuration.getString(HadoopUnitConfig.ALLUXIO_HOSTNAME), configuration.getInt(HadoopUnitConfig.ALLUXIO_MASTER_RPC_PORT)));

        AlluxioURI file = new AlluxioURI("/file");
        Assert.assertFalse(fsMasterClient.isConnected());
        fsMasterClient.connect();
        assertTrue(fsMasterClient.isConnected());
        fsMasterClient.createFile(file, CreateFileOptions.defaults());
        Assert.assertNotNull(fsMasterClient.getStatus(file));
        fsMasterClient.disconnect();
        Assert.assertFalse(fsMasterClient.isConnected());
        fsMasterClient.connect();
        assertTrue(fsMasterClient.isConnected());
        Assert.assertNotNull(fsMasterClient.getStatus(file));
        fsMasterClient.close();
    }

    @Test(timeout = 3000, expected = AlluxioException.class)
    public void getFileInfoReturnsOnError() throws IOException, AlluxioException {
        FileSystemMasterClient fsMasterClient = new FileSystemMasterClient(new InetSocketAddress(configuration.getString(HadoopUnitConfig.ALLUXIO_HOSTNAME), configuration.getInt(HadoopUnitConfig.ALLUXIO_MASTER_RPC_PORT)));

        fsMasterClient.getStatus(new AlluxioURI("/doesNotExist"));
        fsMasterClient.close();
    }

    @Test
    public void write_and_read_on_hdfs_should_success() throws IOException, AlluxioException {
        FileSystem fs = AlluxioUtils.INSTANCE.getFileSystem();
        writeFile(fs);

        assertTrue(readFile(fs));
        assertTrue(HdfsUtils.INSTANCE.getFileSystem().exists(new Path("/alluxio/underFSStorage" + PATH + "/part-1")));
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