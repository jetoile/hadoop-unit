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


import fr.jetoile.hadoopunit.HadoopBootstrap;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.Utils;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.fest.assertions.Assertions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class Hdfs3BootstrapTest {

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
    public void hdfsShouldStart() throws Exception {

        Assertions.assertThat(Utils.available("127.0.0.1", 20112)).isFalse();

        // Write a file to HDFS containing the test string
        FileSystem hdfsFsHandle = ((Hdfs3Bootstrap) HadoopBootstrap.INSTANCE.getService("HDFS3")).getHdfsFileSystemHandle();
        FSDataOutputStream writer = hdfsFsHandle.create(
                new Path(configuration.getString(Hdfs3Config.HDFS3_TEST_FILE_KEY)));
        writer.writeUTF(configuration.getString(Hdfs3Config.HDFS3_TEST_STRING_KEY));
        writer.close();

        // Read the file and compare to test string
        FSDataInputStream reader = hdfsFsHandle.open(
                new Path(configuration.getString(Hdfs3Config.HDFS3_TEST_FILE_KEY)));
        assertEquals(reader.readUTF(), configuration.getString(Hdfs3Config.HDFS3_TEST_STRING_KEY));
        reader.close();
        hdfsFsHandle.close();

        URL url = new URL(
                String.format("http://localhost:%s/webhdfs/v1?op=GETHOMEDIRECTORY&user.name=guest",
                        configuration.getInt(Hdfs3Config.HDFS3_NAMENODE_HTTP_PORT_KEY)));
        URLConnection connection = url.openConnection();
        connection.setRequestProperty("Accept-Charset", "UTF-8");
        BufferedReader response = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        String line = response.readLine();
        response.close();
        assertThat("{\"Path\":\"/user/guest\"}").isEqualTo(line);

    }
}
