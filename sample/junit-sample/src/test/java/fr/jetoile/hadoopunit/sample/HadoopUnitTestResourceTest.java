/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package fr.jetoile.hadoopunit.sample;

import fr.jetoile.hadoopunit.Component;
import fr.jetoile.hadoopunit.HadoopBootstrap;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.Utils;
import fr.jetoile.hadoopunit.component.HdfsBootstrap;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import fr.jetoile.hadoopunit.junit.HadoopUnitTestResourceBuilder;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.fest.assertions.Assertions;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertEquals;


//@RunWith(HadoopUnitJunitRunner.class)
//@HadoopUnitJunitComponents(
//        componentArtifact = {
//                "ZOOKEEPER,fr.jetoile.hadoop,hadoop-unit-zookeeper,2.10-SNAPSHOT"
//        }
//)
public class HadoopUnitTestResourceTest {

    static private Configuration configuration;


    @ClassRule
    public static HadoopUnitTestResourceBuilder hadoopUnitTestResourceBuilder = HadoopUnitTestResourceBuilder
            .forJunit()
            .whereMavenLocalRepo("/home/khanh/.m2/repository")
            .with("ZOOKEEPER", "fr.jetoile.hadoop", "hadoop-unit-zookeeper", "2.10-SNAPSHOT");
//            .with("HDFS", "fr.jetoile.hadoop", "hadoop-unit-hdfs", "2.10-SNAPSHOT");

//    @BeforeClass
//    public static void setup() {
//    HadoopUnitTestResourceBuilder
//            .forJunit()
//            .whereMavenLocalRepo("/home/khanh/.m2/repository")
//            .with("ZOOKEEPER", "fr.jetoile.hadoop", "hadoop-unit-zookeeper", "2.10-SNAPSHOT")
//            .with("HDFS", "fr.jetoile.hadoop", "hadoop-unit-hdfs", "2.10-SNAPSHOT")
//            .build();
//    }


    @BeforeClass
    public static void setup() throws BootstrapException {
        try {
            configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }
    }


    @Test
//    @Ignore
    public void test_zookeeperShouldStart() throws InterruptedException {
        System.out.println("==========================");
        Assertions.assertThat(Utils.available("127.0.0.1", 22010)).isFalse();
        System.out.println("==========================");
    }

    @Test
    @Ignore
    public void test_hdfsShouldStart() throws Exception {

        Assertions.assertThat(Utils.available("127.0.0.1", 20112)).isFalse();

        // Write a file to HDFS containing the test string
        FileSystem hdfsFsHandle = ((HdfsBootstrap) HadoopBootstrap.INSTANCE.getService(Component.HDFS)).getHdfsFileSystemHandle();
        FSDataOutputStream writer = hdfsFsHandle.create(
                new Path(configuration.getString(HadoopUnitConfig.HDFS_TEST_FILE_KEY)));
        writer.writeUTF(configuration.getString(HadoopUnitConfig.HDFS_TEST_STRING_KEY));
        writer.close();

        // Read the file and compare to test string
        FSDataInputStream reader = hdfsFsHandle.open(
                new Path(configuration.getString(HadoopUnitConfig.HDFS_TEST_FILE_KEY)));
        assertEquals(reader.readUTF(), configuration.getString(HadoopUnitConfig.HDFS_TEST_STRING_KEY));
        reader.close();
        hdfsFsHandle.close();

        URL url = new URL(
                String.format( "http://localhost:%s/webhdfs/v1?op=GETHOMEDIRECTORY&user.name=guest",
                        configuration.getInt( HadoopUnitConfig.HDFS_NAMENODE_HTTP_PORT_KEY ) ) );
        URLConnection connection = url.openConnection();
        connection.setRequestProperty( "Accept-Charset", "UTF-8" );
        BufferedReader response = new BufferedReader( new InputStreamReader( connection.getInputStream() ) );
        String line = response.readLine();
        response.close();
        assertThat("{\"Path\":\"/user/guest\"}").isEqualTo(line);

    }

}