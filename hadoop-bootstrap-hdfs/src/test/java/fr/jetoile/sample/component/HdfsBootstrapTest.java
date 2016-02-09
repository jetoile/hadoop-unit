package fr.jetoile.sample.component;


import fr.jetoile.sample.Component;
import fr.jetoile.sample.Config;
import fr.jetoile.sample.HadoopBootstrap;
import fr.jetoile.sample.exception.BootstrapException;
import fr.jetoile.sample.Utils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.fest.assertions.Assertions;
import org.junit.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class HdfsBootstrapTest {

    static private Configuration configuration;


    @BeforeClass
    public static void setup() throws BootstrapException {
        HadoopBootstrap.INSTANCE.startAll();

        try {
            configuration = new PropertiesConfiguration("default.properties");
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
        FileSystem hdfsFsHandle = ((HdfsBootstrap)HadoopBootstrap.INSTANCE.getService(Component.HDFS)).getHdfsFileSystemHandle();
        FSDataOutputStream writer = hdfsFsHandle.create(
                new Path(configuration.getString(Config.HDFS_TEST_FILE_KEY)));
        writer.writeUTF(configuration.getString(Config.HDFS_TEST_STRING_KEY));
        writer.close();

        // Read the file and compare to test string
        FSDataInputStream reader = hdfsFsHandle.open(
                new Path(configuration.getString(Config.HDFS_TEST_FILE_KEY)));
        assertEquals(reader.readUTF(), configuration.getString(Config.HDFS_TEST_STRING_KEY));
        reader.close();
        hdfsFsHandle.close();

        URL url = new URL(
                String.format( "http://localhost:%s/webhdfs/v1?op=GETHOMEDIRECTORY&user.name=guest",
                        configuration.getInt( Config.HDFS_NAMENODE_HTTP_PORT_KEY ) ) );
        URLConnection connection = url.openConnection();
        connection.setRequestProperty( "Accept-Charset", "UTF-8" );
        BufferedReader response = new BufferedReader( new InputStreamReader( connection.getInputStream() ) );
        String line = response.readLine();
        response.close();
        assertThat("{\"Path\":\"/user/guest\"}").isEqualTo(line);

    }
}
