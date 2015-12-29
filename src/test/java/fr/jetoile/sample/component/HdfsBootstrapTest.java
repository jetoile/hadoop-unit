package fr.jetoile.sample.component;


import com.github.sakserv.minicluster.config.ConfigVars;
import fr.jetoile.sample.exception.BootstrapException;
import fr.jetoile.sample.Utils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class HdfsBootstrapTest {

    private static Bootstrap hdfs;
    static private Configuration configuration;


    @BeforeClass
    public static void setup() throws BootstrapException {
        hdfs = HdfsBootstrap.INSTANCE.start();

        try {
            configuration = new PropertiesConfiguration("default.properties");
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }
    }


    @AfterClass
    public static void tearDown() throws BootstrapException {
        hdfs.stop();
    }

    @Test
    public void hdfsShouldStart() throws Exception {

        assertThat(Utils.available("127.0.0.1", 20112)).isFalse();

        // Write a file to HDFS containing the test string
        FileSystem hdfsFsHandle = ((HdfsBootstrap)hdfs).getHdfsFileSystemHandle();
        FSDataOutputStream writer = hdfsFsHandle.create(
                new Path(configuration.getString(ConfigVars.HDFS_TEST_FILE_KEY)));
        writer.writeUTF(configuration.getString(ConfigVars.HDFS_TEST_STRING_KEY));
        writer.close();

        // Read the file and compare to test string
        FSDataInputStream reader = hdfsFsHandle.open(
                new Path(configuration.getString(ConfigVars.HDFS_TEST_FILE_KEY)));
        assertEquals(reader.readUTF(), configuration.getString(ConfigVars.HDFS_TEST_STRING_KEY));
        reader.close();
        hdfsFsHandle.close();

        URL url = new URL(
                String.format( "http://localhost:%s/webhdfs/v1?op=GETHOMEDIRECTORY&user.name=guest",
                        configuration.getInt( ConfigVars.HDFS_NAMENODE_HTTP_PORT_KEY ) ) );
        URLConnection connection = url.openConnection();
        connection.setRequestProperty( "Accept-Charset", "UTF-8" );
        BufferedReader response = new BufferedReader( new InputStreamReader( connection.getInputStream() ) );
        String line = response.readLine();
        response.close();
        assertThat("{\"Path\":\"/user/guest\"}").isEqualTo(line);

    }
}
