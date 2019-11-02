package fr.jetoile.hadoopunit.testcontainers;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;

import static org.fest.assertions.Assertions.assertThat;

public class TestContainerTest {
    private final Logger LOGGER = LoggerFactory.getLogger(TestContainerTest.class);

    @Rule
    public GenericContainer hadoopUnit = new GenericContainer<>("jetoile/hadoop-unit-standalone:3.6")
            .withExposedPorts(20112, 22010, 20102, 50070)
            .waitingFor(
                    Wait.forLogMessage(".* ______  __      _________                         _____  __      __________.*", 1)
            );


    @Test
    public void hadoopUnit_should_start() throws IOException {

        int hdfsPort = hadoopUnit.getMappedPort(20112);
        int hdfsHttpPort = hadoopUnit.getMappedPort(50070);

        // Write a file to HDFS containing the test string
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.default.name", "hdfs://localhost:" + hdfsPort);

        URI uri = URI.create("hdfs://localhost:" + hdfsPort);

        FileSystem fileSystem = FileSystem.get(uri, conf);

        FSDataOutputStream writer = fileSystem.create(new Path("/pouet.txt"));
        writer.writeUTF("TESTING");
        writer.close();

        // Read the file and compare to test string
        FSDataInputStream reader = fileSystem.open(new Path("/pouet.txt"));
        assertThat(reader.readUTF()).isEqualToIgnoringCase("TESTING");
        reader.close();
        fileSystem.close();

        URL url = new URL(String.format("http://localhost:%s/webhdfs/v1?op=GETHOMEDIRECTORY&user.name=guest", hdfsHttpPort));
        URLConnection connection = url.openConnection();
        connection.setRequestProperty("Accept-Charset", "UTF-8");
        BufferedReader response = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        String line = response.readLine();
        response.close();
        assertThat("{\"Path\":\"/user/guest\"}").isEqualTo(line);
    }
}
