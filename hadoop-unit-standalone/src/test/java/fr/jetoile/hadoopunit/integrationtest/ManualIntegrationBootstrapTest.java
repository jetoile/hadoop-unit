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

package fr.jetoile.hadoopunit.integrationtest;


import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.mongodb.*;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import fr.jetoile.hadoopunit.exception.NotFoundServiceException;
import fr.jetoile.hadoopunit.test.alluxio.AlluxioUtils;
import fr.jetoile.hadoopunit.test.hdfs.HdfsUtils;
import fr.jetoile.hadoopunit.test.kafka.KafkaConsumerUtils;
import fr.jetoile.hadoopunit.test.kafka.KafkaProducerUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.zookeeper.KeeperException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.*;
import org.neo4j.driver.v1.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.*;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.*;

@Ignore
public class ManualIntegrationBootstrapTest {

    static private Configuration configuration;

    static private Logger LOGGER = LoggerFactory.getLogger(ManualIntegrationBootstrapTest.class);

    public static final int NB_FILE = 1;
    public static final String PATH = "/fooDirectory";

    @BeforeClass
    public static void setup() throws BootstrapException {
        try {
            configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }
    }


    @AfterClass
    public static void tearDown() throws BootstrapException {
    }

    @Test
    public void solrCloudShouldStart() throws IOException, SolrServerException, KeeperException, InterruptedException {

        String collectionName = configuration.getString(HadoopUnitConfig.SOLR_COLLECTION_NAME);

        String zkHostString = configuration.getString(HadoopUnitConfig.ZOOKEEPER_HOST_KEY) + ":" + configuration.getInt(HadoopUnitConfig.ZOOKEEPER_PORT_KEY);
        CloudSolrClient client = new CloudSolrClient(zkHostString);

        for (int i = 0; i < 1000; ++i) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("cat", "book");
            doc.addField("id", "book-" + i);
            doc.addField("name", "The Legend of the Hobbit part " + i);
            client.add(collectionName, doc);
            if (i % 100 == 0) client.commit(collectionName);  // periodically flush
        }
        client.commit("collection1");

        SolrDocument collection1 = client.getById(collectionName, "book-1");

        assertNotNull(collection1);

        assertThat(collection1.getFieldValue("name")).isEqualTo("The Legend of the Hobbit part 1");


        client.close();
    }

    @Test
    public void kafkaShouldStart() throws Exception {

        // Producer
        for (int i = 0; i < 10; i++) {
            String payload = generateMessage(i);
            KafkaProducerUtils.INSTANCE.produceMessages(configuration.getString(HadoopUnitConfig.KAFKA_TEST_TOPIC_KEY), String.valueOf(i), payload);
        }


        // Consumer
        KafkaConsumerUtils.INSTANCE.consumeMessagesWithNewApi(configuration.getString(HadoopUnitConfig.KAFKA_TEST_TOPIC_KEY), 10);

        // Assert num of messages produced = num of message consumed
        Assert.assertEquals(configuration.getLong(HadoopUnitConfig.KAFKA_TEST_MESSAGE_COUNT_KEY), KafkaConsumerUtils.INSTANCE.getNumRead());
    }

    private String generateMessage(int i) {
        JSONObject obj = new JSONObject();
        try {
            obj.put("id", String.valueOf(i));
            obj.put("msg", "test-message" + 1);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return obj.toString();
    }

    @Test
    public void hiveServer2ShouldStart() throws InterruptedException, ClassNotFoundException, SQLException {

//        assertThat(Utils.available("127.0.0.1", 20103)).isFalse();

        // Load the Hive JDBC driver
        LOGGER.info("HIVE: Loading the Hive JDBC Driver");
        Class.forName("org.apache.hive.jdbc.HiveDriver");

        //
        // Create an ORC table and describe it
        //
        // Get the connection
        Connection con = DriverManager.getConnection("jdbc:hive2://" +
                        configuration.getString(HadoopUnitConfig.HIVE_SERVER2_HOSTNAME_KEY) + ":" +
                        configuration.getInt(HadoopUnitConfig.HIVE_SERVER2_PORT_KEY) + "/" +
                        configuration.getString(HadoopUnitConfig.HIVE_TEST_DATABASE_NAME_KEY),
                "user",
                "pass");

        // Create the DB
        Statement stmt;
        try {
            String createDbDdl = "CREATE DATABASE IF NOT EXISTS " +
                    configuration.getString(HadoopUnitConfig.HIVE_TEST_DATABASE_NAME_KEY);
            stmt = con.createStatement();
            LOGGER.info("HIVE: Running Create Database Statement: {}", createDbDdl);
            stmt.execute(createDbDdl);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Drop the table incase it still exists
        String dropDdl = "DROP TABLE " + configuration.getString(HadoopUnitConfig.HIVE_TEST_DATABASE_NAME_KEY) + "." +
                configuration.getString(HadoopUnitConfig.HIVE_TEST_TABLE_NAME_KEY);
        stmt = con.createStatement();
        LOGGER.info("HIVE: Running Drop Table Statement: {}", dropDdl);
        stmt.execute(dropDdl);

        // Create the ORC table
        String createDdl = "CREATE TABLE IF NOT EXISTS " +
                configuration.getString(HadoopUnitConfig.HIVE_TEST_DATABASE_NAME_KEY) + "." +
                configuration.getString(HadoopUnitConfig.HIVE_TEST_TABLE_NAME_KEY) + " (id INT, msg STRING) " +
                "PARTITIONED BY (dt STRING) " +
                "CLUSTERED BY (id) INTO 16 BUCKETS " +
                "STORED AS ORC tblproperties(\"orc.compress\"=\"NONE\")";
        stmt = con.createStatement();
        LOGGER.info("HIVE: Running Create Table Statement: {}", createDdl);
        stmt.execute(createDdl);

        // Issue a describe on the new table and display the output
        LOGGER.info("HIVE: Validating Table was Created: ");
        ResultSet resultSet = stmt.executeQuery("DESCRIBE FORMATTED " +
                configuration.getString(HadoopUnitConfig.HIVE_TEST_TABLE_NAME_KEY));
        int count = 0;
        while (resultSet.next()) {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                System.out.print(resultSet.getString(i));
            }
            System.out.println();
            count++;
        }
        assertEquals(33, count);

        // Drop the table
        dropDdl = "DROP TABLE " + configuration.getString(HadoopUnitConfig.HIVE_TEST_DATABASE_NAME_KEY) + "." +
                configuration.getString(HadoopUnitConfig.HIVE_TEST_TABLE_NAME_KEY);
        stmt = con.createStatement();
        LOGGER.info("HIVE: Running Drop Table Statement: {}", dropDdl);
        stmt.execute(dropDdl);
    }


    @Test
    public void hdfsShouldStart() throws Exception {
        FileSystem hdfsFsHandle = HdfsUtils.INSTANCE.getFileSystem();


        FSDataOutputStream writer = hdfsFsHandle.create(new Path(configuration.getString(HadoopUnitConfig.HDFS_TEST_FILE_KEY)));
        writer.writeUTF(configuration.getString(HadoopUnitConfig.HDFS_TEST_STRING_KEY));
        writer.close();

        // Read the file and compare to test string
        FSDataInputStream reader = hdfsFsHandle.open(new Path(configuration.getString(HadoopUnitConfig.HDFS_TEST_FILE_KEY)));
        assertEquals(reader.readUTF(), configuration.getString(HadoopUnitConfig.HDFS_TEST_STRING_KEY));
        reader.close();

        URL url = new URL(
                String.format("http://%s:%s/webhdfs/v1?op=GETHOMEDIRECTORY&user.name=guest",
                        configuration.getString(HadoopUnitConfig.HDFS_NAMENODE_HOST_KEY),
                        configuration.getInt(HadoopUnitConfig.HDFS_NAMENODE_HTTP_PORT_KEY)));
        URLConnection connection = url.openConnection();
        connection.setRequestProperty("Accept-Charset", "UTF-8");
        BufferedReader response = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        String line = response.readLine();
        response.close();
        assertThat("{\"Path\":\"/user/guest\"}").isEqualTo(line);

    }

    @Test
    public void hBaseShouldStart() throws Exception {

        String tableName = configuration.getString(HadoopUnitConfig.HBASE_TEST_TABLE_NAME_KEY);
        String colFamName = configuration.getString(HadoopUnitConfig.HBASE_TEST_COL_FAMILY_NAME_KEY);
        String colQualiferName = configuration.getString(HadoopUnitConfig.HBASE_TEST_COL_QUALIFIER_NAME_KEY);
        Integer numRowsToPut = configuration.getInt(HadoopUnitConfig.HBASE_TEST_NUM_ROWS_TO_PUT_KEY);

        org.apache.hadoop.conf.Configuration hbaseConfiguration = HBaseConfiguration.create();
        hbaseConfiguration.set("hbase.zookeeper.quorum", configuration.getString(HadoopUnitConfig.ZOOKEEPER_HOST_KEY));
        hbaseConfiguration.setInt("hbase.zookeeper.property.clientPort", configuration.getInt(HadoopUnitConfig.ZOOKEEPER_PORT_KEY));
        hbaseConfiguration.set("hbase.master", "127.0.0.1:" + configuration.getInt(HadoopUnitConfig.HBASE_MASTER_PORT_KEY));
        hbaseConfiguration.set("zookeeper.znode.parent", configuration.getString(HadoopUnitConfig.HBASE_ZNODE_PARENT_KEY));

        LOGGER.info("HBASE: Deleting table {}", tableName);
        deleteHbaseTable(tableName, hbaseConfiguration);

        LOGGER.info("HBASE: Creating table {} with column family {}", tableName, colFamName);
        createHbaseTable(tableName, colFamName, hbaseConfiguration);

        LOGGER.info("HBASE: Populate the table with {} rows.", numRowsToPut);
        for (int i = 0; i < numRowsToPut; i++) {
            putRow(tableName, colFamName, String.valueOf(i), colQualiferName, "row_" + i, hbaseConfiguration);
        }

        LOGGER.info("HBASE: Fetching and comparing the results");
        for (int i = 0; i < numRowsToPut; i++) {
            Result result = getRow(tableName, colFamName, String.valueOf(i), colQualiferName, hbaseConfiguration);
            assertEquals("row_" + i, new String(result.value()));
        }

    }


    @Test
    @Ignore
    public void oozieWithRealHiveWorkflowShouldStart() throws Exception {

        LOGGER.info("OOZIE: Test Submit Workflow Start");

        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.default.name", "hdfs://" + configuration.getString(HadoopUnitConfig.HDFS_NAMENODE_HOST_KEY) + ":" + configuration.getInt(HadoopUnitConfig.HDFS_NAMENODE_PORT_KEY));
        URI uri = URI.create("hdfs://" + configuration.getString(HadoopUnitConfig.HDFS_NAMENODE_HOST_KEY) + ":" + configuration.getInt(HadoopUnitConfig.HDFS_NAMENODE_PORT_KEY));
        FileSystem hdfsFs = FileSystem.get(uri, conf);
        OozieClient oozieClient = new OozieClient("http://" + configuration.getString(HadoopUnitConfig.OOZIE_HOST) + ":" + configuration.getInt(HadoopUnitConfig.OOZIE_PORT) + "/oozie");


        hdfsFs.mkdirs(new Path("/khanh/test2"));
        hdfsFs.mkdirs(new Path("/khanh/work2"));
        hdfsFs.mkdirs(new Path("/khanh/etc2"));
        hdfsFs.copyFromLocalFile(new Path(ManualIntegrationBootstrapTest.class.getClassLoader().getResource("workflow2.xml").toURI()), new Path("/khanh/test2/workflow.xml"));
        hdfsFs.copyFromLocalFile(new Path(ManualIntegrationBootstrapTest.class.getClassLoader().getResource("hive-site.xml").toURI()), new Path("/khanh/etc2/hive-site.xml"));
        hdfsFs.copyFromLocalFile(new Path(ManualIntegrationBootstrapTest.class.getClassLoader().getResource("test.csv").toURI()), new Path("/khanh/work2/test.csv"));
        hdfsFs.copyFromLocalFile(new Path(ManualIntegrationBootstrapTest.class.getClassLoader().getResource("test.hql").toURI()), new Path("/khanh/etc2/test.hql"));

        //write job.properties
        Properties oozieConf = oozieClient.createConfiguration();
        oozieConf.setProperty(OozieClient.APP_PATH, "hdfs://localhost:20112/khanh/test2/workflow.xml");
        oozieConf.setProperty(OozieClient.USER_NAME, UserGroupInformation.getCurrentUser().getUserName());
        oozieConf.setProperty("jobTracker", "localhost:37001");
        oozieConf.setProperty("nameNode", "hdfs://localhost:20112");
        oozieConf.setProperty("hiveTry", "hdfs://localhost:20112/khanh/etc2/test.hql");

        //submit and check
        final String jobId = oozieClient.run(oozieConf);

        while (oozieClient.getJobInfo(jobId).getStatus() != WorkflowJob.Status.RUNNING) {
            System.out.println("========== workflow job status " + oozieClient.getJobInfo(jobId).getStatus());
            Thread.sleep(1000);
        }

        while (oozieClient.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING) {
            System.out.println("========== workflow job status " + oozieClient.getJobInfo(jobId).getStatus());
            System.out.println("========== job is running");
            Thread.sleep(1000);
        }

        System.out.println("=============== OOZIE: Final Workflow status" + oozieClient.getJobInfo(jobId).getStatus());
        WorkflowJob wf = oozieClient.getJobInfo(jobId);
        System.out.println("=============== OOZIE: Workflow: {}" + wf.toString());

        assertEquals(WorkflowJob.Status.SUCCEEDED, wf.getStatus());

        hdfsFs.close();
    }

    @Test
    @Ignore
    public void oozieWithRealWorkflowShouldStart() throws Exception {

        LOGGER.info("OOZIE: Test Submit Workflow Start");

        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.default.name", "hdfs://" + configuration.getString(HadoopUnitConfig.HDFS_NAMENODE_HOST_KEY) + ":" + configuration.getInt(HadoopUnitConfig.HDFS_NAMENODE_PORT_KEY));
        URI uri = URI.create("hdfs://" + configuration.getString(HadoopUnitConfig.HDFS_NAMENODE_HOST_KEY) + ":" + configuration.getInt(HadoopUnitConfig.HDFS_NAMENODE_PORT_KEY));
        FileSystem hdfsFs = FileSystem.get(uri, conf);
        OozieClient oozieClient = new OozieClient("http://" + configuration.getString(HadoopUnitConfig.OOZIE_HOST) + ":" + configuration.getInt(HadoopUnitConfig.OOZIE_PORT) + "/oozie");


        String testInputFile = "test_input.txt";
        String testInputDir = "/tmp/test_input_dir";

        // Setup input directory and file
        hdfsFs.mkdirs(new Path(testInputDir));
        hdfsFs.copyFromLocalFile(
                new Path(getClass().getClassLoader().getResource(testInputFile).toURI()), new Path(testInputDir));

        hdfsFs.mkdirs(new Path("/khanh/test"));
        hdfsFs.copyFromLocalFile(new Path(ManualIntegrationBootstrapTest.class.getClassLoader().getResource("workflow2.xml").toURI()), new Path("/khanh/test/workflow.xml"));

        //write job.properties
        Properties oozieConf = oozieClient.createConfiguration();
        oozieConf.setProperty(OozieClient.APP_PATH, "hdfs://localhost:20112/khanh/test/workflow.xml");
        oozieConf.setProperty(OozieClient.USER_NAME, UserGroupInformation.getCurrentUser().getUserName());
        oozieConf.setProperty("jobTracker", "localhost:37001");
        oozieConf.setProperty("nameNode", "hdfs://localhost:20112");
        oozieConf.setProperty("doOption", "true");

        //submit and check
        final String jobId = oozieClient.run(oozieConf);
        WorkflowJob wf = oozieClient.getJobInfo(jobId);
        assertNotNull(wf);

        LOGGER.info("OOZIE: Workflow: {}", wf.toString());

        while (oozieClient.getJobInfo(jobId).getStatus() != WorkflowJob.Status.RUNNING) {
            System.out.println("========== workflow job status " + oozieClient.getJobInfo(jobId).getStatus());
            Thread.sleep(1000);
        }

        while (oozieClient.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING) {
            System.out.println("========== workflow job status " + oozieClient.getJobInfo(jobId).getStatus());
            System.out.println("========== job is running");
            Thread.sleep(1000);
        }

        System.out.println("=============== OOZIE: Final Workflow status" + oozieClient.getJobInfo(jobId).getStatus());
        wf = oozieClient.getJobInfo(jobId);
        System.out.println("=============== OOZIE: Workflow: {}" + wf.toString());

        assertEquals(WorkflowJob.Status.SUCCEEDED, wf.getStatus());


        hdfsFs.close();
    }

    @Test
    public void oozieShouldStart() throws Exception {

        LOGGER.info("OOZIE: Test Submit Workflow Start");

        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.default.name", "hdfs://" + configuration.getString(HadoopUnitConfig.HDFS_NAMENODE_HOST_KEY) + ":" + configuration.getInt(HadoopUnitConfig.HDFS_NAMENODE_PORT_KEY));

        URI uri = URI.create("hdfs://" + configuration.getString(HadoopUnitConfig.HDFS_NAMENODE_HOST_KEY) + ":" + configuration.getInt(HadoopUnitConfig.HDFS_NAMENODE_PORT_KEY));

        FileSystem hdfsFs = FileSystem.get(uri, conf);

        OozieClient oozieClient = new OozieClient("http://" + configuration.getString(HadoopUnitConfig.OOZIE_HOST) + ":" + configuration.getInt(HadoopUnitConfig.OOZIE_PORT) + "/oozie");

        Path appPath = new Path(hdfsFs.getHomeDirectory(), "testApp");
        hdfsFs.mkdirs(new Path(appPath, "lib"));
        Path workflow = new Path(appPath, "workflow.xml");

        //write workflow.xml
        String wfApp = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='test-wf'>" +
                "    <start to='end'/>" +
                "    <end name='end'/>" +
                "</workflow-app>";

        Writer writer = new OutputStreamWriter(hdfsFs.create(workflow));
        writer.write(wfApp);
        writer.close();

        //write job.properties
        Properties oozieConf = oozieClient.createConfiguration();
        oozieConf.setProperty(OozieClient.APP_PATH, workflow.toString());
        oozieConf.setProperty(OozieClient.USER_NAME, UserGroupInformation.getCurrentUser().getUserName());

        //submit and check
        final String jobId = oozieClient.submit(oozieConf);
        WorkflowJob wf = oozieClient.getJobInfo(jobId);
        assertNotNull(wf);
        assertEquals(WorkflowJob.Status.PREP, wf.getStatus());

        LOGGER.info("OOZIE: Workflow: {}", wf.toString());
        hdfsFs.close();

    }

    @Test
    public void knoxWithWebhbaseShouldStart() throws Exception {

        String tableName = configuration.getString(HadoopUnitConfig.HBASE_TEST_TABLE_NAME_KEY);
        String colFamName = configuration.getString(HadoopUnitConfig.HBASE_TEST_COL_FAMILY_NAME_KEY);
        String colQualiferName = configuration.getString(HadoopUnitConfig.HBASE_TEST_COL_QUALIFIER_NAME_KEY);
        Integer numRowsToPut = configuration.getInt(HadoopUnitConfig.HBASE_TEST_NUM_ROWS_TO_PUT_KEY);

        org.apache.hadoop.conf.Configuration hbaseConfiguration = HBaseConfiguration.create();
        hbaseConfiguration.set("hbase.zookeeper.quorum", configuration.getString(HadoopUnitConfig.ZOOKEEPER_HOST_KEY));
        hbaseConfiguration.setInt("hbase.zookeeper.property.clientPort", configuration.getInt(HadoopUnitConfig.ZOOKEEPER_PORT_KEY));
        hbaseConfiguration.set("hbase.master", "127.0.0.1:" + configuration.getInt(HadoopUnitConfig.HBASE_MASTER_PORT_KEY));
        hbaseConfiguration.set("zookeeper.znode.parent", configuration.getString(HadoopUnitConfig.HBASE_ZNODE_PARENT_KEY));

        LOGGER.info("HBASE: Deleting table {}", tableName);
        deleteHbaseTable(tableName, hbaseConfiguration);

        LOGGER.info("HBASE: Creating table {} with column family {}", tableName, colFamName);
        createHbaseTable(tableName, colFamName, hbaseConfiguration);

        LOGGER.info("HBASE: Populate the table with {} rows.", numRowsToPut);
        for (int i = 0; i < numRowsToPut; i++) {
            putRow(tableName, colFamName, String.valueOf(i), colQualiferName, "row_" + i, hbaseConfiguration);
        }

        URL url = new URL(String.format("http://%s:%s/",
                configuration.getString(HadoopUnitConfig.KNOX_HOST_KEY),
                configuration.getString(HadoopUnitConfig.HBASE_REST_PORT_KEY)));
        URLConnection connection = url.openConnection();
        connection.setRequestProperty("Accept-Charset", "UTF-8");
        try (BufferedReader response = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String line = response.readLine();
            assertTrue(line.contains(tableName));
        }

        url = new URL(String.format("http://%s:%s/%s/schema",
                configuration.getString(HadoopUnitConfig.KNOX_HOST_KEY),
                configuration.getString(HadoopUnitConfig.HBASE_REST_PORT_KEY),
                tableName));
        connection = url.openConnection();
        connection.setRequestProperty("Accept-Charset", "UTF-8");
        try (BufferedReader response = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String line = response.readLine();
            assertTrue(line.contains("{ NAME=> 'hbase_test_table', IS_META => 'false', COLUMNS => [ { NAME => 'cf1', BLOOMFILTER => 'ROW'"));
        }

        // Knox clients need self trusted certificates in tests
        defaultBlindTrust();

        // Read the hbase throught Knox
        url = new URL(String.format("https://%s:%s/gateway/mycluster/hbase",
                configuration.getString(HadoopUnitConfig.KNOX_HOST_KEY),
                configuration.getString(HadoopUnitConfig.KNOX_PORT_KEY)));
        connection = url.openConnection();
        connection.setRequestProperty("Accept-Charset", "UTF-8");
        try (BufferedReader response = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String line = response.readLine();
            assertTrue(line.contains(tableName));
        }

        url = new URL(String.format("https://%s:%s/gateway/mycluster/hbase/%s/schema",
                configuration.getString(HadoopUnitConfig.KNOX_HOST_KEY),
                configuration.getString(HadoopUnitConfig.KNOX_PORT_KEY),
                tableName));
        connection = url.openConnection();
        connection.setRequestProperty("Accept-Charset", "UTF-8");
        try (BufferedReader response = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String line = response.readLine();
            assertTrue(line.contains("{ NAME=> 'hbase_test_table', IS_META => 'false', COLUMNS => [ { NAME => 'cf1', BLOOMFILTER => 'ROW'"));
        }
    }

    @Test
    public void alluxioShouldStart() throws IOException, AlluxioException, InterruptedException {
        alluxio.client.file.FileSystem fs = AlluxioUtils.INSTANCE.getFileSystem();
        writeFile(fs);

        assertTrue(readFile(fs));

        HdfsUtils.INSTANCE.getFileSystem().mkdirs(new Path("/khanh/alluxio"));
        FSDataOutputStream writer = HdfsUtils.INSTANCE.getFileSystem().create(new Path("/khanh/alluxio/test.txt"), true);
        writer.writeUTF(configuration.getString(HadoopUnitConfig.HDFS_TEST_STRING_KEY));
        writer.close();

        fs.mount(new AlluxioURI(PATH + "/hdfs"), new AlluxioURI("hdfs://localhost:20112/khanh/alluxio"));
        assertTrue(fs.exists(new AlluxioURI(PATH + "/hdfs/test.txt")));

        fs.unmount(new AlluxioURI(PATH + "/hdfs"));
        assertFalse(fs.exists(new AlluxioURI(PATH + "/hdfs/test.txt")));
    }

    private boolean readFile(alluxio.client.file.FileSystem fs) throws IOException, AlluxioException {
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

    private void writeFile(alluxio.client.file.FileSystem fs) throws IOException, AlluxioException {
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

    private static void createHbaseTable(String tableName, String colFamily,
                                         org.apache.hadoop.conf.Configuration configuration) throws Exception {

        final HBaseAdmin admin = new HBaseAdmin(configuration);
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(colFamily);

        hTableDescriptor.addFamily(hColumnDescriptor);
        admin.createTable(hTableDescriptor);
    }

    private static void deleteHbaseTable(String tableName, org.apache.hadoop.conf.Configuration configuration) throws Exception {

        final HBaseAdmin admin = new HBaseAdmin(configuration);
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
    }

    private static void putRow(String tableName, String colFamName, String rowKey, String colQualifier, String value,
                               org.apache.hadoop.conf.Configuration configuration) throws Exception {
        HTable table = new HTable(configuration, tableName);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(colFamName), Bytes.toBytes(colQualifier), Bytes.toBytes(value));
        table.put(put);
        table.flushCommits();
        table.close();
    }

    private static Result getRow(String tableName, String colFamName, String rowKey, String colQualifier,
                                 org.apache.hadoop.conf.Configuration configuration) throws Exception {
        Result result;
        HTable table = new HTable(configuration, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(colFamName), Bytes.toBytes(colQualifier));
        get.setMaxVersions(1);
        result = table.get(get);
        return result;
    }

    private void defaultBlindTrust() throws NoSuchAlgorithmException, KeyManagementException {
        TrustManager[] trustAllCerts = new TrustManager[]{
                new X509ExtendedTrustManager() {
                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }

                    @Override
                    public void checkClientTrusted(X509Certificate[] certs, String authType) {
                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] certs, String authType) {
                    }

                    @Override
                    public void checkClientTrusted(X509Certificate[] xcs, String string, Socket socket) throws CertificateException {

                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] xcs, String string, Socket socket) throws CertificateException {

                    }

                    @Override
                    public void checkClientTrusted(X509Certificate[] xcs, String string, SSLEngine ssle) throws CertificateException {

                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] xcs, String string, SSLEngine ssle) throws CertificateException {

                    }

                }
        };
        SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, trustAllCerts, new java.security.SecureRandom());
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        HostnameVerifier allHostsValid = new HostnameVerifier() {
            @Override
            public boolean verify(String hostname, SSLSession session) {
                return true;
            }
        };
        HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
    }

    @Test
    public void mongodbShouldStart() throws UnknownHostException {
        MongoClient mongo = new MongoClient(configuration.getString(HadoopUnitConfig.MONGO_IP_KEY), configuration.getInt(HadoopUnitConfig.MONGO_PORT_KEY));

        DB db = mongo.getDB(configuration.getString(HadoopUnitConfig.MONGO_DATABASE_NAME_KEY));
        DBCollection col = db.createCollection(configuration.getString(HadoopUnitConfig.MONGO_COLLECTION_NAME_KEY),
                new BasicDBObject());

        col.save(new BasicDBObject("testDoc", new java.util.Date()));
        LOGGER.info("MONGODB: Number of items in collection: {}", col.count());
        assertEquals(1, col.count());

        DBCursor cursor = col.find();
        while (cursor.hasNext()) {
            LOGGER.info("MONGODB: Document output: {}", cursor.next());
        }
        cursor.close();
    }

    @Test
    public void cassandraShouldStart() throws NotFoundServiceException {
        Cluster cluster = Cluster.builder()
                .addContactPoints(configuration.getString(HadoopUnitConfig.CASSANDRA_IP_KEY)).withPort(configuration.getInt(HadoopUnitConfig.CASSANDRA_PORT_KEY)).build();
        Session session = cluster.connect();

        session.execute("create KEYSPACE test WITH replication = {'class': 'SimpleStrategy' , 'replication_factor': '1' }");
        session.execute("CREATE TABLE test.test (user text, value text, PRIMARY KEY (user))");
        session.execute("insert into test.test(user, value) values('user1', 'value1')");
        session.execute("insert into test.test(user, value) values('user2', 'value2')");

        com.datastax.driver.core.ResultSet execute = session.execute("select * from test.test");

        List<com.datastax.driver.core.Row> res = execute.all();
        assertEquals(res.size(), 2);
        assertEquals(res.get(0).getString("user"), "user2");
        assertEquals(res.get(0).getString("value"), "value2");
        assertEquals(res.get(1).getString("user"), "user1");

    }

    @Test
    public void elasticSearchShouldStartWithRealDriver() throws NotFoundServiceException, IOException {

        Settings settings = Settings.builder()
                .put("cluster.name", configuration.getString(HadoopUnitConfig.ELASTICSEARCH_CLUSTER_NAME))
                .build();
        Client client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(configuration.getString(HadoopUnitConfig.ELASTICSEARCH_IP_KEY)), configuration.getInt(HadoopUnitConfig.ELASTICSEARCH_TCP_PORT_KEY)));

        ObjectMapper mapper = new ObjectMapper();

        Sample sample = new Sample("value2", 0.33, 3);

        String jsonString = mapper.writeValueAsString(sample);

        // indexing document
        IndexResponse ir = client.prepareIndex("test_index", "type").setSource(jsonString).setId("2").execute().actionGet();
        client.admin().indices().prepareRefresh("test_index").execute().actionGet();

        assertNotNull(ir);

        GetResponse gr = client.prepareGet("test_index", "type", "2").execute().actionGet();

        assertNotNull(gr);
        assertEquals(gr.getSourceAsString(), "{\"value\":\"value2\",\"size\":0.33,\"price\":3.0}");

    }

    @Test
    public void neo4jShouldStartWithRealDriver() {

        org.neo4j.driver.v1.Driver driver = GraphDatabase.driver(
                "bolt://localhost:13533",
                Config.build()
                        .withEncryptionLevel(Config.EncryptionLevel.NONE)
                        .toConfig()
        );

        List<Record> results = new ArrayList<>();
        try (org.neo4j.driver.v1.Session session = driver.session()) {
            session.run("CREATE (person:Person {name: {name}, title:'King'})", Values.parameters("name", "Arthur"));

            StatementResult result = session.run("MATCH (a:Person) WHERE a.name = 'Arthur' RETURN a.name AS name, a.title AS title");
            while (result.hasNext()) {
                Record record = result.next();
                results.add(record);
                LOGGER.debug(record.get("title").asString() + " " + record.get("name").asString());
            }
        }

        assertEquals(1, results.size());
        assertEquals("King", results.get(0).get("title").asString());
        assertEquals("Arthur", results.get(0).get("name").asString());
    }

}


class Sample implements Serializable {
    private static final long serialVersionUID = 1L;

    private String value;
    private double size;
    private double price;

    public Sample(String value, double size, double price) {
        this.value = value;
        this.size = size;
        this.price = price;
    }

    public double getSize() {
        return size;
    }

    public void setSize(double size) {
        this.size = size;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object obj) {

        if (obj == null) return false;
        if (!(obj instanceof Sample)) return false;

        Sample sample = (Sample) obj;

        if (this.value != sample.value && this.value != null && !this.value.equals(sample.value)) return false;

        if (this.size != sample.size) return false;
        if (this.price != sample.price) return false;

        return true;
    }
}