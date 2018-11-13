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
import fr.jetoile.hadoopunit.exception.BootstrapException;
import fr.jetoile.hadoopunit.test.hdfs.HdfsUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.URL;
import java.net.URLConnection;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KnoxBootstrapTest {
    static private Logger LOGGER = LoggerFactory.getLogger(KnoxBootstrapTest.class);


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
    public void testKnoxWithWebhbase() throws Exception {

        String tableName = configuration.getString(KnoxConfig.HBASE_TEST_TABLE_NAME_KEY);
        String colFamName = configuration.getString(KnoxConfig.HBASE_TEST_COL_FAMILY_NAME_KEY);
        String colQualiferName = configuration.getString(KnoxConfig.HBASE_TEST_COL_QUALIFIER_NAME_KEY);
        Integer numRowsToPut = configuration.getInt(KnoxConfig.HBASE_TEST_NUM_ROWS_TO_PUT_KEY);
        org.apache.hadoop.conf.Configuration hbaseConfiguration = ((BootstrapHadoop) HadoopBootstrap.INSTANCE.getService("HBASE")).getConfiguration();

        LOGGER.info("HBASE: Creating table {} with column family {}", tableName, colFamName);
        createHbaseTable(tableName, colFamName, hbaseConfiguration);

        LOGGER.info("HBASE: Populate the table with {} rows.", numRowsToPut);
        for (int i = 0; i < numRowsToPut; i++) {
            putRow(tableName, colFamName, String.valueOf(i), colQualiferName, "row_" + i, hbaseConfiguration);
        }

        URL url = new URL(String.format("http://%s:%s/",
                configuration.getString(KnoxConfig.KNOX_HOST_KEY),
                configuration.getString(KnoxConfig.HBASE_REST_PORT_KEY)));
        URLConnection connection = url.openConnection();
        connection.setRequestProperty("Accept-Charset", "UTF-8");
        try (BufferedReader response = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String line = response.readLine();
            assertTrue(line.contains(tableName));
        }

        url = new URL(String.format("http://%s:%s/%s/schema",
                configuration.getString(KnoxConfig.KNOX_HOST_KEY),
                configuration.getString(KnoxConfig.HBASE_REST_PORT_KEY),
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
                configuration.getString(KnoxConfig.KNOX_HOST_KEY),
                configuration.getString(KnoxConfig.KNOX_PORT_KEY)));
        connection = url.openConnection();
        connection.setRequestProperty("Accept-Charset", "UTF-8");
        try (BufferedReader response = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String line = response.readLine();
            assertTrue(line.contains(tableName));
        }

        url = new URL(String.format("https://%s:%s/gateway/mycluster/hbase/%s/schema",
                configuration.getString(KnoxConfig.KNOX_HOST_KEY),
                configuration.getString(KnoxConfig.KNOX_PORT_KEY),
                tableName));
        connection = url.openConnection();
        connection.setRequestProperty("Accept-Charset", "UTF-8");
        try (BufferedReader response = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String line = response.readLine();
            assertTrue(line.contains("{ NAME=> 'hbase_test_table', IS_META => 'false', COLUMNS => [ { NAME => 'cf1', BLOOMFILTER => 'ROW'"));
        }
    }

    @Test
    public void testKnoxWithWebhdfs() throws Exception {

        // Write a file to HDFS containing the test string
        FileSystem hdfsFsHandle = HdfsUtils.INSTANCE.getFileSystem();
        try (FSDataOutputStream writer = hdfsFsHandle.create(
                new Path(configuration.getString(KnoxConfig.HDFS_TEST_FILE_KEY)))) {
            writer.write(configuration.getString(KnoxConfig.HDFS_TEST_STRING_KEY).getBytes("UTF-8"));
            writer.flush();
        }

        // Read the file throught webhdfs
        URL url = new URL(
                String.format("http://%s:%s/webhdfs/v1?op=GETHOMEDIRECTORY",
                        configuration.getString(KnoxConfig.KNOX_HOST_KEY),
                        configuration.getString(KnoxConfig.HDFS_NAMENODE_HTTP_PORT_KEY)));
        URLConnection connection = url.openConnection();
        connection.setRequestProperty("Accept-Charset", "UTF-8");
        try (BufferedReader response = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String line = response.readLine();
            assertEquals("{\"Path\":\"/user/dr.who\"}", line);
        }

        url = new URL(
                String.format("http://%s:%s/webhdfs/v1%s?op=OPEN",
                        configuration.getString(KnoxConfig.KNOX_HOST_KEY),
                        configuration.getString(KnoxConfig.HDFS_NAMENODE_HTTP_PORT_KEY),
                        configuration.getString(KnoxConfig.HDFS_TEST_FILE_KEY)));
        connection = url.openConnection();
        connection.setRequestProperty("Accept-Charset", "UTF-8");
        try (BufferedReader response = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String line = response.readLine();
            response.close();
            assertEquals(configuration.getString(KnoxConfig.HDFS_TEST_STRING_KEY), line);
        }

        // Knox clients need self trusted certificates in tests
        defaultBlindTrust();

        // Read the file throught Knox
        url = new URL(
                String.format("https://%s:%s/gateway/mycluster/webhdfs/v1?op=GETHOMEDIRECTORY",
                        configuration.getString(KnoxConfig.KNOX_HOST_KEY),
                        configuration.getString(KnoxConfig.KNOX_PORT_KEY)));
        connection = url.openConnection();
        connection.setRequestProperty("Accept-Charset", "UTF-8");
        try (BufferedReader response = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String line = response.readLine();
            assertEquals("{\"Path\":\"/user/dr.who\"}", line);
        }

        url = new URL(
                String.format("https://%s:%s/gateway/mycluster/webhdfs/v1/%s?op=OPEN",
                        configuration.getString(KnoxConfig.KNOX_HOST_KEY),
                        configuration.getString(KnoxConfig.KNOX_PORT_KEY),
                        configuration.getString(KnoxConfig.HDFS_TEST_FILE_KEY)));
        connection = url.openConnection();
        connection.setRequestProperty("Accept-Charset", "UTF-8");
        try (BufferedReader response = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String line = response.readLine();
            response.close();
            assertEquals(configuration.getString(KnoxConfig.HDFS_TEST_STRING_KEY), line);
        }
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

    private static void createHbaseTable(String tableName, String colFamily,
                                         org.apache.hadoop.conf.Configuration configuration) throws Exception {

        final HBaseAdmin admin = new HBaseAdmin(configuration);
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(colFamily);

        hTableDescriptor.addFamily(hColumnDescriptor);
        admin.createTable(hTableDescriptor);
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
}