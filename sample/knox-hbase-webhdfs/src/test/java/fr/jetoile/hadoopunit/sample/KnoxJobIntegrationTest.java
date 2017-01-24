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
package fr.jetoile.hadoopunit.sample;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.gateway.shell.Hadoop;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import static org.junit.Assert.assertTrue;

public class KnoxJobIntegrationTest {

    private static Hadoop hadoop;

    @BeforeClass
    static public void setUp() throws ConfigurationException, URISyntaxException {
        Configuration configuration = new PropertiesConfiguration("src/test/resources/hadoop-unit-default.properties");

        String host = configuration.getString("knox.host");
        String port = configuration.getString("knox.port");
        String gateway = configuration.getString("knox.path");
        String cluster = configuration.getString("knox.cluster");

        hadoop = Hadoop.loginInsecure("https://" + host + ":" + port + "/" + gateway + "/" + cluster, "none", "none");
    }

    @AfterClass
    static public void tearDown() throws InterruptedException {
        hadoop.shutdown();
    }

    @Test
    public void hdfs_through_knox_should_be_ok() throws ConfigurationException, KeyManagementException, URISyntaxException, IOException, NoSuchAlgorithmException {
        KnoxJob knoxJob = new KnoxJob();
        knoxJob.createHdfsDirectory(hadoop);
        knoxJob.createFileOnHdfs(hadoop);
        knoxJob.getFileOnHdfs(hadoop);
    }


    @Test
    public void hbase_through_knox_should_be_ok() throws ConfigurationException, KeyManagementException, URISyntaxException, IOException, NoSuchAlgorithmException {
        KnoxJob knoxJob = new KnoxJob();

        assertTrue(knoxJob.getHBaseStatus(hadoop).contains("\"regions\":3"));
        knoxJob.createHBaseTable(hadoop);

        System.out.println("==============================");
        System.out.println(knoxJob.getHBaseTableSchema(hadoop));
        System.out.println("==============================");

        assertTrue(knoxJob.getHBaseTableSchema(hadoop).contains("{\"name\":\"test\",\"ColumnSchema\":[{\"name\":\"family1\""));
        knoxJob.putHBaseData(hadoop);

        System.out.println("==============================");
        System.out.println(knoxJob.readHBaseData(hadoop));
        System.out.println("==============================");

        assertTrue(knoxJob.readHBaseData(hadoop).contains("{\"Row\":[{\"key\":\""));
    }
}