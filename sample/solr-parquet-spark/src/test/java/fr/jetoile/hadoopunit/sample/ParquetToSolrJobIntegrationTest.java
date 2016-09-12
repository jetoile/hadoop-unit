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

import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import fr.jetoile.hadoopunit.exception.NotFoundServiceException;
import fr.jetoile.hadoopunit.test.hdfs.HdfsUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;

import static junit.framework.TestCase.assertNotNull;
import static org.fest.assertions.Assertions.assertThat;

public class ParquetToSolrJobIntegrationTest {

    static private Logger LOGGER = LoggerFactory.getLogger(ParquetToSolrJobIntegrationTest.class);


    private static Configuration configuration;


    @BeforeClass
    public static void setUp() throws BootstrapException, SQLException, ClassNotFoundException, NotFoundServiceException {
        try {
            configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }

    }

    @Before
    public void before() throws IOException, URISyntaxException {
        FileSystem fileSystem = HdfsUtils.INSTANCE.getFileSystem();

        fileSystem.mkdirs(new Path("/khanh/test"));
        fileSystem.mkdirs(new Path("/khanh/test_parquet"));
        fileSystem.copyFromLocalFile(new Path(ParquetToSolrJobIntegrationTest.class.getClassLoader().getResource("test.csv").toURI()), new Path("/khanh/test/test.csv"));
    }

    @After
    public void clean() throws IOException {
        FileSystem fileSystem = HdfsUtils.INSTANCE.getFileSystem();
        fileSystem.delete(new Path("/khanh"), true);
    }


    @Test
    public void spark_should_read_parquet_file_and_index_into_solr() throws IOException, SolrServerException {
        //given
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("test");

        JavaSparkContext context = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(context);
        DataFrame df = sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("header", "true") // Use first line of all files as header
                .option("inferSchema", "true") // Automatically infer data types
                .load("hdfs://localhost:" + configuration.getInt(HadoopUnitConfig.HDFS_NAMENODE_PORT_KEY) + "/khanh/test/test.csv");

        df.write().parquet("hdfs://localhost:" + configuration.getInt(HadoopUnitConfig.HDFS_NAMENODE_PORT_KEY) + "/khanh/test_parquet/file.parquet");

        FileSystem fileSystem = HdfsUtils.INSTANCE.getFileSystem();
        assertThat(fileSystem.exists(new Path("hdfs://localhost:" + configuration.getInt(HadoopUnitConfig.HDFS_NAMENODE_PORT_KEY) + "/khanh/test_parquet/file.parquet"))).isTrue();

        context.close();

        //when
        context = new JavaSparkContext(conf);

        ParquetToSolrJob parquetToSolrJob = new ParquetToSolrJob(context);
        parquetToSolrJob.run();

        String zkHostString = configuration.getString(HadoopUnitConfig.ZOOKEEPER_HOST_KEY) + ":" + configuration.getInt(HadoopUnitConfig.ZOOKEEPER_PORT_KEY);

        //then
        CloudSolrClient client = new CloudSolrClient(zkHostString);
        SolrDocument collection1 = client.getById("collection1", "1");

        assertNotNull(collection1);
        assertThat(collection1.getFieldValue("value_s")).isEqualTo("value1");


        client.close();


        context.close();


    }

}