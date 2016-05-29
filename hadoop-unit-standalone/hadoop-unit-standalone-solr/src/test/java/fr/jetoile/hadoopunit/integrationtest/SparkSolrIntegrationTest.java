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

import com.lucidworks.spark.SolrSupport;
import com.ninja_squad.dbsetup.Operations;
import com.ninja_squad.dbsetup.operation.Operation;
import fr.jetoile.hadoopunit.Component;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.HadoopBootstrap;
import fr.jetoile.hadoopunit.component.SolrCloudBootstrap;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import fr.jetoile.hadoopunit.exception.NotFoundServiceException;
import fr.jetoile.hadoopunit.test.hdfs.HdfsUtils;
import fr.jetoile.hadoopunit.test.hive.HiveConnectionUtils;
import fr.jetoile.hadoopunit.test.hive.HiveSetup;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;

import static com.ninja_squad.dbsetup.Operations.sequenceOf;
import static com.ninja_squad.dbsetup.Operations.sql;
import static junit.framework.TestCase.assertNotNull;
import static org.fest.assertions.Assertions.assertThat;


@Ignore
public class SparkSolrIntegrationTest {
    static private Logger LOGGER = LoggerFactory.getLogger(SparkSolrIntegrationTest.class);


    private static Configuration configuration;

    public static Operation CREATE_TABLES = null;
    public static Operation DROP_TABLES = null;


    @BeforeClass
    public static void setUp() throws BootstrapException, SQLException, ClassNotFoundException, NotFoundServiceException {
        try {
            configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }

        HadoopBootstrap.INSTANCE
                .add(Component.ZOOKEEPER)
                .add(Component.HDFS)
                .add(Component.HIVEMETA)
                .add(Component.HIVESERVER2)
                .add(Component.SOLRCLOUD)
                .startAll();

        CREATE_TABLES =
                sequenceOf(sql("CREATE EXTERNAL TABLE IF NOT EXISTS default.test(id INT, value STRING) " +
                                " ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'" +
                                " STORED AS TEXTFILE" +
                                " LOCATION 'hdfs://localhost:" + configuration.getInt(HadoopUnitConfig.HDFS_NAMENODE_PORT_KEY) + "/khanh/test'"),
                        sql("CREATE EXTERNAL TABLE IF NOT EXISTS default.test_parquet(id INT, value STRING) " +
                                " ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'" +
                                " STORED AS PARQUET" +
                                " LOCATION 'hdfs://localhost:" + configuration.getInt(HadoopUnitConfig.HDFS_NAMENODE_PORT_KEY) + "/khanh/test_parquet'"));

        DROP_TABLES =
                sequenceOf(sql("DROP TABLE IF EXISTS default.test"),
                        sql("DROP TABLE IF EXISTS default.test_parquet"));
    }

    @AfterClass
    public static void tearDown() throws NotFoundServiceException {

        HadoopBootstrap.INSTANCE
                .stopAll();
    }

    @Before
    public void before() throws IOException, URISyntaxException {
        FileSystem fileSystem = HdfsUtils.INSTANCE.getFileSystem();

        fileSystem.mkdirs(new Path("/khanh/test"));
        fileSystem.mkdirs(new Path("/khanh/test_parquet"));
        fileSystem.copyFromLocalFile(new Path(SparkSolrIntegrationTest.class.getClassLoader().getResource("test.csv").toURI()), new Path("/khanh/test/test.csv"));

        new HiveSetup(HiveConnectionUtils.INSTANCE.getDestination(), Operations.sequenceOf(CREATE_TABLES)).launch();
    }

    @After
    public void clean() throws IOException {
        new HiveSetup(HiveConnectionUtils.INSTANCE.getDestination(), Operations.sequenceOf(DROP_TABLES)).launch();

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

        //read hive-site from classpath
        HiveContext hiveContext = new HiveContext(context.sc());

        DataFrame sql = hiveContext.sql("SELECT * FROM default.test");
        sql.write().parquet("hdfs://localhost:" + configuration.getInt(HadoopUnitConfig.HDFS_NAMENODE_PORT_KEY) + "/khanh/test_parquet/file.parquet");

        FileSystem fileSystem = HdfsUtils.INSTANCE.getFileSystem();
        assertThat(fileSystem.exists(new Path("hdfs://localhost:" + configuration.getInt(HadoopUnitConfig.HDFS_NAMENODE_PORT_KEY) + "/khanh/test_parquet/file.parquet"))).isTrue();

        context.close();

        //when
        context = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(context);

        DataFrame file = sqlContext.read().parquet("hdfs://localhost:" + configuration.getInt(HadoopUnitConfig.HDFS_NAMENODE_PORT_KEY) + "/khanh/test_parquet/file.parquet");
        DataFrame select = file.select("id", "value");

        JavaRDD<SolrInputDocument> solrInputDocument = select.toJavaRDD().map(r -> {
            SolrInputDocument solr = new SolrInputDocument();
            solr.setField("id", r.getInt(0));
            solr.setField("value_s", r.getString(1));
            return solr;
        });

        String collectionName = configuration.getString(SolrCloudBootstrap.SOLR_COLLECTION_NAME);
        String zkHostString = configuration.getString(HadoopUnitConfig.ZOOKEEPER_HOST_KEY) + ":" + configuration.getInt(HadoopUnitConfig.ZOOKEEPER_PORT_KEY);
        SolrSupport.indexDocs(zkHostString, collectionName, 1000, solrInputDocument);

        //then
        CloudSolrClient client = new CloudSolrClient(zkHostString);
        SolrDocument collection1 = client.getById(collectionName, "1");

        assertNotNull(collection1);
        assertThat(collection1.getFieldValue("value_s")).isEqualTo("value1");


        client.close();


        context.close();


    }

}
