/*
 * Copyright (c) 2011 Khanh Tuong Maudoux <kmx.petals@gmail.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package fr.jetoile.sample.integrationtest;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.lucidworks.spark.SchemaPreservingSolrRDD;
import com.lucidworks.spark.SolrSupport;
import com.ninja_squad.dbsetup.Operations;
import com.ninja_squad.dbsetup.operation.Operation;
import fr.jetoile.hadoop.test.hdfs.HdfsUtils;
import fr.jetoile.hadoop.test.hive.HiveConnectionUtils;
import fr.jetoile.hadoop.test.hive.HiveSetup;
import fr.jetoile.sample.Component;
import fr.jetoile.sample.HadoopBootstrap;
import fr.jetoile.sample.component.SolrCloudBootstrap;
import fr.jetoile.sample.exception.BootstrapException;
import org.apache.commons.collections.map.HashedMap;
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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.HashMap;

import static com.ninja_squad.dbsetup.Operations.sequenceOf;
import static com.ninja_squad.dbsetup.Operations.sql;
import static junit.framework.TestCase.assertNotNull;
import static org.fest.assertions.Assertions.assertThat;

public class SparkSolrIntegrationTest {
    static private Logger LOGGER = LoggerFactory.getLogger(SparkSolrIntegrationTest.class);


    private static HadoopBootstrap hadoopBootstrap;
    private static Configuration configuration;

    public static Operation CREATE_TABLES = null;
    public static Operation DROP_TABLES = null;


    @BeforeClass
    public static void setUp() throws BootstrapException, SQLException, ClassNotFoundException {
        try {
            configuration = new PropertiesConfiguration("default.properties");
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }

        hadoopBootstrap = new HadoopBootstrap(Component.ZOOKEEPER, Component.HDFS, Component.HIVEMETA, Component.HIVESERVER2, Component.SOLRCLOUD);
        hadoopBootstrap.startAll();

        CREATE_TABLES =
                sequenceOf(sql("CREATE EXTERNAL TABLE IF NOT EXISTS default.test(id INT, value STRING) " +
                                " ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'" +
                                " STORED AS TEXTFILE" +
                                " LOCATION 'hdfs://localhost:" + configuration.getInt(ConfigVars.HDFS_NAMENODE_PORT_KEY) + "/khanh/test'"),
                        sql("CREATE EXTERNAL TABLE IF NOT EXISTS default.test_parquet(id INT, value STRING) " +
                                " ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'" +
                                " STORED AS PARQUET" +
                                " LOCATION 'hdfs://localhost:" + configuration.getInt(ConfigVars.HDFS_NAMENODE_PORT_KEY) + "/khanh/test_parquet'"));

        DROP_TABLES =
                sequenceOf(sql("DROP TABLE IF EXISTS default.test"),
                        sql("DROP TABLE IF EXISTS default.test_parquet"));
    }

    @AfterClass
    public static void tearDown() {
        hadoopBootstrap.stopAll();
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
        sql.write().parquet("hdfs://localhost:" + configuration.getInt(ConfigVars.HDFS_NAMENODE_PORT_KEY) + "/khanh/test_parquet/file.parquet");

        FileSystem fileSystem = HdfsUtils.INSTANCE.getFileSystem();
        assertThat(fileSystem.exists(new Path("hdfs://localhost:" + configuration.getInt(ConfigVars.HDFS_NAMENODE_PORT_KEY) + "/khanh/test_parquet/file.parquet"))).isTrue();

        context.close();

        //when
        context = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(context);

        DataFrame file = sqlContext.read().parquet("hdfs://localhost:" + configuration.getInt(ConfigVars.HDFS_NAMENODE_PORT_KEY) + "/khanh/test_parquet/file.parquet");
        DataFrame select = file.select("id", "value");

        JavaRDD<SolrInputDocument> solrInputDocument = select.toJavaRDD().map(r -> {
            SolrInputDocument solr = new SolrInputDocument();
            solr.setField("id", r.getInt(0));
            solr.setField("value_s", r.getString(1));
            return solr;
        });

        String collectionName = configuration.getString(SolrCloudBootstrap.SOLR_COLLECTION_NAME);
        String zkHostString = configuration.getString(ConfigVars.ZOOKEEPER_HOST_KEY) + ":" + configuration.getInt(ConfigVars.ZOOKEEPER_PORT_KEY);
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
