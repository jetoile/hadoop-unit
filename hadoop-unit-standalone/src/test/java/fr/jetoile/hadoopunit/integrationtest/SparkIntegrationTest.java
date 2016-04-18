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

import com.ninja_squad.dbsetup.Operations;
import com.ninja_squad.dbsetup.operation.Operation;
import fr.jetoile.hadoopunit.test.hdfs.HdfsUtils;
import fr.jetoile.hadoopunit.test.hive.HiveConnectionUtils;
import fr.jetoile.hadoopunit.test.hive.HiveSetup;
import fr.jetoile.hadoopunit.Component;
import fr.jetoile.hadoopunit.Config;
import fr.jetoile.hadoopunit.HadoopBootstrap;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import fr.jetoile.hadoopunit.exception.NotFoundServiceException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.ninja_squad.dbsetup.Operations.sequenceOf;
import static com.ninja_squad.dbsetup.Operations.sql;
import static org.fest.assertions.Assertions.assertThat;

@Ignore
public class SparkIntegrationTest {
    static private Logger LOGGER = LoggerFactory.getLogger(SparkIntegrationTest.class);


    private static Configuration configuration;

    public static Operation CREATE_TABLES = null;
    public static Operation DROP_TABLES = null;


    @BeforeClass
    public static void setUp() throws BootstrapException, SQLException, ClassNotFoundException, NotFoundServiceException {
        try {
            configuration = new PropertiesConfiguration(Config.DEFAULT_PROPS_FILE);
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }

        HadoopBootstrap.INSTANCE
                .add(Component.ZOOKEEPER)
                .add(Component.HDFS)
                .add(Component.HIVEMETA)
                .add(Component.HIVESERVER2)
                .startAll();

        CREATE_TABLES =
                sequenceOf(sql("CREATE EXTERNAL TABLE IF NOT EXISTS default.test(id INT, value STRING) " +
                                " ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'" +
                                " STORED AS TEXTFILE" +
                                " LOCATION 'hdfs://localhost:" + configuration.getInt(Config.HDFS_NAMENODE_PORT_KEY) + "/khanh/test'"),
                        sql("CREATE EXTERNAL TABLE IF NOT EXISTS default.test_parquet(id INT, value STRING) " +
                                " ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'" +
                                " STORED AS PARQUET" +
                                " LOCATION 'hdfs://localhost:" + configuration.getInt(Config.HDFS_NAMENODE_PORT_KEY) + "/khanh/test_parquet'"));

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
        fileSystem.copyFromLocalFile(new Path(SparkIntegrationTest.class.getClassLoader().getResource("test.csv").toURI()), new Path("/khanh/test/test.csv"));

        new HiveSetup(HiveConnectionUtils.INSTANCE.getDestination(), Operations.sequenceOf(CREATE_TABLES)).launch();
    }

    @After
    public void clean() throws IOException {
        new HiveSetup(HiveConnectionUtils.INSTANCE.getDestination(), Operations.sequenceOf(DROP_TABLES)).launch();

        FileSystem fileSystem = HdfsUtils.INSTANCE.getFileSystem();
        fileSystem.delete(new Path("/khanh"), true);


    }

    @Test
    public void upload_file_into_hdfs_and_map_hive_should_success() throws SQLException {

        Statement stmt = HiveConnectionUtils.INSTANCE.getConnection().createStatement();

        String select = "SELECT * FROM default.test";
        ResultSet resultSet = stmt.executeQuery(select);
        while (resultSet.next()) {
            int id = resultSet.getInt(1);
            String value = resultSet.getString(2);
            assertThat(id).isNotNull();
            assertThat(value).isNotNull();
        }
    }

    @Test
    public void spark_should_read_hive() throws SQLException {
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("test");

        JavaSparkContext context = new JavaSparkContext(conf);

        //read hive-site from classpath
        HiveContext sqlContext = new HiveContext(context.sc());

        DataFrame sql = sqlContext.sql("SELECT * FROM default.test");
//        sql.printSchema();
        Row[] rows = sql.collect();

        for (int i = 1; i < 4; i++) {
            Row row = rows[i - 1];
            assertThat("value" + i).isEqualToIgnoringCase(row.getString(1));
            assertThat(i).isEqualTo(row.getInt(0));
        }

        context.close();
    }

    @Test
    public void spark_should_create_a_parquet_file() throws SQLException, IOException {
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("test");

        JavaSparkContext context = new JavaSparkContext(conf);

        //read hive-site from classpath
        HiveContext sqlContext = new HiveContext(context.sc());

        DataFrame sql = sqlContext.sql("SELECT * FROM default.test");
        sql.write().parquet("hdfs://localhost:" + configuration.getInt(Config.HDFS_NAMENODE_PORT_KEY) + "/khanh/test_parquet/file.parquet");

        FileSystem fileSystem = HdfsUtils.INSTANCE.getFileSystem();
        assertThat(fileSystem.exists(new Path("hdfs://localhost:" + configuration.getInt(Config.HDFS_NAMENODE_PORT_KEY) + "/khanh/test_parquet/file.parquet"))).isTrue();

        context.close();
    }


    @Test
    public void spark_should_read_parquet_file() throws IOException {
        //given
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("test");

        JavaSparkContext context = new JavaSparkContext(conf);

        //read hive-site from classpath
        HiveContext hiveContext = new HiveContext(context.sc());

        DataFrame sql = hiveContext.sql("SELECT * FROM default.test");
        sql.write().parquet("hdfs://localhost:" + configuration.getInt(Config.HDFS_NAMENODE_PORT_KEY) + "/khanh/test_parquet/file.parquet");

        FileSystem fileSystem = HdfsUtils.INSTANCE.getFileSystem();
        assertThat(fileSystem.exists(new Path("hdfs://localhost:" + configuration.getInt(Config.HDFS_NAMENODE_PORT_KEY) + "/khanh/test_parquet/file.parquet"))).isTrue();

        context.close();

        //when
        context = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(context);

        DataFrame file = sqlContext.read().parquet("hdfs://localhost:" + configuration.getInt(Config.HDFS_NAMENODE_PORT_KEY) + "/khanh/test_parquet/file.parquet");
        DataFrame select = file.select("id", "value");
        Row[] rows = select.collect();

        //then
        for (int i = 1; i < 4; i++) {
            Row row = rows[i - 1];
            assertThat("value" + i).isEqualToIgnoringCase(row.getString(1));
            assertThat(i).isEqualTo(row.getInt(0));
        }

        context.close();


    }

}
