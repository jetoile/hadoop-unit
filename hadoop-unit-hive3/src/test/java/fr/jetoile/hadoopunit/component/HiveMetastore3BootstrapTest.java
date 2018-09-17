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


import fr.jetoile.hadoopunit.Component;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.HadoopBootstrap;
import fr.jetoile.hadoopunit.Utils;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import fr.jetoile.hadoopunit.exception.NotFoundServiceException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.thrift.TException;
import org.fest.assertions.Assertions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.fest.assertions.Assertions.assertThat;

@Ignore
public class HiveMetastore3BootstrapTest {

    private static Configuration configuration;


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
    public void hiveMetastoreShouldStart() throws InterruptedException, NotFoundServiceException {
//        Assertions.assertThat(Utils.available("127.0.0.1", 20102)).isFalse();


        // Create a table and display it back
        try {
            HiveMetaStoreClient hiveClient = new HiveMetaStoreClient((HiveConf) ((BootstrapHadoop)HadoopBootstrap.INSTANCE.getService(Component.HIVEMETA_3)).getConfiguration());

            hiveClient.dropTable(configuration.getString(HadoopUnitConfig.HIVE_TEST_DATABASE_NAME_KEY),
                    configuration.getString(HadoopUnitConfig.HIVE_TEST_TABLE_NAME_KEY),
                    true,
                    true);

            // Define the cols
            List<FieldSchema> cols = new ArrayList<>();
            cols.add(new FieldSchema("id", serdeConstants.INT_TYPE_NAME, ""));
            cols.add(new FieldSchema("msg", serdeConstants.STRING_TYPE_NAME, ""));

            // Values for the StorageDescriptor
            String location = new File(configuration.getString(HadoopUnitConfig.HIVE_TEST_TABLE_NAME_KEY)).getAbsolutePath();
            String inputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
            String outputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";
            int numBuckets = 16;
            Map<String, String> orcProps = new HashMap<>();
            orcProps.put("orc.compress", "NONE");
            SerDeInfo serDeInfo = new SerDeInfo(OrcSerde.class.getSimpleName(), OrcSerde.class.getName(), orcProps);
            List<String> bucketCols = new ArrayList<>();
            bucketCols.add("id");

            // Build the StorageDescriptor
            StorageDescriptor sd = new StorageDescriptor();
            sd.setCols(cols);
            sd.setLocation(location);
            sd.setInputFormat(inputFormat);
            sd.setOutputFormat(outputFormat);
            sd.setNumBuckets(numBuckets);
            sd.setSerdeInfo(serDeInfo);
            sd.setBucketCols(bucketCols);
            sd.setSortCols(new ArrayList<>());
            sd.setParameters(new HashMap<>());

            // Define the table
            Table tbl = new Table();
            tbl.setDbName(configuration.getString(HadoopUnitConfig.HIVE_TEST_DATABASE_NAME_KEY));
            tbl.setTableName(configuration.getString(HadoopUnitConfig.HIVE_TEST_TABLE_NAME_KEY));
            tbl.setSd(sd);
            tbl.setOwner(System.getProperty("user.name"));
            tbl.setParameters(new HashMap<>());
            tbl.setViewOriginalText("");
            tbl.setViewExpandedText("");
            tbl.setTableType(TableType.EXTERNAL_TABLE.name());
            List<FieldSchema> partitions = new ArrayList<FieldSchema>();
            partitions.add(new FieldSchema("dt", serdeConstants.STRING_TYPE_NAME, ""));
            tbl.setPartitionKeys(partitions);

            // Create the table
            hiveClient.createTable(tbl);

            // Describe the table
            Table createdTable = hiveClient.getTable(
                    configuration.getString(HadoopUnitConfig.HIVE_TEST_DATABASE_NAME_KEY),
                    configuration.getString(HadoopUnitConfig.HIVE_TEST_TABLE_NAME_KEY));
            assertThat(createdTable.toString()).contains(configuration.getString(HadoopUnitConfig.HIVE_TEST_TABLE_NAME_KEY));

        } catch (MetaException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
    }
}
