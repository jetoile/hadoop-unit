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

import com.github.sakserv.minicluster.impl.HiveLocalMetaStore;
import com.github.sakserv.minicluster.util.FileUtils;
import com.github.sakserv.minicluster.util.WindowsLibsUtils;
import fr.jetoile.hadoopunit.Component;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.HadoopUtils;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge23;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Map;

public class HiveMetastore2Bootstrap implements BootstrapHadoop {
    final public static String NAME = Component.HIVEMETA_2.name();

    final static private Logger LOGGER = LoggerFactory.getLogger(HiveMetastore2Bootstrap.class);

    private HiveLocalMetaStore hiveLocalMetaStore;

    private State state = State.STOPPED;

    private Configuration configuration;
    private String host;
    private int port;
    private String derbyDirectory;
    private String scratchDirectory;
    private String warehouseDirectory;

    public HiveMetastore2Bootstrap() {
        if (hiveLocalMetaStore == null) {
            try {
                configuration = HadoopUtils.INSTANCE.loadConfigFile(null);
                loadConfig();
            } catch (BootstrapException e) {
                LOGGER.error("unable to load configuration", e);
            }
        }
    }

    public HiveMetastore2Bootstrap(URL url) {
        if (hiveLocalMetaStore == null) {
            try {
                configuration = HadoopUtils.INSTANCE.loadConfigFile(url);
                loadConfig();
            } catch (BootstrapException e) {
                LOGGER.error("unable to load configuration", e);
            }
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getProperties() {
        return "\n \t\t\t port:" + port;
    }

    private void loadConfig() throws BootstrapException {
        host = configuration.getString(HadoopUnitConfig.HIVE_METASTORE_HOSTNAME_KEY);
        port = configuration.getInt(HadoopUnitConfig.HIVE_METASTORE_PORT_KEY);
        derbyDirectory = configuration.getString(HadoopUnitConfig.HIVE_METASTORE_DERBY_DB_DIR_KEY);
        scratchDirectory = configuration.getString(HadoopUnitConfig.HIVE_SCRATCH_DIR_KEY);
        warehouseDirectory = configuration.getString(HadoopUnitConfig.HIVE_WAREHOUSE_DIR_KEY);
    }

    @Override
    public void loadConfig(Map<String, String> configs) {
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.HIVE_METASTORE_HOSTNAME_KEY))) {
            host = configs.get(HadoopUnitConfig.HIVE_METASTORE_HOSTNAME_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.HIVE_METASTORE_PORT_KEY))) {
            port = Integer.parseInt(configs.get(HadoopUnitConfig.HIVE_METASTORE_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.HIVE_METASTORE_DERBY_DB_DIR_KEY))) {
            derbyDirectory = configs.get(HadoopUnitConfig.HIVE_METASTORE_DERBY_DB_DIR_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.HIVE_SCRATCH_DIR_KEY))) {
            scratchDirectory = configs.get(HadoopUnitConfig.HIVE_SCRATCH_DIR_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.HIVE_WAREHOUSE_DIR_KEY))) {
            warehouseDirectory = configs.get(HadoopUnitConfig.HIVE_WAREHOUSE_DIR_KEY);
        }
    }

    private void cleanup() {
        FileUtils.deleteFolder(derbyDirectory);
        FileUtils.deleteFolder(derbyDirectory.substring(derbyDirectory.lastIndexOf("/") + 1));

    }

    private void build() {
        hiveLocalMetaStore = new HiveLocalMetaStore.Builder()
                .setHiveMetastoreDerbyDbDir(derbyDirectory)
                .setHiveMetastoreHostname(host)
                .setHiveMetastorePort(port)
                .setHiveWarehouseDir(warehouseDirectory)
                .setHiveScratchDir(scratchDirectory)
                .setHiveConf(buildHiveConf())
                .build();
    }

    private HiveConf buildHiveConf() {
        // Handle Windows
        WindowsLibsUtils.setHadoopHome();

        HiveConf hiveConf = new HiveConf();
        hiveConf.set("fs.defaultFS", "hdfs://" + configuration.getString(HadoopUnitConfig.HDFS_NAMENODE_HOST_KEY) + ":" + configuration.getInt(HadoopUnitConfig.HDFS_NAMENODE_PORT_KEY));
        hiveConf.set("datanucleus.autoCreateSchema", "true");
        hiveConf.set("hive.metastore.schema.verification", "false");

//        hiveConf.set(HiveConf.ConfVars.HIVE_TXN_MANAGER.varname, "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
//        hiveConf.set(HiveConf.ConfVars.HIVE_COMPACTOR_INITIATOR_ON.varname, "true");
//        hiveConf.set(HiveConf.ConfVars.HIVE_COMPACTOR_WORKER_THREADS.varname, "5");
//        hiveConf.set("hive.root.logger", "DEBUG,console");
//        hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
//        hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
//        hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
//        System.setProperty(HiveConf.ConfVars.PREEXECHOOKS.varname, " ");
//        System.setProperty(HiveConf.ConfVars.POSTEXECHOOKS.varname, " ");

        return hiveConf;
    }

    @Override
    public Bootstrap start() {
        if (state == State.STOPPED) {
            state = State.STARTING;
            LOGGER.info("{} is starting", this.getClass().getName());
            build();

            try {
                HiveMetaStore.startMetaStore(port, new HadoopThriftAuthBridge23(), buildHiveConf());
            } catch (Throwable e) {
                LOGGER.error("unable to add hivemetastore", e);
            }
            state = State.STARTED;
            LOGGER.info("{} is started", this.getClass().getName());
        }
        return this;
    }

    @Override
    public Bootstrap stop() {
        if (state == State.STARTED) {
            state = State.STOPPING;
            LOGGER.info("{} is stopping", this.getClass().getName());
            try {
                hiveLocalMetaStore.stop(true);
            } catch (Exception e) {
                LOGGER.error("unable to stop hivemetastore", e);
            }
            cleanup();
            state = State.STOPPED;
            LOGGER.info("{} is stopped", this.getClass().getName());
        }
        return this;
    }

    @Override
    public org.apache.hadoop.conf.Configuration getConfiguration() {
        return hiveLocalMetaStore.getHiveConf();
    }

}
