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
import fr.jetoile.hadoopunit.ComponentMetadata;
import fr.jetoile.hadoopunit.HadoopUtils;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Map;

public class HiveMetastore3Bootstrap implements BootstrapHadoop3 {
    final static private Logger LOGGER = LoggerFactory.getLogger(HiveMetastore3Bootstrap.class);

    private HiveLocalMetaStore hiveLocalMetaStore;

    private State state = State.STOPPED;

    private Configuration configuration;
    private String host;
    private int port;
    private String derbyDirectory;
    private String scratchDirectory;
    private String warehouseDirectory;

    public HiveMetastore3Bootstrap() {
        if (hiveLocalMetaStore == null) {
            try {
                configuration = HadoopUtils.INSTANCE.loadConfigFile(null);
                loadConfig();
            } catch (BootstrapException e) {
                LOGGER.error("unable to load configuration", e);
            }
        }
    }

    public HiveMetastore3Bootstrap(URL url) {
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
    public ComponentMetadata getMetadata() {
        return new HiveMetastore3Metadata();
    }

    @Override
    public String getProperties() {
        return "\n \t\t\t port:" + port;
    }

    private void loadConfig() throws BootstrapException {
        host = configuration.getString(Hive3Config.HIVE3_METASTORE_HOSTNAME_KEY);
        port = configuration.getInt(Hive3Config.HIVE3_METASTORE_PORT_KEY);
        derbyDirectory = configuration.getString(Hive3Config.HIVE3_METASTORE_DERBY_DB_DIR_KEY);
        scratchDirectory = getTmpDirPath(configuration, Hive3Config.HIVE3_SCRATCH_DIR_KEY);
        warehouseDirectory = configuration.getString(Hive3Config.HIVE3_WAREHOUSE_DIR_KEY);
    }

    @Override
    public void loadConfig(Map<String, String> configs) {
        if (StringUtils.isNotEmpty(configs.get(Hive3Config.HIVE3_METASTORE_HOSTNAME_KEY))) {
            host = configs.get(Hive3Config.HIVE3_METASTORE_HOSTNAME_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(Hive3Config.HIVE3_METASTORE_PORT_KEY))) {
            port = Integer.parseInt(configs.get(Hive3Config.HIVE3_METASTORE_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(Hive3Config.HIVE3_METASTORE_DERBY_DB_DIR_KEY))) {
            derbyDirectory = configs.get(Hive3Config.HIVE3_METASTORE_DERBY_DB_DIR_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(Hive3Config.HIVE3_SCRATCH_DIR_KEY))) {
            scratchDirectory = getTmpDirPath(configs, Hive3Config.HIVE3_SCRATCH_DIR_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(Hive3Config.HIVE3_WAREHOUSE_DIR_KEY))) {
            warehouseDirectory = configs.get(Hive3Config.HIVE3_WAREHOUSE_DIR_KEY);
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
        hiveConf.set("fs.defaultFS", "hdfs://" + configuration.getString(Hdfs3Config.HDFS3_NAMENODE_HOST_CLIENT_KEY) + ":" + configuration.getInt(Hdfs3Config.HDFS3_NAMENODE_PORT_KEY));
//        hiveConf.set(HiveConf.ConfVars.HIVE_TXN_MANAGER.varname, "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
//        hiveConf.set(HiveConf.ConfVars.HIVE_COMPACTOR_INITIATOR_ON.varname, "true");
//        hiveConf.set(HiveConf.ConfVars.HIVE_COMPACTOR_WORKER_THREADS.varname, "5");
//        hiveConf.set("hive.root.logger", "DEBUG,console");
//        hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
//        hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
//        hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
//        System.setProperty(HiveConf.ConfVars.PREEXECHOOKS.varname, " ");
//        System.setProperty(HiveConf.ConfVars.POSTEXECHOOKS.varname, " ");

        hiveConf.set("metastore.metastore.event.db.notification.api.auth", "false");

        hiveConf.set("datanucleus.schema.autoCreateAll", "true");
        hiveConf.set("metastore.client.capability.check", "false");

        return hiveConf;
    }

    @Override
    public Bootstrap start() {
        if (state == State.STOPPED) {
            state = State.STARTING;
            LOGGER.info("{} is starting", this.getClass().getName());
            build();

            try {
                hiveLocalMetaStore.start();
            } catch (Exception e) {
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
