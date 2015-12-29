package fr.jetoile.sample.component;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.impl.HiveLocalMetaStore;
import com.github.sakserv.minicluster.util.FileUtils;
import com.github.sakserv.minicluster.util.WindowsLibsUtils;
import fr.jetoile.sample.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum HiveMetastoreBootstrap implements Bootstrap {
    INSTANCE;

    final private Logger LOGGER = LoggerFactory.getLogger(HiveMetastoreBootstrap.class);

    private HiveLocalMetaStore hiveLocalMetaStore;

    private Configuration configuration;
    private String host;
    private int port;
    private String derbyDirectory;
    private String scratchDirectory;
    private String warehouseDirectory;

    HiveMetastoreBootstrap() {
        if (hiveLocalMetaStore == null) {
            try {
                loadConfig();
            } catch (BootstrapException e) {
                LOGGER.error("unable to load configuration", e);
            }
            build();
        }
    }

    private void loadConfig() throws BootstrapException {
        try {
            configuration = new PropertiesConfiguration("default.properties");
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }
        host = configuration.getString(ConfigVars.HIVE_METASTORE_HOSTNAME_KEY);
        port = configuration.getInt(ConfigVars.HIVE_METASTORE_PORT_KEY);
        derbyDirectory = configuration.getString(ConfigVars.HIVE_METASTORE_DERBY_DB_DIR_KEY);
        scratchDirectory = configuration.getString(ConfigVars.HIVE_SCRATCH_DIR_KEY);
        warehouseDirectory = configuration.getString(ConfigVars.HIVE_WAREHOUSE_DIR_KEY);

    }


    private void cleanup() {
            FileUtils.deleteFolder(derbyDirectory);
            FileUtils.deleteFolder(derbyDirectory.substring(derbyDirectory.lastIndexOf("/")+1));

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
        try {
            hiveLocalMetaStore.start();
        } catch (Exception e) {
            LOGGER.error("unable to start hivemetastore", e);
        }
        return this;
    }

    @Override
    public Bootstrap stop() {
        try {
            hiveLocalMetaStore.stop(true);
        } catch (Exception e) {
            LOGGER.error("unable to stop hivemetastore", e);
        }
        cleanup();
        return this;
    }

    @Override
    public org.apache.hadoop.conf.Configuration getConfiguration() {
        return hiveLocalMetaStore.getHiveConf();
    }


}
