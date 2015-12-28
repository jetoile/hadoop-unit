package fr.jetoile.sample.component;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.impl.HiveLocalMetaStore;
import com.github.sakserv.minicluster.impl.HiveLocalServer2;
import com.github.sakserv.minicluster.util.FileUtils;
import fr.jetoile.sample.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public enum HiveServer2Bootstrap implements Bootstrap {
    INSTANCE;

    final private Logger LOGGER = LoggerFactory.getLogger(HiveServer2Bootstrap.class);

    private HiveLocalServer2 hiveLocalServer2;

    private Configuration configuration;
    private String host;
    private int port;
    private String derbyDirectory;
    private String scratchDirectory;
    private String warehouseDirectory;
    private String zookeeperConnectionString;
    private String hostMetastore;
    private int portMetastore;


    HiveServer2Bootstrap() {
        if (hiveLocalServer2 == null) {
            try {
                loadConfig();
            } catch (BootstrapException e) {
                LOGGER.error("unable to load configuration", e);
            }
            init();
            build();
        }
    }

    private void loadConfig() throws BootstrapException {
        try {
            configuration = new PropertiesConfiguration("default.properties");
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }
        host = configuration.getString(ConfigVars.HIVE_SERVER2_HOSTNAME_KEY);
        port = configuration.getInt(ConfigVars.HIVE_SERVER2_PORT_KEY);
        hostMetastore = configuration.getString(ConfigVars.HIVE_METASTORE_HOSTNAME_KEY);
        portMetastore = configuration.getInt(ConfigVars.HIVE_METASTORE_PORT_KEY);
        derbyDirectory = configuration.getString(ConfigVars.HIVE_METASTORE_DERBY_DB_DIR_KEY);
        scratchDirectory = configuration.getString(ConfigVars.HIVE_SCRATCH_DIR_KEY);
        warehouseDirectory = configuration.getString(ConfigVars.HIVE_WAREHOUSE_DIR_KEY);
        zookeeperConnectionString = configuration.getString(ConfigVars.ZOOKEEPER_HOST_KEY) + ":" + configuration.getInt(ConfigVars.ZOOKEEPER_PORT_KEY);

    }


    private void init() {
        Path path2 = Paths.get(scratchDirectory);
        Path path3 = Paths.get(warehouseDirectory);
        try {
            Files.createDirectories(path2);
            Files.createDirectories(path3);
        } catch (IOException e) {
            LOGGER.error("unable to create mandatory directory", e);
        }

    }

    private void cleanup() {
            FileUtils.deleteFolder(derbyDirectory);
            FileUtils.deleteFolder(derbyDirectory.substring(derbyDirectory.lastIndexOf("/")+1));

    }

    private void build() {
        hiveLocalServer2 = new HiveLocalServer2.Builder()
                .setHiveMetastoreDerbyDbDir(derbyDirectory)
                .setHiveServer2Hostname(host)
                .setHiveServer2Port(port)
                .setHiveMetastoreHostname(hostMetastore)
                .setHiveMetastorePort(portMetastore)
                .setHiveWarehouseDir(warehouseDirectory)
                .setHiveScratchDir(scratchDirectory)
                .setHiveConf(buildHiveConf())
                .setZookeeperConnectionString(zookeeperConnectionString)
                .build();

    }

    private HiveConf buildHiveConf() {
        HiveConf hiveConf = new HiveConf();
        return hiveConf;
    }

    @Override
    public Bootstrap start() {
        try {
            hiveLocalServer2.start();
        } catch (Exception e) {
            LOGGER.error("unable to start hiveserver2", e);
        }
        return this;
    }

    @Override
    public Bootstrap stop() {
        try {
            hiveLocalServer2.stop(true);
        } catch (Exception e) {
            LOGGER.error("unable to stop hiveserver2", e);
        }
        cleanup();
        return this;
    }
}
