package fr.jetoile.sample.component;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.impl.HbaseLocalCluster;
import com.github.sakserv.minicluster.util.FileUtils;
import fr.jetoile.sample.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum HBaseBootstrap implements Bootstrap {
    INSTANCE;

    final private Logger LOGGER = LoggerFactory.getLogger(HBaseBootstrap.class);

    private HbaseLocalCluster hbaseLocalCluster;

    private Configuration configuration;
    private int port;
    private int infoPort;
    private int nbRegionServer;
    private String rootDirectory;
    private int zookeeperPort;
    private String zookeeperZnodeParent;
    private boolean enableWalReplication;
    private String zookeeperConnectionString;


    HBaseBootstrap() {
        if (hbaseLocalCluster == null) {
            try {
                loadConfig();
            } catch (BootstrapException e) {
                LOGGER.error("unable to load configuration", e);
            }
            init();
            build();
        }
    }

    private void init() {

    }

    private void build() {
        hbaseLocalCluster = new HbaseLocalCluster.Builder()
                .setHbaseMasterPort(port)
                .setHbaseMasterInfoPort(infoPort)
                .setNumRegionServers(nbRegionServer)
                .setHbaseRootDir(rootDirectory)
                .setZookeeperPort(zookeeperPort)
                .setZookeeperConnectionString(zookeeperConnectionString)
                .setZookeeperZnodeParent(zookeeperZnodeParent)
                .setHbaseWalReplicationEnabled(enableWalReplication)
                .setHbaseConfiguration(new org.apache.hadoop.conf.Configuration())
                .build();
    }

    private void loadConfig() throws BootstrapException {
        try {
            configuration = new PropertiesConfiguration("default.properties");
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }

        port = configuration.getInt(ConfigVars.HBASE_MASTER_PORT_KEY);
        infoPort = configuration.getInt(ConfigVars.HBASE_MASTER_INFO_PORT_KEY);
        nbRegionServer = configuration.getInt(ConfigVars.HBASE_NUM_REGION_SERVERS_KEY);
        rootDirectory = configuration.getString(ConfigVars.HBASE_ROOT_DIR_KEY);
        zookeeperConnectionString = configuration.getString(ConfigVars.ZOOKEEPER_HOST_KEY) + ":" + configuration.getInt(ConfigVars.ZOOKEEPER_PORT_KEY);
        zookeeperPort = configuration.getInt(ConfigVars.ZOOKEEPER_PORT_KEY);
        zookeeperZnodeParent = configuration.getString(ConfigVars.HBASE_ZNODE_PARENT_KEY);
        enableWalReplication = configuration.getBoolean(ConfigVars.HBASE_WAL_REPLICATION_ENABLED_KEY);

    }

    private void cleanup() {
        FileUtils.deleteFolder(rootDirectory);
        FileUtils.deleteFolder(rootDirectory.substring(rootDirectory.lastIndexOf("/")+1));

    }

    @Override
    public Bootstrap start() {
        try {
            hbaseLocalCluster.start();
        } catch (Exception e) {
            LOGGER.error("unable to start hbase", e);
        }
        return this;
    }

    @Override
    public Bootstrap stop() {
        try {
            hbaseLocalCluster.stop(true);
        } catch (Exception e) {
            LOGGER.error("unable to stop hbase", e);
        }
        cleanup();
        return this;

    }

    @Override
    public org.apache.hadoop.conf.Configuration getConfiguration() {
        return hbaseLocalCluster.getHbaseConfiguration();
    }


}
