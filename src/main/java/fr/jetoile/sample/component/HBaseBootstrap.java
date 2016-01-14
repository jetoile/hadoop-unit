package fr.jetoile.sample.component;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.impl.HbaseLocalCluster;
import com.github.sakserv.minicluster.util.FileUtils;
import com.google.common.collect.Maps;
import fr.jetoile.sample.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.ipc.PhoenixRpcSchedulerFactory;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.ipc.controller.ServerRpcControllerFactory;
import org.apache.hadoop.hbase.regionserver.IndexHalfStoreFileReaderGenerator;
import org.apache.hadoop.hbase.regionserver.LocalIndexSplitter;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.phoenix.coprocessor.*;
import org.apache.phoenix.hbase.index.Indexer;
import org.apache.phoenix.hbase.index.covered.CoveredColumnsIndexBuilder;
import org.apache.phoenix.index.PhoenixIndexBuilder;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public enum HBaseBootstrap implements Bootstrap {
    INSTANCE;

    final private Logger LOGGER = LoggerFactory.getLogger(HBaseBootstrap.class);

    private HbaseLocalCluster hbaseLocalCluster;

    private State state = State.STOPPED;

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
        }
    }

    private void init() {

    }

    private void build() {
        org.apache.hadoop.conf.Configuration hbaseConfiguration = new org.apache.hadoop.conf.Configuration();
        hbaseConfiguration.setBoolean("hbase.table.sanity.checks", false);
        hbaseConfiguration.set("fs.default.name", "hdfs://127.0.0.1:" + configuration.getString(ConfigVars.HDFS_NAMENODE_PORT_KEY));

//        QueryServices services = new PhoenixTestDriver().getQueryServices();
//        for (Map.Entry<String,String> entry : services.getProps()) {
//            hbaseConfiguration.set(entry.getKey(), entry.getValue());
//        }
        //no point doing sanity checks when running tests.
        hbaseConfiguration.setBoolean("hbase.table.sanity.checks", false);
        // set the server rpc controller and rpc scheduler factory, used to configure the cluster
        String DEFAULT_SERVER_RPC_CONTROLLER_FACTORY = ServerRpcControllerFactory.class.getName();
        String DEFAULT_RPC_SCHEDULER_FACTORY = PhoenixRpcSchedulerFactory.class.getName();
        hbaseConfiguration.set(RpcControllerFactory.CUSTOM_CONTROLLER_CONF_KEY, DEFAULT_SERVER_RPC_CONTROLLER_FACTORY);
        hbaseConfiguration.set(RSRpcServices.REGION_SERVER_RPC_SCHEDULER_FACTORY_CLASS, DEFAULT_RPC_SCHEDULER_FACTORY);

        hbaseConfiguration.set("hbase.coprocessor.enabled", "true");
        hbaseConfiguration.set("hbase.coprocessor.user.enabled", "true");
        hbaseConfiguration.set("phoenix.view.allowNewColumnFamily", "true");
        hbaseConfiguration.set("phoenix.query.dateFormat", "yyyy-MM-dd HH:mm:ss.SSS");
        hbaseConfiguration.set("index.builder", "org.apache.phoenix.index.PhoenixIndexBuilder");
        hbaseConfiguration.set(CoveredColumnsIndexBuilder.CODEC_CLASS_NAME_KEY, PhoenixIndexCodec.class.getName());

        hbaseConfiguration.setStrings(
                CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
                ServerCachingEndpointImpl.class.getName(),
                GroupedAggregateRegionObserver.class.getName(),
                Indexer.class.getName(),
                MetaDataRegionObserver.class.getName(),
                ScanRegionObserver.class.getName(),
                SequenceRegionObserver.class.getName(),
                UngroupedAggregateRegionObserver.class.getName(),
                IndexHalfStoreFileReaderGenerator.class.getName(),
                LocalIndexSplitter.class.getName(),
                MetaDataEndpointImpl.class.getName()

                );

//        Path parentdir = fs.getHomeDirectory();
//        hbaseConfiguration.set(HConstants.HBASE_DIR, parentdir.toString());
//        fs.mkdirs(parentdir);
//        FSUtils.setVersion(fs, parentdir);


        hbaseLocalCluster = new HbaseLocalCluster.Builder()
                .setHbaseMasterPort(port)
                .setHbaseMasterInfoPort(infoPort)
                .setNumRegionServers(nbRegionServer)
                .setHbaseRootDir(rootDirectory)
                .setZookeeperPort(zookeeperPort)
                .setZookeeperConnectionString(zookeeperConnectionString)
                .setZookeeperZnodeParent(zookeeperZnodeParent)
                .setHbaseWalReplicationEnabled(enableWalReplication)
                .setHbaseConfiguration(hbaseConfiguration)
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
        FileUtils.deleteFolder(rootDirectory.substring(rootDirectory.lastIndexOf("/") + 1));

    }

//    public static PhoenixTestDriver initAndRegisterDriver(String url, ReadOnlyProps props) throws Exception {
//        PhoenixTestDriver newDriver = new PhoenixTestDriver(props);
//        DriverManager.registerDriver(newDriver);
//        Driver oldDriver = DriverManager.getDriver(url);
//        if (oldDriver != newDriver) {
//            destroyDriver(oldDriver);
//        }
//        Connection conn = newDriver.connect(url, PropertiesUtil.deepCopy(TEST_PROPERTIES));
//        conn.close();
//        return newDriver;
//    }

    @Override
    public Bootstrap start() {
        if (state == State.STOPPED) {
            state = State.STARTING;
            LOGGER.info("{} is starting", this.getClass().getName());
            init();
            build();
            try {
                hbaseLocalCluster.start();
            } catch (Exception e) {
                LOGGER.error("unable to start hbase", e);
            }

            try {
                String zkConfig = configuration.getString(ConfigVars.ZOOKEEPER_HOST_KEY) + ":" + configuration.getInt(ConfigVars.ZOOKEEPER_PORT_KEY);

                Properties prop = new Properties();
                prop.setProperty("hbase.zookeeper.quorum", configuration.getString(ConfigVars.ZOOKEEPER_HOST_KEY));
                prop.setProperty("hbase.zookeeper.property.clientPort", configuration.getString(ConfigVars.ZOOKEEPER_PORT_KEY));
                prop.setProperty("hbase.master", "127.0.0.1:" + configuration.getInt(ConfigVars.HBASE_MASTER_PORT_KEY));
                prop.setProperty("zookeeper.znode.parent", configuration.getString(ConfigVars.HBASE_ZNODE_PARENT_KEY));

                Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
                Connection con = DriverManager.getConnection("jdbc:phoenix:" + zkConfig + ":" +  configuration.getString(ConfigVars.HBASE_ZNODE_PARENT_KEY), prop);
            } catch (SQLException | ClassNotFoundException e ) {
                e.printStackTrace();
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
                hbaseLocalCluster.stop(true);
            } catch (Exception e) {
                LOGGER.error("unable to stop hbase", e);
            }
            cleanup();
            state = State.STOPPED;
            LOGGER.info("{} is stopped", this.getClass().getName());
        }
        return this;

    }

    @Override
    public org.apache.hadoop.conf.Configuration getConfiguration() {
        return hbaseLocalCluster.getHbaseConfiguration();
    }


}
