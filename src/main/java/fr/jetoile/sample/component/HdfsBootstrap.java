package fr.jetoile.sample.component;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.impl.HdfsLocalCluster;
import fr.jetoile.sample.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum HdfsBootstrap implements Bootstrap {
    INSTANCE;

    final private Logger LOGGER = LoggerFactory.getLogger(HdfsBootstrap.class);

    private HdfsLocalCluster hdfsLocalCluster;

    private Configuration configuration;
    private int port;
    private boolean enableRunningUserAsProxy;
    private String tempDirectory;
    private int numDatanodes;
    private boolean enablePermission;
    private boolean format;
    private int httpPort;


    HdfsBootstrap() {
        if (hdfsLocalCluster == null) {
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
        hdfsLocalCluster = new HdfsLocalCluster.Builder()
                .setHdfsNamenodePort(port)
                .setHdfsNamenodeHttpPort(httpPort)
                .setHdfsEnablePermissions(enablePermission)
                .setHdfsEnableRunningUserAsProxyUser(enableRunningUserAsProxy)
                .setHdfsFormat(format)
                .setHdfsNumDatanodes(numDatanodes)
                .setHdfsTempDir(tempDirectory)
                .setHdfsConfig(new HdfsConfiguration())
                .build();
    }

    private void loadConfig() throws BootstrapException {
        try {
            configuration = new PropertiesConfiguration("default.properties");
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }

        port = configuration.getInt(ConfigVars.HDFS_NAMENODE_PORT_KEY);
        httpPort = configuration.getInt(ConfigVars.HDFS_NAMENODE_HTTP_PORT_KEY);
        tempDirectory = configuration.getString(ConfigVars.HDFS_TEMP_DIR_KEY);
        numDatanodes = configuration.getInt(ConfigVars.HDFS_NUM_DATANODES_KEY);
        enablePermission = configuration.getBoolean(ConfigVars.HDFS_ENABLE_PERMISSIONS_KEY);
        format = configuration.getBoolean(ConfigVars.HDFS_FORMAT_KEY);
        enableRunningUserAsProxy = configuration.getBoolean(ConfigVars.HDFS_ENABLE_RUNNING_USER_AS_PROXY_USER);
    }

    @Override
    public Bootstrap start() {
        try {
            hdfsLocalCluster.start();
        } catch (Exception e) {
            LOGGER.error("unable to start hdfs", e);
        }
        return this;
    }

    @Override
    public Bootstrap stop() {
        try {
            hdfsLocalCluster.stop(true);
        } catch (Exception e) {
            LOGGER.error("unable to stop hdfs", e);
        }
        return this;

    }

    @Override
    public org.apache.hadoop.conf.Configuration getConfiguration() {
        return hdfsLocalCluster.getHdfsConfig();
    }


    public FileSystem getHdfsFileSystemHandle() throws Exception {
        return hdfsLocalCluster.getHdfsFileSystemHandle();
    }


}
