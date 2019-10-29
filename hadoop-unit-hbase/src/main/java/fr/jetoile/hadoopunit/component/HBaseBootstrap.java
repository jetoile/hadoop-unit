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

import com.github.sakserv.minicluster.impl.HbaseLocalCluster;
import com.github.sakserv.minicluster.util.FileUtils;
import fr.jetoile.hadoopunit.ComponentMetadata;
import fr.jetoile.hadoopunit.HadoopUtils;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Map;

public class HBaseBootstrap implements BootstrapHadoop {
    static final private Logger LOGGER = LoggerFactory.getLogger(HBaseBootstrap.class);

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

    private String restHost;
    private int restPort;
    private int restInfoPort;
    private boolean restReadOnly;
    private int restMaxThread;
    private int restMinThread;

    private String hdfsUri;

    public HBaseBootstrap() {
        if (hbaseLocalCluster == null) {
            try {
                configuration = HadoopUtils.INSTANCE.loadConfigFile(null);
                loadConfig();
            } catch (BootstrapException e) {
                LOGGER.error("unable to load configuration", e);
            }
        }
    }

    public HBaseBootstrap(URL url) {
        if (hbaseLocalCluster == null) {
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
        return new HBaseMetadata();
    }

    @Override
    public String getProperties() {
        return "\n \t\t\t port:" + port +
                "\n \t\t\t restPort:" + restPort;
    }

    private void init() {

    }

    private void build() {
        org.apache.hadoop.conf.Configuration hbaseConfiguration = new org.apache.hadoop.conf.Configuration();
        hbaseConfiguration.setBoolean("hbase.table.sanity.checks", false);
        hbaseConfiguration.set("fs.default.name", hdfsUri);
        hbaseConfiguration.set("hbase.regionserver.hlog.tolerable.lowreplication", "1");

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
                .activeRestGateway()
                .setHbaseRestHost(restHost)
                .setHbaseRestPort(restPort)
                .setHbaseRestInfoPort(restInfoPort)
                .setHbaseRestReadOnly(restReadOnly)
                .setHbaseRestThreadMax(restMaxThread)
                .setHbaseRestThreadMin(restMinThread)
                .build()
                .build();
    }

    private void loadConfig() throws BootstrapException {
        port = configuration.getInt(HBaseConfig.HBASE_MASTER_PORT_KEY);
        infoPort = configuration.getInt(HBaseConfig.HBASE_MASTER_INFO_PORT_KEY);
        nbRegionServer = configuration.getInt(HBaseConfig.HBASE_NUM_REGION_SERVERS_KEY);
        rootDirectory = configuration.getString(HBaseConfig.HBASE_ROOT_DIR_KEY);
        zookeeperConnectionString = configuration.getString(ZookeeperConfig.ZOOKEEPER_HOST_CLIENT_KEY) + ":" + configuration.getInt(ZookeeperConfig.ZOOKEEPER_PORT_KEY);
        zookeeperPort = configuration.getInt(ZookeeperConfig.ZOOKEEPER_PORT_KEY);
        zookeeperZnodeParent = configuration.getString(HBaseConfig.HBASE_ZNODE_PARENT_KEY);
        enableWalReplication = configuration.getBoolean(HBaseConfig.HBASE_WAL_REPLICATION_ENABLED_KEY);

        restHost = configuration.getString(HBaseConfig.HBASE_REST_HOST_KEY);
        restPort = configuration.getInt(HBaseConfig.HBASE_REST_PORT_KEY);
        restInfoPort = configuration.getInt(HBaseConfig.HBASE_REST_INFO_PORT_KEY);
        restReadOnly = configuration.getBoolean(HBaseConfig.HBASE_REST_READONLY_KEY);
        restMaxThread = configuration.getInt(HBaseConfig.HBASE_REST_THREADMAX_KEY);
        restMinThread = configuration.getInt(HBaseConfig.HBASE_REST_THREADMIN_KEY);
        hdfsUri = "hdfs://" + configuration.getString(HdfsConfig.HDFS_NAMENODE_HOST_CLIENT_KEY) + ":" + configuration.getString(HdfsConfig.HDFS_NAMENODE_PORT_KEY);
    }

    @Override
    public void loadConfig(Map<String, String> configs) {
        if (StringUtils.isNotEmpty(configs.get(HBaseConfig.HBASE_MASTER_PORT_KEY))) {
            port = Integer.parseInt(configs.get(HBaseConfig.HBASE_MASTER_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HBaseConfig.HBASE_MASTER_INFO_PORT_KEY))) {
            infoPort = Integer.parseInt(configs.get(HBaseConfig.HBASE_MASTER_INFO_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HBaseConfig.HBASE_NUM_REGION_SERVERS_KEY))) {
            nbRegionServer = Integer.parseInt(configs.get(HBaseConfig.HBASE_NUM_REGION_SERVERS_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HBaseConfig.HBASE_ROOT_DIR_KEY))) {
            rootDirectory = configs.get(HBaseConfig.HBASE_ROOT_DIR_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(ZookeeperConfig.ZOOKEEPER_HOST_CLIENT_KEY)) && StringUtils.isNotEmpty(configs.get(ZookeeperConfig.ZOOKEEPER_PORT_KEY))) {
            zookeeperConnectionString = configs.get(ZookeeperConfig.ZOOKEEPER_HOST_CLIENT_KEY) + ":" + configs.get(ZookeeperConfig.ZOOKEEPER_PORT_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(ZookeeperConfig.ZOOKEEPER_PORT_KEY))) {
            zookeeperPort = Integer.parseInt(configs.get(ZookeeperConfig.ZOOKEEPER_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HBaseConfig.HBASE_ZNODE_PARENT_KEY))) {
            zookeeperZnodeParent = configs.get(HBaseConfig.HBASE_ZNODE_PARENT_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(HBaseConfig.HBASE_WAL_REPLICATION_ENABLED_KEY))) {
            enableWalReplication = Boolean.parseBoolean(configs.get(HBaseConfig.HBASE_WAL_REPLICATION_ENABLED_KEY));
        }

        if (StringUtils.isNotEmpty(configs.get(HBaseConfig.HBASE_REST_HOST_KEY))) {
            restHost = configs.get(HBaseConfig.HBASE_REST_HOST_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(HBaseConfig.HBASE_REST_PORT_KEY))) {
            restPort = Integer.parseInt(configs.get(HBaseConfig.HBASE_REST_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HBaseConfig.HBASE_REST_INFO_PORT_KEY))) {
            restInfoPort = Integer.parseInt(configs.get(HBaseConfig.HBASE_REST_INFO_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HBaseConfig.HBASE_REST_READONLY_KEY))) {
            restReadOnly = Boolean.parseBoolean(configs.get(HBaseConfig.HBASE_REST_READONLY_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HBaseConfig.HBASE_REST_THREADMAX_KEY))) {
            restMaxThread = Integer.parseInt(configs.get(HBaseConfig.HBASE_REST_THREADMAX_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HBaseConfig.HBASE_REST_THREADMIN_KEY))) {
            restMinThread = Integer.parseInt(configs.get(HBaseConfig.HBASE_REST_THREADMIN_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HdfsConfig.HDFS_NAMENODE_HOST_CLIENT_KEY)) && StringUtils.isNotEmpty(configs.get(HdfsConfig.HDFS_NAMENODE_PORT_KEY))) {
            hdfsUri = "hdfs://" + configs.get(HdfsConfig.HDFS_NAMENODE_HOST_CLIENT_KEY) + ":" + Integer.parseInt(configs.get(HdfsConfig.HDFS_NAMENODE_PORT_KEY));
        }
    }

    private void cleanup() {
        FileUtils.deleteFolder(rootDirectory);
        FileUtils.deleteFolder(rootDirectory.substring(rootDirectory.lastIndexOf("/") + 1));

    }

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
                LOGGER.error("unable to add hbase", e);
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
