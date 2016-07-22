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
import fr.jetoile.hadoopunit.Component;
import fr.jetoile.hadoopunit.HadoopBootstrap;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.HadoopUtils;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import fr.jetoile.hadoopunit.exception.NotFoundServiceException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseBootstrap implements Bootstrap {
    final public static String NAME = Component.HBASE.name();

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


    public HBaseBootstrap() {
        if (hbaseLocalCluster == null) {
            try {
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
        return "[" +
                "port:" + port +
                "]";
    }

    private void init() {

    }

    private void build() {
        org.apache.hadoop.conf.Configuration hbaseConfiguration = new org.apache.hadoop.conf.Configuration();
        hbaseConfiguration.setBoolean("hbase.table.sanity.checks", false);
        hbaseConfiguration.set("fs.default.name", "hdfs://" + configuration.getString(HadoopUnitConfig.HDFS_NAMENODE_HOST_KEY) + ":" + configuration.getString(HadoopUnitConfig.HDFS_NAMENODE_PORT_KEY));

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
        HadoopUtils.setHadoopHome();
        try {
            configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }

        port = configuration.getInt(HadoopUnitConfig.HBASE_MASTER_PORT_KEY);
        infoPort = configuration.getInt(HadoopUnitConfig.HBASE_MASTER_INFO_PORT_KEY);
        nbRegionServer = configuration.getInt(HadoopUnitConfig.HBASE_NUM_REGION_SERVERS_KEY);
        rootDirectory = configuration.getString(HadoopUnitConfig.HBASE_ROOT_DIR_KEY);
        zookeeperConnectionString = configuration.getString(HadoopUnitConfig.ZOOKEEPER_HOST_KEY) + ":" + configuration.getInt(HadoopUnitConfig.ZOOKEEPER_PORT_KEY);
        zookeeperPort = configuration.getInt(HadoopUnitConfig.ZOOKEEPER_PORT_KEY);
        zookeeperZnodeParent = configuration.getString(HadoopUnitConfig.HBASE_ZNODE_PARENT_KEY);
        enableWalReplication = configuration.getBoolean(HadoopUnitConfig.HBASE_WAL_REPLICATION_ENABLED_KEY);

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

    final public static void main(String... args) throws NotFoundServiceException {

        HadoopBootstrap bootstrap = HadoopBootstrap.INSTANCE;

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                bootstrap.stopAll();
            }
        });

        bootstrap.add(Component.HBASE).startAll();
    }
}
