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

import com.github.sakserv.minicluster.impl.HdfsLocalCluster;
import fr.jetoile.hadoopunit.ComponentMetadata;
import fr.jetoile.hadoopunit.HadoopUtils;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Map;

public class HdfsBootstrap implements BootstrapHadoop {
    static final private Logger LOGGER = LoggerFactory.getLogger(HdfsBootstrap.class);

    private HdfsLocalCluster hdfsLocalCluster;

    private State state = State.STOPPED;

    private Configuration configuration;

    private int port;
    private boolean enableRunningUserAsProxy;
    private String tempDirectory;
    private int numDatanodes;
    private boolean enablePermission;
    private boolean format;
    private int httpPort;
    private String host;
    private Integer replication = 1;

    public HdfsBootstrap() {
        if (hdfsLocalCluster == null) {
            try {
                configuration = HadoopUtils.INSTANCE.loadConfigFile(null);
                loadConfig();
            } catch (BootstrapException e) {
                LOGGER.error("unable to load configuration", e);
            }
        }
    }

    public HdfsBootstrap(URL url) {
        if (hdfsLocalCluster == null) {
            try {
                configuration = HadoopUtils.INSTANCE.loadConfigFile(url);
                loadConfig();
            } catch (BootstrapException e) {
                LOGGER.error("unable to load configuration", e);
            }
        }
    }


    @Override
    public String getProperties() {
        return "\n \t\t\t host:" + host +
                "\n \t\t\t port:" + port +
                "\n \t\t\t httpPort:" + httpPort;
    }

    @Override
    public ComponentMetadata getMetadata() {
        return new HdfsMetadata();
    }

    private void build() {
        HdfsConfiguration hdfsConfiguration = new HdfsConfiguration();
        hdfsConfiguration.set("dfs.replication", replication.toString());

        hdfsLocalCluster = new HdfsLocalCluster.Builder()
                .setHdfsNamenodePort(port)
                .setHdfsNamenodeHttpPort(httpPort)
                .setHdfsEnablePermissions(enablePermission)
                .setHdfsEnableRunningUserAsProxyUser(enableRunningUserAsProxy)
                .setHdfsFormat(format)
                .setHdfsNumDatanodes(numDatanodes)
                .setHdfsTempDir(tempDirectory)
                .setHdfsConfig(hdfsConfiguration)
                .build();
    }

    private void loadConfig() throws BootstrapException {
        HadoopUtils.INSTANCE.setHadoopHome();

        host = configuration.getString(HdfsConfig.HDFS_NAMENODE_HOST_KEY);
        port = configuration.getInt(HdfsConfig.HDFS_NAMENODE_PORT_KEY);
        httpPort = configuration.getInt(HdfsConfig.HDFS_NAMENODE_HTTP_PORT_KEY);
        tempDirectory = configuration.getString(HdfsConfig.HDFS_TEMP_DIR_KEY);
        numDatanodes = configuration.getInt(HdfsConfig.HDFS_NUM_DATANODES_KEY);
        enablePermission = configuration.getBoolean(HdfsConfig.HDFS_ENABLE_PERMISSIONS_KEY);
        format = configuration.getBoolean(HdfsConfig.HDFS_FORMAT_KEY);
        enableRunningUserAsProxy = configuration.getBoolean(HdfsConfig.HDFS_ENABLE_RUNNING_USER_AS_PROXY_USER);
        replication = configuration.getInt(HdfsConfig.HDFS_REPLICATION_KEY, 1);
    }

    @Override
    public void loadConfig(Map<String, String> configs) {
        if (StringUtils.isNotEmpty(configs.get(HdfsConfig.HDFS_NAMENODE_HOST_KEY))) {
            host = configs.get(HdfsConfig.HDFS_NAMENODE_HOST_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(HdfsConfig.HDFS_NAMENODE_PORT_KEY))) {
            port = Integer.parseInt(configs.get(HdfsConfig.HDFS_NAMENODE_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HdfsConfig.HDFS_NAMENODE_HTTP_PORT_KEY))) {
            httpPort = Integer.parseInt(configs.get(HdfsConfig.HDFS_NAMENODE_HTTP_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HdfsConfig.HDFS_TEMP_DIR_KEY))) {
            tempDirectory = configs.get(HdfsConfig.HDFS_TEMP_DIR_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(HdfsConfig.HDFS_NUM_DATANODES_KEY))) {
            numDatanodes = Integer.parseInt(configs.get(HdfsConfig.HDFS_NUM_DATANODES_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HdfsConfig.HDFS_ENABLE_PERMISSIONS_KEY))) {
            enablePermission = Boolean.parseBoolean(configs.get(HdfsConfig.HDFS_ENABLE_PERMISSIONS_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HdfsConfig.HDFS_FORMAT_KEY))) {
            format = Boolean.parseBoolean(configs.get(HdfsConfig.HDFS_FORMAT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HdfsConfig.HDFS_ENABLE_RUNNING_USER_AS_PROXY_USER))) {
            enableRunningUserAsProxy = Boolean.parseBoolean(configs.get(HdfsConfig.HDFS_ENABLE_RUNNING_USER_AS_PROXY_USER));
        }
        if (StringUtils.isNotEmpty(configs.get(HdfsConfig.HDFS_REPLICATION_KEY))) {
            replication = Integer.parseInt(configs.get(HdfsConfig.HDFS_REPLICATION_KEY));
        }
    }


    @Override
    public Bootstrap start() {
        if (state == State.STOPPED) {
            state = State.STARTING;
            LOGGER.info("{} is starting", this.getClass().getName());
            build();
            try {
                hdfsLocalCluster.start();
            } catch (Exception e) {
                LOGGER.error("unable to add hdfs", e);
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
                hdfsLocalCluster.stop(true);
            } catch (Exception e) {
                LOGGER.error("unable to stop hdfs", e);
            }
            state = State.STOPPED;
            LOGGER.info("{} is stopped", this.getClass().getName());
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
