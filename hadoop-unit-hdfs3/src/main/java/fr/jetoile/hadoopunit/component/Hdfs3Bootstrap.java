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

public class Hdfs3Bootstrap implements BootstrapHadoop3 {
    static final private Logger LOGGER = LoggerFactory.getLogger(Hdfs3Bootstrap.class);

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

    private String datanodeAddress = "127.0.0.1:50010";
    private String datanodeHttpAddress = "127.0.0.1:50075";
    private String datanodeIpcAddress = "127.0.0.1:50020";

    public Hdfs3Bootstrap() {
        if (hdfsLocalCluster == null) {
            try {
                configuration = HadoopUtils.INSTANCE.loadConfigFile(null);
                loadConfig();
            } catch (BootstrapException e) {
                LOGGER.error("unable to load configuration", e);
            }
        }
    }

    public Hdfs3Bootstrap(URL url) {
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
                "\n \t\t\t datanodeAddress:" + datanodeAddress +
                "\n \t\t\t datanodeHttpAddress:" + datanodeHttpAddress +
                "\n \t\t\t datanodeIpcAddress:" + datanodeIpcAddress +
                "\n \t\t\t httpPort:" + httpPort;
    }

    @Override
    public ComponentMetadata getMetadata() {
        return new Hdfs3Metadata();
    }

    private void build() {
        HdfsConfiguration hdfsConfiguration = new HdfsConfiguration();
        hdfsConfiguration.set("dfs.replication", replication.toString());

        hdfsConfiguration.set("dfs.datanode.address", datanodeAddress);
        hdfsConfiguration.set("dfs.datanode.http.address", datanodeHttpAddress);
        hdfsConfiguration.set("dfs.datanode.ipc.address", datanodeIpcAddress);

        hdfsConfiguration.set("dfs.datanode.hostname", host);
        hdfsConfiguration.set("fs.defaultFS", "hdfs://" + host + ":" + port);
        hdfsConfiguration.set("dfs.namenode.http-address", host + ":" + httpPort);
        hdfsConfiguration.set("dfs.namenode.rpc-address", host + ":" + port);

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

        host = configuration.getString(Hdfs3Config.HDFS3_NAMENODE_HOST_KEY);
        port = configuration.getInt(Hdfs3Config.HDFS3_NAMENODE_PORT_KEY);
        httpPort = configuration.getInt(Hdfs3Config.HDFS3_NAMENODE_HTTP_PORT_KEY);
        tempDirectory = getTmpDirPath(configuration, Hdfs3Config.HDFS3_TEMP_DIR_KEY);
        numDatanodes = configuration.getInt(Hdfs3Config.HDFS3_NUM_DATANODES_KEY);
        enablePermission = configuration.getBoolean(Hdfs3Config.HDFS3_ENABLE_PERMISSIONS_KEY);
        format = configuration.getBoolean(Hdfs3Config.HDFS3_FORMAT_KEY);
        enableRunningUserAsProxy = configuration.getBoolean(Hdfs3Config.HDFS3_ENABLE_RUNNING_USER_AS_PROXY_USER);
        replication = configuration.getInt(Hdfs3Config.HDFS3_REPLICATION_KEY, replication);

        datanodeAddress = configuration.getString(Hdfs3Config.HDFS3_DATANODE_ADDRESS_KEY, datanodeAddress);
        datanodeHttpAddress = configuration.getString(Hdfs3Config.HDFS3_DATANODE_HTTP_ADDRESS_KEY, datanodeHttpAddress);
        datanodeIpcAddress = configuration.getString(Hdfs3Config.HDFS3_DATANODE_IPC_ADDRESS_KEY, datanodeIpcAddress);
    }

    @Override
    public void loadConfig(Map<String, String> configs) {
        if (StringUtils.isNotEmpty(configs.get(Hdfs3Config.HDFS3_NAMENODE_HOST_KEY))) {
            host = configs.get(Hdfs3Config.HDFS3_NAMENODE_HOST_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(Hdfs3Config.HDFS3_NAMENODE_PORT_KEY))) {
            port = Integer.parseInt(configs.get(Hdfs3Config.HDFS3_NAMENODE_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(Hdfs3Config.HDFS3_NAMENODE_HTTP_PORT_KEY))) {
            httpPort = Integer.parseInt(configs.get(Hdfs3Config.HDFS3_NAMENODE_HTTP_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(Hdfs3Config.HDFS3_TEMP_DIR_KEY))) {
            tempDirectory = getTmpDirPath(configs, Hdfs3Config.HDFS3_TEMP_DIR_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(Hdfs3Config.HDFS3_NUM_DATANODES_KEY))) {
            numDatanodes = Integer.parseInt(configs.get(Hdfs3Config.HDFS3_NUM_DATANODES_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(Hdfs3Config.HDFS3_ENABLE_PERMISSIONS_KEY))) {
            enablePermission = Boolean.parseBoolean(configs.get(Hdfs3Config.HDFS3_ENABLE_PERMISSIONS_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(Hdfs3Config.HDFS3_FORMAT_KEY))) {
            format = Boolean.parseBoolean(configs.get(Hdfs3Config.HDFS3_FORMAT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(Hdfs3Config.HDFS3_ENABLE_RUNNING_USER_AS_PROXY_USER))) {
            enableRunningUserAsProxy = Boolean.parseBoolean(configs.get(Hdfs3Config.HDFS3_ENABLE_RUNNING_USER_AS_PROXY_USER));
        }
        if (StringUtils.isNotEmpty(configs.get(Hdfs3Config.HDFS3_REPLICATION_KEY))) {
            replication = Integer.parseInt(configs.get(Hdfs3Config.HDFS3_REPLICATION_KEY));
        }

        if (StringUtils.isNotEmpty(configs.get(Hdfs3Config.HDFS3_DATANODE_ADDRESS_KEY))) {
            datanodeAddress = configs.get(Hdfs3Config.HDFS3_DATANODE_ADDRESS_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(Hdfs3Config.HDFS3_DATANODE_HTTP_ADDRESS_KEY))) {
            datanodeHttpAddress = configs.get(Hdfs3Config.HDFS3_DATANODE_HTTP_ADDRESS_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(Hdfs3Config.HDFS3_DATANODE_IPC_ADDRESS_KEY))) {
            datanodeIpcAddress = configs.get(Hdfs3Config.HDFS3_DATANODE_IPC_ADDRESS_KEY);
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
