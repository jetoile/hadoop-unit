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
import fr.jetoile.hadoopunit.Component;
import fr.jetoile.hadoopunit.HadoopBootstrap;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.HadoopUtils;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import fr.jetoile.hadoopunit.exception.NotFoundServiceException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class HdfsBootstrap implements Bootstrap {
    final public static String NAME = Component.HDFS.name();

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

    public HdfsBootstrap() {
        if (hdfsLocalCluster == null) {
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
        HadoopUtils.INSTANCE.setHadoopHome();
        try {
            configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }

        port = configuration.getInt(HadoopUnitConfig.HDFS_NAMENODE_PORT_KEY);
        httpPort = configuration.getInt(HadoopUnitConfig.HDFS_NAMENODE_HTTP_PORT_KEY);
        tempDirectory = configuration.getString(HadoopUnitConfig.HDFS_TEMP_DIR_KEY);
        numDatanodes = configuration.getInt(HadoopUnitConfig.HDFS_NUM_DATANODES_KEY);
        enablePermission = configuration.getBoolean(HadoopUnitConfig.HDFS_ENABLE_PERMISSIONS_KEY);
        format = configuration.getBoolean(HadoopUnitConfig.HDFS_FORMAT_KEY);
        enableRunningUserAsProxy = configuration.getBoolean(HadoopUnitConfig.HDFS_ENABLE_RUNNING_USER_AS_PROXY_USER);
    }

    @Override
    public void loadConfig(Map<String, String> configs) {
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.HDFS_NAMENODE_PORT_KEY))) {
            port = Integer.parseInt(configs.get(HadoopUnitConfig.HDFS_NAMENODE_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.HDFS_NAMENODE_HTTP_PORT_KEY))) {
            httpPort = Integer.parseInt(configs.get(HadoopUnitConfig.HDFS_NAMENODE_HTTP_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.HDFS_TEMP_DIR_KEY))) {
            tempDirectory = configs.get(HadoopUnitConfig.HDFS_TEMP_DIR_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.HDFS_NUM_DATANODES_KEY))) {
            numDatanodes = Integer.parseInt(configs.get(HadoopUnitConfig.HDFS_NUM_DATANODES_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.HDFS_ENABLE_PERMISSIONS_KEY))) {
            enablePermission = Boolean.parseBoolean(configs.get(HadoopUnitConfig.HDFS_ENABLE_PERMISSIONS_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.HDFS_FORMAT_KEY))) {
            format = Boolean.parseBoolean(configs.get(HadoopUnitConfig.HDFS_FORMAT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.HDFS_ENABLE_RUNNING_USER_AS_PROXY_USER))) {
            enableRunningUserAsProxy = Boolean.parseBoolean(configs.get(HadoopUnitConfig.HDFS_ENABLE_RUNNING_USER_AS_PROXY_USER));
        }
    }


    @Override
    public Bootstrap start() {
        if (state == State.STOPPED) {
            state = State.STARTING;
            LOGGER.info("{} is starting", this.getClass().getName());
            init();
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
