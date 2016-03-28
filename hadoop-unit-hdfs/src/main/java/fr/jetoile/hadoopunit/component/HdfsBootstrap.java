/*
 * Copyright (c) 2011 Khanh Tuong Maudoux <kmx.petals@gmail.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package fr.jetoile.hadoopunit.component;

import com.github.sakserv.minicluster.impl.HdfsLocalCluster;
import fr.jetoile.hadoopunit.Component;
import fr.jetoile.hadoopunit.Config;
import fr.jetoile.hadoopunit.HadoopUtils;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsBootstrap implements Bootstrap {
    final public static String NAME = Component.HDFS.name();

    final private Logger LOGGER = LoggerFactory.getLogger(HdfsBootstrap.class);

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
        HadoopUtils.setHadoopHome();
        try {
            configuration = new PropertiesConfiguration("default.properties");
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }

        port = configuration.getInt(Config.HDFS_NAMENODE_PORT_KEY);
        httpPort = configuration.getInt(Config.HDFS_NAMENODE_HTTP_PORT_KEY);
        tempDirectory = configuration.getString(Config.HDFS_TEMP_DIR_KEY);
        numDatanodes = configuration.getInt(Config.HDFS_NUM_DATANODES_KEY);
        enablePermission = configuration.getBoolean(Config.HDFS_ENABLE_PERMISSIONS_KEY);
        format = configuration.getBoolean(Config.HDFS_FORMAT_KEY);
        enableRunningUserAsProxy = configuration.getBoolean(Config.HDFS_ENABLE_RUNNING_USER_AS_PROXY_USER);
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
                LOGGER.error("unable to start hdfs", e);
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
