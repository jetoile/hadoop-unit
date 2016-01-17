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
package fr.jetoile.sample.component;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.impl.HiveLocalMetaStore;
import com.github.sakserv.minicluster.util.FileUtils;
import com.github.sakserv.minicluster.util.WindowsLibsUtils;
import fr.jetoile.sample.HadoopUtils;
import fr.jetoile.sample.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum HiveMetastoreBootstrap implements Bootstrap {
    INSTANCE;

    final private Logger LOGGER = LoggerFactory.getLogger(HiveMetastoreBootstrap.class);

    private HiveLocalMetaStore hiveLocalMetaStore;

    private State state = State.STOPPED;

    private Configuration configuration;
    private String host;
    private int port;
    private String derbyDirectory;
    private String scratchDirectory;
    private String warehouseDirectory;

    HiveMetastoreBootstrap() {
        if (hiveLocalMetaStore == null) {
            try {
                loadConfig();
            } catch (BootstrapException e) {
                LOGGER.error("unable to load configuration", e);
            }
        }
    }

    private void loadConfig() throws BootstrapException {
        HadoopUtils.setHadoopHome();
        try {
            configuration = new PropertiesConfiguration("default.properties");
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }
        host = configuration.getString(ConfigVars.HIVE_METASTORE_HOSTNAME_KEY);
        port = configuration.getInt(ConfigVars.HIVE_METASTORE_PORT_KEY);
        derbyDirectory = configuration.getString(ConfigVars.HIVE_METASTORE_DERBY_DB_DIR_KEY);
        scratchDirectory = configuration.getString(ConfigVars.HIVE_SCRATCH_DIR_KEY);
        warehouseDirectory = configuration.getString(ConfigVars.HIVE_WAREHOUSE_DIR_KEY);

    }


    private void cleanup() {
        FileUtils.deleteFolder(derbyDirectory);
        FileUtils.deleteFolder(derbyDirectory.substring(derbyDirectory.lastIndexOf("/") + 1));

    }

    private void build() {
        hiveLocalMetaStore = new HiveLocalMetaStore.Builder()
                .setHiveMetastoreDerbyDbDir(derbyDirectory)
                .setHiveMetastoreHostname(host)
                .setHiveMetastorePort(port)
                .setHiveWarehouseDir(warehouseDirectory)
                .setHiveScratchDir(scratchDirectory)
                .setHiveConf(buildHiveConf())
                .build();
    }

    private HiveConf buildHiveConf() {
        // Handle Windows
        WindowsLibsUtils.setHadoopHome();

        HiveConf hiveConf = new HiveConf();
//        hiveConf.set(HiveConf.ConfVars.HIVE_TXN_MANAGER.varname, "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
//        hiveConf.set(HiveConf.ConfVars.HIVE_COMPACTOR_INITIATOR_ON.varname, "true");
//        hiveConf.set(HiveConf.ConfVars.HIVE_COMPACTOR_WORKER_THREADS.varname, "5");
//        hiveConf.set("hive.root.logger", "DEBUG,console");
//        hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
//        hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
//        hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
//        System.setProperty(HiveConf.ConfVars.PREEXECHOOKS.varname, " ");
//        System.setProperty(HiveConf.ConfVars.POSTEXECHOOKS.varname, " ");

        return hiveConf;
    }

    @Override
    public Bootstrap start() {
        if (state == State.STOPPED) {
            state = State.STARTING;
            LOGGER.info("{} is starting", this.getClass().getName());
            build();

            try {
                hiveLocalMetaStore.start();
            } catch (Exception e) {
                LOGGER.error("unable to start hivemetastore", e);
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
                hiveLocalMetaStore.stop(true);
            } catch (Exception e) {
                LOGGER.error("unable to stop hivemetastore", e);
            }
            cleanup();
            state = State.STOPPED;
            LOGGER.info("{} is stopped", this.getClass().getName());
        }
        return this;
    }

    @Override
    public org.apache.hadoop.conf.Configuration getConfiguration() {
        return hiveLocalMetaStore.getHiveConf();
    }


}
