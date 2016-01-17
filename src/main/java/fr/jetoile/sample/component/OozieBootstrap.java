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
import com.github.sakserv.minicluster.impl.MRLocalCluster;
import com.github.sakserv.minicluster.impl.OozieLocalServer;
import com.github.sakserv.minicluster.util.FileUtils;
import fr.jetoile.sample.HadoopUtils;
import fr.jetoile.sample.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.OozieClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public enum OozieBootstrap implements Bootstrap {
    INSTANCE;

    final private Logger LOGGER = LoggerFactory.getLogger(OozieBootstrap.class);

    public static final String OOZIE_PORT = "oozie.port";
    public static final String OOZIE_HOST = "oozie.host";
    private static final String OOZIE_SHARELIB_PATH_KEY = "oozie.sharelib.path";
    private static final String OOZIE_SHARELIB_NAME_KEY = "oozie.sharelib.name";
    private static final String SHARE_LIB_LOCAL_TEMP_PREFIX = "oozie_share_lib_tmp";
    private static final String SHARE_LIB_PREFIX = "lib_";

    private OozieLocalServer oozieLocalCluster;
    private MRLocalCluster mrLocalCluster;


    private State state = State.STOPPED;

    private Configuration configuration;
    private String oozieTestDir;
    private String oozieHomeDir;
    private String oozieUsername;
    private String oozieGroupname;
    private String oozieYarnResourceManagerAddress;
    private org.apache.hadoop.conf.Configuration hadoopConf;
    private String hdfsDefaultFs;
    private String oozieHdfsShareLibDir;
    private boolean oozieShareLibCreate;
    private String oozieLocalShareLibCacheDir;
    private boolean ooziePurgeLocalShareLibCache;
    private int numNodeManagers;
    private String jobHistoryAddress;
    private String resourceManagerAddress;
    private String resourceManagerHostname;
    private String resourceManagerSchedulerAddress;
    private String resourceManagerResourceTrackerAddress;
    private String resourceManagerWebappAddress;
    private boolean useInJvmContainerExecutor;
    private String oozieShareLibPath;
    private String oozieShareLibName;
    private int ooziePort;
    private String oozieHost;
    private String fullOozieShareLibTarFilePath;
    private String oozieShareLibExtractTempDir;


    OozieBootstrap() {
        if (oozieLocalCluster == null) {
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
        mrLocalCluster = new MRLocalCluster.Builder()
                .setNumNodeManagers(numNodeManagers)
                .setJobHistoryAddress(jobHistoryAddress)
                .setResourceManagerAddress(resourceManagerAddress)
                .setResourceManagerHostname(resourceManagerHostname)
                .setResourceManagerSchedulerAddress(resourceManagerSchedulerAddress)
                .setResourceManagerResourceTrackerAddress(resourceManagerResourceTrackerAddress)
                .setResourceManagerWebappAddress(resourceManagerWebappAddress)
                .setUseInJvmContainerExecutor(useInJvmContainerExecutor)
                .setHdfsDefaultFs(hdfsDefaultFs)
                .setConfig(hadoopConf)
                .build();

        oozieLocalCluster = new OozieLocalServer.Builder()
                .setOozieTestDir(oozieTestDir)
                .setOozieHomeDir(oozieHomeDir)
                .setOozieUsername(oozieUsername)
                .setOozieGroupname(oozieGroupname)
                .setOozieYarnResourceManagerAddress(oozieYarnResourceManagerAddress)
                .setOozieHdfsDefaultFs(hdfsDefaultFs)
                .setOozieConf(hadoopConf)
                .setOozieHdfsShareLibDir(oozieHdfsShareLibDir)
                .setOozieShareLibCreate(oozieShareLibCreate)
                .setOozieLocalShareLibCacheDir(oozieLocalShareLibCacheDir)
                .setOoziePurgeLocalShareLibCache(ooziePurgeLocalShareLibCache)
                .setOoziePort(ooziePort)
                .setOozieHost(oozieHost)
                .build();
    }

    private void loadConfig() throws BootstrapException {
        HadoopUtils.setHadoopHome();

        try {
            configuration = new PropertiesConfiguration("default.properties");
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }

        oozieTestDir = configuration.getString(ConfigVars.OOZIE_TEST_DIR_KEY);
        oozieHomeDir = configuration.getString(ConfigVars.OOZIE_HOME_DIR_KEY);
        oozieUsername = System.getProperty("user.name");
        oozieGroupname = configuration.getString(ConfigVars.OOZIE_GROUPNAME_KEY);
        oozieYarnResourceManagerAddress = configuration.getString(ConfigVars.YARN_RESOURCE_MANAGER_ADDRESS_KEY);
        hdfsDefaultFs = HdfsBootstrap.INSTANCE.getConfiguration().get("fs.defaultFS");
        hadoopConf = HdfsBootstrap.INSTANCE.getConfiguration();
        oozieHdfsShareLibDir = configuration.getString(ConfigVars.OOZIE_HDFS_SHARE_LIB_DIR_KEY);
        oozieShareLibCreate = configuration.getBoolean(ConfigVars.OOZIE_SHARE_LIB_CREATE_KEY);
        oozieLocalShareLibCacheDir = configuration.getString(ConfigVars.OOZIE_LOCAL_SHARE_LIB_CACHE_DIR_KEY);
        ooziePurgeLocalShareLibCache = configuration.getBoolean(ConfigVars.OOZIE_PURGE_LOCAL_SHARE_LIB_CACHE_KEY);

        numNodeManagers = Integer.parseInt(configuration.getString(ConfigVars.YARN_NUM_NODE_MANAGERS_KEY));
        jobHistoryAddress = configuration.getString(ConfigVars.MR_JOB_HISTORY_ADDRESS_KEY);
        resourceManagerAddress = configuration.getString(ConfigVars.YARN_RESOURCE_MANAGER_ADDRESS_KEY);
        resourceManagerHostname = configuration.getString(ConfigVars.YARN_RESOURCE_MANAGER_HOSTNAME_KEY);
        resourceManagerSchedulerAddress = configuration.getString(ConfigVars.YARN_RESOURCE_MANAGER_SCHEDULER_ADDRESS_KEY);
        resourceManagerResourceTrackerAddress = configuration.getString(ConfigVars.YARN_RESOURCE_MANAGER_RESOURCE_TRACKER_ADDRESS_KEY);
        resourceManagerWebappAddress = configuration.getString(ConfigVars.YARN_RESOURCE_MANAGER_WEBAPP_ADDRESS_KEY);
        useInJvmContainerExecutor = configuration.getBoolean(ConfigVars.YARN_USE_IN_JVM_CONTAINER_EXECUTOR_KEY);

        ooziePort = configuration.getInt(OOZIE_PORT);
        oozieHost = configuration.getString(OOZIE_HOST);

        oozieShareLibPath = configuration.getString(OOZIE_SHARELIB_PATH_KEY);
        oozieShareLibName = configuration.getString(OOZIE_SHARELIB_NAME_KEY);

    }

    @Override
    public Bootstrap start() {
        if (state == State.STOPPED) {
            state = State.STARTING;
            LOGGER.info("{} is starting", this.getClass().getName());
            init();
            build();
            createShareLib();
            try {
                mrLocalCluster.start();
                oozieLocalCluster.start();
            } catch (Exception e) {
                LOGGER.error("unable to start oozie", e);
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
                oozieLocalCluster.stop(true);
                mrLocalCluster.stop(true);
                cleanup();
            } catch (Exception e) {
                LOGGER.error("unable to stop hdfs", e);
            }
            state = State.STOPPED;
            LOGGER.info("{} is stopped", this.getClass().getName());
        }
        return this;

    }

    private void cleanup() {
        FileUtils.deleteFolder(fullOozieShareLibTarFilePath);
        FileUtils.deleteFolder(oozieShareLibExtractTempDir);
    }

    @Override
    public org.apache.hadoop.conf.Configuration getConfiguration() {
        return oozieLocalCluster.getOozieConf();
    }

    public OozieClient getOozieClient() {
        return oozieLocalCluster.getOozieClient();
    }


    // Main driver that downloads, extracts, and deploys the oozie sharelib
    public void createShareLib() {

        if (!oozieShareLibCreate) {
            LOGGER.info("OOZIE: Share Lib Create Disabled... skipping");
        } else {

            try {
                // Get and extract the oozie release
                String oozieExtractTempDir = extractOozieTarFileToTempDir(new File(oozieShareLibPath + Path.SEPARATOR + oozieShareLibName));

                // Extract the sharelib tarball to a temp dir
                fullOozieShareLibTarFilePath = oozieExtractTempDir + Path.SEPARATOR + "oozie-" + getOozieVersionFromOozieTarFileName()
                        + Path.SEPARATOR + "oozie-sharelib-" + getOozieVersionFromOozieTarFileName() + ".tar.gz";
                ;

                oozieShareLibExtractTempDir = extractOozieShareLibTarFileToTempDir(new File(fullOozieShareLibTarFilePath));

                // Copy the sharelib into HDFS
                Path destPath = new Path(oozieHdfsShareLibDir + Path.SEPARATOR +
                        SHARE_LIB_PREFIX + getTimestampDirectory());
                LOGGER.info("OOZIE: Writing share lib contents to: {}", destPath);
                FileSystem hdfsFileSystem = null;
                try {
                    hdfsFileSystem = HdfsBootstrap.INSTANCE.getHdfsFileSystemHandle();
                } catch (Exception e) {
                    LOGGER.error("unable to get hdfs client");
                }
                hdfsFileSystem.copyFromLocalFile(false, new Path(new File(oozieShareLibExtractTempDir).toURI()), destPath);

//                if (purgeLocalShareLibCache) {
//                    FileUtils.deleteDirectory(new File(shareLibCacheDir));
//                }

            } catch (IOException e) {
                LOGGER.error("unable to copy oozie sharelib into hdfs");
            }
        }
    }

    public String extractOozieTarFileToTempDir(File fullOozieTarFilePath) throws IOException {
        File tempDir = File.createTempFile(SHARE_LIB_LOCAL_TEMP_PREFIX, "");
        tempDir.delete();
        tempDir.mkdir();
        tempDir.deleteOnExit();

        FileUtil.unTar(fullOozieTarFilePath, tempDir);

        return tempDir.getAbsolutePath();
    }

    public String extractOozieShareLibTarFileToTempDir(File fullOozieShareLibTarFilePath) throws IOException {
        File tempDir = File.createTempFile(SHARE_LIB_LOCAL_TEMP_PREFIX, "");
        tempDir.delete();
        tempDir.mkdir();
        tempDir.deleteOnExit();

        FileUtil.unTar(fullOozieShareLibTarFilePath, tempDir);

        return tempDir.getAbsolutePath();
    }

    public String getOozieVersionFromOozieTarFileName() {
        return oozieShareLibName.replace("-distro.tar.gz", "").replace("oozie-", "");
    }

    public String getTimestampDirectory() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        Date date = new Date();
        return dateFormat.format(date).toString();
    }
}
