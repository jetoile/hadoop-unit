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

import com.github.sakserv.minicluster.impl.MRLocalCluster;
import com.github.sakserv.minicluster.impl.OozieLocalServer;
import com.github.sakserv.minicluster.util.FileUtils;
import fr.jetoile.hadoopunit.Component;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.HadoopBootstrap;
import fr.jetoile.hadoopunit.HadoopUtils;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import fr.jetoile.hadoopunit.exception.NotFoundServiceException;
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
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;

public class OozieBootstrap implements Bootstrap {
    final public static String NAME = Component.OOZIE.name();

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
    private String oozieTmpDir;
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


    public OozieBootstrap() {
        if (oozieLocalCluster == null) {
            try {
                loadConfig();
            } catch (BootstrapException | NotFoundServiceException e) {
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
                "host:" + oozieHost +
                ", port:" + ooziePort +
                "]";
    }

    private void init() {

    }

    private void build() throws NotFoundServiceException {
        //is done lazy because hdfsBootstrap should be started
        hdfsDefaultFs = HadoopBootstrap.INSTANCE.getService(Component.HDFS).getConfiguration().get("fs.defaultFS");
        hadoopConf = HadoopBootstrap.INSTANCE.getService(Component.HDFS).getConfiguration();

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
                .setOozieHdfsShareLibDir("file://"+oozieShareLibExtractTempDir)
                .setOozieShareLibCreate(oozieShareLibCreate)
//                .setOozieLocalShareLibCacheDir(oozieShareLibExtractTempDir)
                .setOozieLocalShareLibCacheDir(oozieLocalShareLibCacheDir)
                .setOoziePurgeLocalShareLibCache(ooziePurgeLocalShareLibCache)
                .setOoziePort(ooziePort)
                .setOozieHost(oozieHost)
                .build();
    }

    private void loadConfig() throws BootstrapException, NotFoundServiceException {
        HadoopUtils.setHadoopHome();

        try {
            configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }

        oozieTestDir = configuration.getString(HadoopUnitConfig.OOZIE_TEST_DIR_KEY);
        oozieHomeDir = configuration.getString(HadoopUnitConfig.OOZIE_HOME_DIR_KEY);
        oozieUsername = System.getProperty("user.name");
        oozieGroupname = configuration.getString(HadoopUnitConfig.OOZIE_GROUPNAME_KEY);
        oozieYarnResourceManagerAddress = configuration.getString(HadoopUnitConfig.YARN_RESOURCE_MANAGER_ADDRESS_KEY);

        oozieHdfsShareLibDir = configuration.getString(HadoopUnitConfig.OOZIE_HDFS_SHARE_LIB_DIR_KEY);
        oozieShareLibCreate = configuration.getBoolean(HadoopUnitConfig.OOZIE_SHARE_LIB_CREATE_KEY);
        oozieLocalShareLibCacheDir = configuration.getString(HadoopUnitConfig.OOZIE_LOCAL_SHARE_LIB_CACHE_DIR_KEY);
        ooziePurgeLocalShareLibCache = configuration.getBoolean(HadoopUnitConfig.OOZIE_PURGE_LOCAL_SHARE_LIB_CACHE_KEY);

        oozieTmpDir = configuration.getString(HadoopUnitConfig.OOZIE_TMP_DIR_KEY);

        numNodeManagers = Integer.parseInt(configuration.getString(HadoopUnitConfig.YARN_NUM_NODE_MANAGERS_KEY));
        jobHistoryAddress = configuration.getString(HadoopUnitConfig.MR_JOB_HISTORY_ADDRESS_KEY);
        resourceManagerAddress = configuration.getString(HadoopUnitConfig.YARN_RESOURCE_MANAGER_ADDRESS_KEY);
        resourceManagerHostname = configuration.getString(HadoopUnitConfig.YARN_RESOURCE_MANAGER_HOSTNAME_KEY);
        resourceManagerSchedulerAddress = configuration.getString(HadoopUnitConfig.YARN_RESOURCE_MANAGER_SCHEDULER_ADDRESS_KEY);
        resourceManagerResourceTrackerAddress = configuration.getString(HadoopUnitConfig.YARN_RESOURCE_MANAGER_RESOURCE_TRACKER_ADDRESS_KEY);
        resourceManagerWebappAddress = configuration.getString(HadoopUnitConfig.YARN_RESOURCE_MANAGER_WEBAPP_ADDRESS_KEY);
        useInJvmContainerExecutor = configuration.getBoolean(HadoopUnitConfig.YARN_USE_IN_JVM_CONTAINER_EXECUTOR_KEY);

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
            try {
                createShareLib();
                build();
            } catch (NotFoundServiceException e) {
                LOGGER.error("unable to add oozie", e);
            }
            try {
                mrLocalCluster.start();
                oozieLocalCluster.start();
            } catch (Exception e) {
                LOGGER.error("unable to add oozie", e);
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
        FileUtils.deleteFolder(oozieTmpDir);
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
                Paths.get(oozieTmpDir).toFile().mkdirs();
                // Get and extract the oozie release
                String oozieExtractTempDir = extractOozieTarFileToTempDir(new File(oozieShareLibPath + Path.SEPARATOR + oozieShareLibName));

                // Extract the sharelib tarball to a temp dir
                fullOozieShareLibTarFilePath = oozieExtractTempDir + Path.SEPARATOR + "oozie-" + getOozieVersionFromOozieTarFileName()
                        + Path.SEPARATOR + "oozie-sharelib-" + getOozieVersionFromOozieTarFileName() + ".tar.gz";
                ;

                oozieShareLibExtractTempDir = extractOozieShareLibTarFileToTempDir(new File(fullOozieShareLibTarFilePath));

                // Copy the sharelib into HDFS
                Path destPath = new Path(oozieHdfsShareLibDir + Path.SEPARATOR + SHARE_LIB_PREFIX + getTimestampDirectory());
                LOGGER.info("OOZIE: Writing share lib contents to: {}", destPath);
                FileSystem hdfsFileSystem = null;
                try {
                    hdfsFileSystem = ((HdfsBootstrap) HadoopBootstrap.INSTANCE.getService(Component.HDFS)).getHdfsFileSystemHandle();
                } catch (Exception e) {
                    LOGGER.error("unable to get hdfs client");
                }
                hdfsFileSystem.copyFromLocalFile(false, new Path(new File(oozieShareLibExtractTempDir).toURI()), destPath);

//                if (purgeLocalShareLibCache) {
//                    FileUtils.deleteDirectory(new File(shareLibCacheDir));
//                }

            } catch (IOException e) {
                LOGGER.error("unable to copy oozie sharelib into hdfs", e);
            }
        }
    }

    public String extractOozieTarFileToTempDir(File fullOozieTarFilePath) throws IOException {
        File tempDir = File.createTempFile(SHARE_LIB_LOCAL_TEMP_PREFIX, "", Paths.get(oozieTmpDir).toFile());
        tempDir.delete();
        tempDir.mkdir();
        tempDir.deleteOnExit();

        FileUtil.unTar(fullOozieTarFilePath, tempDir);

        return tempDir.getAbsolutePath();
    }

    public String extractOozieShareLibTarFileToTempDir(File fullOozieShareLibTarFilePath) throws IOException {
        File tempDir = File.createTempFile(SHARE_LIB_LOCAL_TEMP_PREFIX, "", Paths.get(oozieTmpDir).toFile());
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
