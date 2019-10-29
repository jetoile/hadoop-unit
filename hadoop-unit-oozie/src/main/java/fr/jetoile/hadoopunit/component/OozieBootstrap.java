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
import com.github.sakserv.minicluster.oozie.sharelib.Framework;
import com.github.sakserv.minicluster.util.FileUtils;
import fr.jetoile.hadoopunit.ComponentMetadata;
import fr.jetoile.hadoopunit.HadoopUtils;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import fr.jetoile.hadoopunit.exception.NotFoundServiceException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.OozieClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class OozieBootstrap implements BootstrapHadoop {
    final private Logger LOGGER = LoggerFactory.getLogger(OozieBootstrap.class);

    private static final String SHARE_LIB_LOCAL_TEMP_PREFIX = "oozie_share_lib_tmp";
    private static final String SHARE_LIB_PREFIX = "lib_";

    private OozieLocalServer oozieLocalCluster;

    private State state = State.STOPPED;

    private Configuration configuration;
    private String oozieTmpDir;
    private String oozieTestDir;
    private String oozieHomeDir;
    private String oozieUsername;
    private String oozieGroupname;
    private String oozieYarnResourceManagerAddress;
    private String hdfsDefaultFs;
    private String oozieHdfsShareLibDir;
    private boolean oozieShareLibCreate;
    private String oozieLocalShareLibCacheDir;
    private boolean ooziePurgeLocalShareLibCache;
    private String resourceManagerWebappAddress;
    private String oozieShareLibPath;
    private String oozieShareLibName;
    private int ooziePort;
    private String oozieHost;
    private List<Framework> oozieShareLibFrameworks = new ArrayList<>();


    public OozieBootstrap() {
        if (oozieLocalCluster == null) {
            try {
                configuration = HadoopUtils.INSTANCE.loadConfigFile(null);
                loadConfig();
            } catch (BootstrapException | NotFoundServiceException e) {
                LOGGER.error("unable to load configuration", e);
            }
        }
    }

    public OozieBootstrap(URL url) {
        if (oozieLocalCluster == null) {
            try {
                configuration = HadoopUtils.INSTANCE.loadConfigFile(url);
                loadConfig();
            } catch (BootstrapException | NotFoundServiceException e) {
                LOGGER.error("unable to load configuration", e);
            }
        }
    }

    @Override
    public ComponentMetadata getMetadata() {
        return new OozieMetadata();
    }

    @Override
    public String getProperties() {
        return "\n \t\t\t host:" + oozieHost +
                "\n \t\t\t port:" + ooziePort +
                "\n \t\t\t resourceManager address: " + resourceManagerWebappAddress;
    }

    private void init() {

    }

    private void build() throws NotFoundServiceException {
        hdfsDefaultFs = "hdfs://" + configuration.getString(HdfsConfig.HDFS_NAMENODE_HOST_CLIENT_KEY) + ":" + configuration.getString(HdfsConfig.HDFS_NAMENODE_PORT_KEY);

        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("hadoop.proxyuser." + System.getProperty("user.name") + ".hosts", "*");
        hadoopConf.set("hadoop.proxyuser." + System.getProperty("user.name") + ".groups", "*");
        hadoopConf.set("oozie.service.WorkflowAppService.system.libpath", hdfsDefaultFs + "/" + oozieHdfsShareLibDir);
        hadoopConf.set("oozie.use.system.libpath", "true");

        hadoopConf.set("fs.defaultFS", hdfsDefaultFs);

        oozieLocalCluster = new OozieLocalServer.Builder()
                .setOozieTestDir(oozieTestDir)
                .setOozieHomeDir(oozieHomeDir)
                .setOozieUsername(oozieUsername)
                .setOozieGroupname(oozieGroupname)
                .setOozieYarnResourceManagerAddress(oozieYarnResourceManagerAddress)
                .setOozieHdfsDefaultFs(hdfsDefaultFs)
                .setOozieConf(hadoopConf)
                .setOozieHdfsShareLibDir(oozieHdfsShareLibDir)
                .setOozieShareLibCreate(Boolean.TRUE)
                .setOozieLocalShareLibCacheDir(oozieLocalShareLibCacheDir)
                .setOoziePurgeLocalShareLibCache(Boolean.FALSE)
                .setOozieShareLibFrameworks(
                        oozieShareLibFrameworks)
                .setOoziePort(ooziePort)
                .setOozieHost(oozieHost)
                .build();

        createShareLib();
    }

    private void loadConfig() throws BootstrapException, NotFoundServiceException {
        oozieTestDir = getTmpDirPath(configuration, OozieConfig.OOZIE_TEST_DIR_KEY);
        oozieHomeDir = configuration.getString(OozieConfig.OOZIE_HOME_DIR_KEY);
        oozieUsername = System.getProperty("user.name");
        oozieGroupname = configuration.getString(OozieConfig.OOZIE_GROUPNAME_KEY);
        oozieYarnResourceManagerAddress = configuration.getString(YarnConfig.YARN_RESOURCE_MANAGER_ADDRESS_CLIENT_KEY);

        oozieHdfsShareLibDir = configuration.getString(OozieConfig.OOZIE_HDFS_SHARE_LIB_DIR_KEY);
        oozieShareLibCreate = configuration.getBoolean(OozieConfig.OOZIE_SHARE_LIB_CREATE_KEY);
        oozieLocalShareLibCacheDir = configuration.getString(OozieConfig.OOZIE_LOCAL_SHARE_LIB_CACHE_DIR_KEY);
        ooziePurgeLocalShareLibCache = configuration.getBoolean(OozieConfig.OOZIE_PURGE_LOCAL_SHARE_LIB_CACHE_KEY);

        oozieTmpDir = getTmpDirPath(configuration, OozieConfig.OOZIE_TMP_DIR_KEY);

        resourceManagerWebappAddress = configuration.getString(YarnConfig.YARN_RESOURCE_MANAGER_WEBAPP_ADDRESS_CLIENT_KEY);

        ooziePort = configuration.getInt(OozieConfig.OOZIE_PORT);
        oozieHost = configuration.getString(OozieConfig.OOZIE_HOST);

        oozieShareLibPath = HadoopUtils.resolveDir(configuration.getString(OozieConfig.OOZIE_SHARELIB_PATH_KEY));
        oozieShareLibName = configuration.getString(OozieConfig.OOZIE_SHARELIB_NAME_KEY);

        List<Object> frameworks = configuration.getList(OozieConfig.OOZIE_SHARE_LIB_COMPONENT_KEY);
        oozieShareLibFrameworks = frameworks.stream().map(f -> Framework.valueOf(f.toString())).collect(Collectors.toList());
    }

    @Override
    public void loadConfig(Map<String, String> configs) {
        if (StringUtils.isNotEmpty(configs.get(OozieConfig.OOZIE_TEST_DIR_KEY))) {
            oozieTestDir = getTmpDirPath(configs, OozieConfig.OOZIE_TEST_DIR_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(OozieConfig.OOZIE_HOME_DIR_KEY))) {
            oozieHomeDir = configs.get(OozieConfig.OOZIE_HOME_DIR_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(OozieConfig.OOZIE_GROUPNAME_KEY))) {
            oozieGroupname = configs.get(OozieConfig.OOZIE_GROUPNAME_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(YarnConfig.YARN_RESOURCE_MANAGER_ADDRESS_CLIENT_KEY))) {
            oozieYarnResourceManagerAddress = configs.get(YarnConfig.YARN_RESOURCE_MANAGER_ADDRESS_CLIENT_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(OozieConfig.OOZIE_HDFS_SHARE_LIB_DIR_KEY))) {
            oozieHdfsShareLibDir = configs.get(OozieConfig.OOZIE_HDFS_SHARE_LIB_DIR_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(OozieConfig.OOZIE_SHARE_LIB_CREATE_KEY))) {
            oozieShareLibCreate = Boolean.parseBoolean(configs.get(OozieConfig.OOZIE_SHARE_LIB_CREATE_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(OozieConfig.OOZIE_LOCAL_SHARE_LIB_CACHE_DIR_KEY))) {
            oozieLocalShareLibCacheDir = configs.get(OozieConfig.OOZIE_LOCAL_SHARE_LIB_CACHE_DIR_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(OozieConfig.OOZIE_PURGE_LOCAL_SHARE_LIB_CACHE_KEY))) {
            ooziePurgeLocalShareLibCache = Boolean.parseBoolean(configs.get(OozieConfig.OOZIE_PURGE_LOCAL_SHARE_LIB_CACHE_KEY));
        }

        if (StringUtils.isNotEmpty(configs.get(OozieConfig.OOZIE_TMP_DIR_KEY))) {
            oozieTmpDir = getTmpDirPath(configs, OozieConfig.OOZIE_TMP_DIR_KEY);
        }

        if (StringUtils.isNotEmpty(configs.get(YarnConfig.YARN_RESOURCE_MANAGER_WEBAPP_ADDRESS_CLIENT_KEY))) {
            resourceManagerWebappAddress = configs.get(YarnConfig.YARN_RESOURCE_MANAGER_WEBAPP_ADDRESS_CLIENT_KEY);
        }

        if (StringUtils.isNotEmpty(configs.get(OozieConfig.OOZIE_PORT))) {
            ooziePort = Integer.parseInt(configs.get(OozieConfig.OOZIE_PORT));
        }
        if (StringUtils.isNotEmpty(configs.get(OozieConfig.OOZIE_HOST))) {
            oozieHost = configs.get(OozieConfig.OOZIE_HOST);
        }

        if (StringUtils.isNotEmpty(configs.get(OozieConfig.OOZIE_SHARELIB_PATH_KEY))) {
            oozieShareLibPath = HadoopUtils.resolveDir(configs.get(OozieConfig.OOZIE_SHARELIB_PATH_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(OozieConfig.OOZIE_SHARELIB_NAME_KEY))) {
            oozieShareLibName = configs.get(OozieConfig.OOZIE_SHARELIB_NAME_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(OozieConfig.OOZIE_SHARE_LIB_COMPONENT_KEY))) {
            List<String> frameworks = Arrays.asList(configs.get(OozieConfig.OOZIE_SHARE_LIB_COMPONENT_KEY).split(","));
            oozieShareLibFrameworks = frameworks.stream().map(f -> Framework.valueOf(f)).collect(Collectors.toList());
        }
    }

    @Override
    public Bootstrap start() {
        if (state == State.STOPPED) {
            state = State.STARTING;
            LOGGER.info("{} is starting", this.getClass().getName());
            init();
            try {
                build();
            } catch (NotFoundServiceException e) {
                LOGGER.error("unable to add oozie", e);
            }
            try {
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
                cleanup();
            } catch (Exception e) {
                LOGGER.error("unable to stop oozie", e);
            }
            state = State.STOPPED;
            LOGGER.info("{} is stopped", this.getClass().getName());
        }
        return this;

    }

    private void cleanup() {
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

            Paths.get(oozieTmpDir).toFile().mkdirs();
            final String fullOozieTarFilePath = oozieShareLibPath + Path.SEPARATOR + oozieShareLibName;

            try {

                // Get and extract the oozie release
                String oozieExtractTempDir = extractOozieTarFileToTempDir(new File(fullOozieTarFilePath));

                // Extract the sharelib tarball to a temp dir
                String fullOozieShareLibTarFilePath = oozieExtractTempDir + Path.SEPARATOR + "oozie-" + getOozieVersionFromOozieTarFileName() + Path.SEPARATOR + "oozie-sharelib-" + getOozieVersionFromOozieTarFileName() + ".tar.gz";
                String oozieShareLibExtractTempDir = extractOozieShareLibTarFileToTempDir(new File(fullOozieShareLibTarFilePath));

                // Copy the sharelib into HDFS
                Path destPath = new Path(oozieHdfsShareLibDir + Path.SEPARATOR + "oozie" + Path.SEPARATOR + SHARE_LIB_PREFIX + getTimestampDirectory());
                LOGGER.info("OOZIE: Writing share lib contents to: {}", destPath);

                FileSystem hdfsFileSystem = null;
                org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
                conf.set("fs.default.name", hdfsDefaultFs);
                conf.set("oozie.service.WorkflowAppService.system.libpath", hdfsDefaultFs + "/" + oozieHdfsShareLibDir);
                conf.set("hadoop.proxyuser." + System.getProperty("user.name") + ".hosts", "*");
                conf.set("hadoop.proxyuser." + System.getProperty("user.name") + ".groups", "*");

                URI uri = URI.create(hdfsDefaultFs);
                try {
                    hdfsFileSystem = FileSystem.get(uri, conf);
                } catch (IOException e) {
                    LOGGER.error("unable to create FileSystem", e);
                }
                hdfsFileSystem.copyFromLocalFile(false, new Path(new File(oozieShareLibExtractTempDir).toURI()), destPath);

                if (ooziePurgeLocalShareLibCache) {
                    org.apache.commons.io.FileUtils.deleteDirectory(new File(oozieShareLibExtractTempDir));
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public String extractOozieShareLibTarFileToTempDir(File fullOozieShareLibTarFilePath) throws IOException {
        File tempDir = File.createTempFile(SHARE_LIB_LOCAL_TEMP_PREFIX, "");
        tempDir.delete();
        tempDir.mkdir();
        tempDir.deleteOnExit();

        FileUtil.unTar(fullOozieShareLibTarFilePath, tempDir);

        // Remove shared lib to try to get the CP down.
        if (oozieShareLibFrameworks != null || !oozieShareLibFrameworks.isEmpty()) {
            Arrays.stream(Framework.values()).forEach(framework -> {
                if (!oozieShareLibFrameworks.contains(framework)) {
                    LOGGER.info("OOZIE: Excluding framework " + framework.getValue() + " from shared lib.");
                    File removeShareLibDir = new File(tempDir.getAbsolutePath() + "/share/lib/" + framework.getValue());
                    if (removeShareLibDir.isDirectory()) {
                        try {
                            org.apache.commons.io.FileUtils.deleteDirectory(removeShareLibDir);
                        } catch (IOException e) {
                            LOGGER.error("unable to delete directory {}", removeShareLibDir);
                        }
                    }
                }
            });

        }
        return tempDir.getAbsolutePath();
    }


    public String extractOozieTarFileToTempDir(File fullOozieTarFilePath) throws IOException {
        File tempDir = File.createTempFile(OozieConfig.SHARE_LIB_LOCAL_TEMP_PREFIX, "", Paths.get(oozieTmpDir).toFile());
        tempDir.delete();
        tempDir.mkdir();
        tempDir.deleteOnExit();

        FileUtil.unTar(fullOozieTarFilePath, tempDir);

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
