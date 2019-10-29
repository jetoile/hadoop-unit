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

import com.github.sakserv.minicluster.impl.YarnLocalCluster;
import com.github.sakserv.minicluster.util.FileUtils;
import com.github.sakserv.minicluster.yarn.InJvmContainerExecutor;
import fr.jetoile.hadoopunit.ComponentMetadata;
import fr.jetoile.hadoopunit.HadoopUtils;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import fr.jetoile.hadoopunit.exception.NotFoundServiceException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.util.Map;

public class Yarn3Bootstrap implements BootstrapHadoop3 {
    final private Logger LOGGER = LoggerFactory.getLogger(Yarn3Bootstrap.class);

    private State state = State.STOPPED;

    private Configuration configuration;

    private YarnLocalCluster yarnLocalCluster = null;

    private int yarnNumNodeManagers;
    private int yarnNumLocalDirs;
    private int yarnNumLogDirs;
    private String yarnRMAddress;
    private String yarnRMHostname;
    private String yarnRMSchedulerAddress;
    private String yarnRMResourceTrackerAddress;
    private String yarnRMWebappAddress;
    private boolean inJvmContainer;
    private String jobHistoryAddress;

    public Yarn3Bootstrap() {
        if (yarnLocalCluster == null) {
            try {
                configuration = HadoopUtils.INSTANCE.loadConfigFile(null);
                loadConfig();
            } catch (BootstrapException | NotFoundServiceException e) {
                LOGGER.error("unable to load configuration", e);
            }
        }
    }

    public Yarn3Bootstrap(URL url) {
        if (yarnLocalCluster == null) {
            try {
                configuration = HadoopUtils.INSTANCE.loadConfigFile(url);
                loadConfig();
            } catch (BootstrapException | NotFoundServiceException e) {
                LOGGER.error("unable to load configuration", e);
            }
        }
    }

    private void init() {

    }

    @Override
    public ComponentMetadata getMetadata() {
        return new Yarn3Metadata();
    }

    @Override
    public String getProperties() {
        return "\n \t\t\t RM address:" + yarnRMAddress +
                "\n \t\t\t RM Scheduler address:" + yarnRMSchedulerAddress +
                "\n \t\t\t RM Resource Tracker address:" + yarnRMResourceTrackerAddress +
                "\n \t\t\t RM Webapp address:" + yarnRMWebappAddress +
                "\n \t\t\t jobHistoryAddress" + jobHistoryAddress +
                "\n \t\t\t InJvmContainer:" + inJvmContainer;
    }

    private void build() throws NotFoundServiceException {
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();

        configuration.set(JHAdminConfig.MR_HISTORY_ADDRESS, jobHistoryAddress);
        configuration.set(JHAdminConfig.MR_HISTORY_MINICLUSTER_FIXED_PORTS, "true");
        configuration.set("hadoop.proxyuser." + System.getProperty("user.name") + ".hosts", "*");
        configuration.set("hadoop.proxyuser." + System.getProperty("user.name") + ".groups", "*");

        yarnLocalCluster = new YarnLocalCluster.Builder()
                .setNumNodeManagers(yarnNumNodeManagers)
                .setNumLocalDirs(yarnNumLocalDirs)
                .setNumLogDirs(yarnNumLogDirs)
                .setResourceManagerAddress(yarnRMAddress)
                .setResourceManagerHostname(yarnRMHostname)
                .setResourceManagerSchedulerAddress(yarnRMSchedulerAddress)
                .setResourceManagerResourceTrackerAddress(yarnRMResourceTrackerAddress)
                .setResourceManagerWebappAddress(yarnRMWebappAddress)
                .setUseInJvmContainerExecutor(inJvmContainer)
                .setConfig(configuration)
                .build();

    }

    private void loadConfig() throws BootstrapException, NotFoundServiceException {
        yarnNumNodeManagers = configuration.getInt(Yarn3Config.YARN3_NUM_NODE_MANAGERS_KEY);
        yarnNumLocalDirs = configuration.getInt(Yarn3Config.YARN3_NUM_LOCAL_DIRS_KEY);
        yarnNumLogDirs = configuration.getInt(Yarn3Config.YARN3_NUM_LOG_DIRS_KEY);
        yarnRMAddress = configuration.getString(Yarn3Config.YARN3_RESOURCE_MANAGER_ADDRESS_KEY);
        yarnRMHostname = configuration.getString(Yarn3Config.YARN3_RESOURCE_MANAGER_HOSTNAME_KEY);
        yarnRMSchedulerAddress = configuration.getString(Yarn3Config.YARN3_RESOURCE_MANAGER_SCHEDULER_ADDRESS_KEY);
        yarnRMResourceTrackerAddress = configuration.getString(Yarn3Config.YARN3_RESOURCE_MANAGER_RESOURCE_TRACKER_ADDRESS_KEY);
        yarnRMWebappAddress = configuration.getString(Yarn3Config.YARN3_RESOURCE_MANAGER_WEBAPP_ADDRESS_KEY);
        inJvmContainer = configuration.getBoolean(Yarn3Config.YARN3_USE_IN_JVM_CONTAINER_EXECUTOR_KEY);

        jobHistoryAddress = configuration.getString(Yarn3Config.MR_JOB_HISTORY_ADDRESS_KEY);
    }

    @Override
    public void loadConfig(Map<String, String> configs) {
        if (StringUtils.isNotEmpty(configs.get(Yarn3Config.YARN3_NUM_NODE_MANAGERS_KEY))) {
            yarnNumNodeManagers = Integer.parseInt(configs.get(Yarn3Config.YARN3_NUM_NODE_MANAGERS_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(Yarn3Config.YARN3_NUM_LOCAL_DIRS_KEY))) {
            yarnNumLocalDirs = Integer.parseInt(configs.get(Yarn3Config.YARN3_NUM_LOCAL_DIRS_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(Yarn3Config.YARN3_NUM_LOG_DIRS_KEY))) {
            yarnNumLogDirs = Integer.parseInt(configs.get(Yarn3Config.YARN3_NUM_LOG_DIRS_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(Yarn3Config.YARN3_RESOURCE_MANAGER_ADDRESS_KEY))) {
            yarnRMAddress = configs.get(Yarn3Config.YARN3_RESOURCE_MANAGER_ADDRESS_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(Yarn3Config.YARN3_RESOURCE_MANAGER_HOSTNAME_KEY))) {
            yarnRMHostname = configs.get(Yarn3Config.YARN3_RESOURCE_MANAGER_HOSTNAME_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(Yarn3Config.YARN3_RESOURCE_MANAGER_SCHEDULER_ADDRESS_KEY))) {
            yarnRMSchedulerAddress = configs.get(Yarn3Config.YARN3_RESOURCE_MANAGER_SCHEDULER_ADDRESS_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(Yarn3Config.YARN3_RESOURCE_MANAGER_RESOURCE_TRACKER_ADDRESS_KEY))) {
            yarnRMResourceTrackerAddress = configs.get(Yarn3Config.YARN3_RESOURCE_MANAGER_RESOURCE_TRACKER_ADDRESS_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(Yarn3Config.YARN3_RESOURCE_MANAGER_WEBAPP_ADDRESS_KEY))) {
            yarnRMWebappAddress = configs.get(Yarn3Config.YARN3_RESOURCE_MANAGER_WEBAPP_ADDRESS_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(Yarn3Config.YARN3_USE_IN_JVM_CONTAINER_EXECUTOR_KEY))) {
            inJvmContainer = Boolean.parseBoolean(configs.get(Yarn3Config.YARN3_USE_IN_JVM_CONTAINER_EXECUTOR_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(Yarn3Config.MR_JOB_HISTORY_ADDRESS_KEY))) {
            jobHistoryAddress = configs.get(Yarn3Config.MR_JOB_HISTORY_ADDRESS_KEY);
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
                LOGGER.error("unable to add yarn", e);
            }
            try {
                yarnLocalCluster.start();
            } catch (Exception e) {
                LOGGER.error("unable to add yarn", e);
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
                System.setSecurityManager(new InJvmContainerExecutor.SystemExitAllowSecurityManager());
                yarnLocalCluster.stop();
                cleanup();
            } catch (Exception e) {
                LOGGER.error("unable to stop yarn", e);
            }
            state = State.STOPPED;
            LOGGER.info("{} is stopped", this.getClass().getName());
        }
        return this;
    }

    private void cleanup() {
        if (new File("./target/classes").exists()) {
            FileUtils.deleteFolder("./target/" + getTestName());
        } else {
            FileUtils.deleteFolder("./target");
        }
    }

    @Override
    public org.apache.hadoop.conf.Configuration getConfiguration() {
        return yarnLocalCluster.getConfig();
    }

    public String getTestName() {
        return yarnLocalCluster.getTestName();
    }

}
