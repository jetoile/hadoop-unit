package fr.jetoile.hadoopunit.component;

import com.github.sakserv.minicluster.impl.YarnLocalCluster;
import com.github.sakserv.minicluster.util.FileUtils;
import com.github.sakserv.minicluster.yarn.InJvmContainerExecutor;
import fr.jetoile.hadoopunit.Component;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import fr.jetoile.hadoopunit.exception.NotFoundServiceException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;

public class YarnBootstrap implements BootstrapHadoop {
    final public static String NAME = Component.YARN.name();

    final private Logger LOGGER = LoggerFactory.getLogger(YarnBootstrap.class);

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

    public YarnBootstrap() {
        if (yarnLocalCluster == null) {
            try {
                loadConfig();
            } catch (BootstrapException | NotFoundServiceException e) {
                LOGGER.error("unable to load configuration", e);
            }
        }
    }

    private void init() {

    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getProperties() {
        return "[" +
                "RM address:" + yarnRMAddress +
                ", RM Scheduler address:" + yarnRMSchedulerAddress +
                ", RM Resource Tracker address:" + yarnRMResourceTrackerAddress +
                ", RM Webapp address:" + yarnRMWebappAddress +
                ", InJvmContainer:" + inJvmContainer +
                "]";
    }

    private void build() throws NotFoundServiceException {
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
                .setConfig(new org.apache.hadoop.conf.Configuration())
                .build();

    }

    private void loadConfig() throws BootstrapException, NotFoundServiceException {

        try {
            configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }

        yarnNumNodeManagers = configuration.getInt(HadoopUnitConfig.YARN_NUM_NODE_MANAGERS_KEY);
        yarnNumLocalDirs = configuration.getInt(HadoopUnitConfig.YARN_NUM_LOCAL_DIRS_KEY);
        yarnNumLogDirs = configuration.getInt(HadoopUnitConfig.YARN_NUM_LOG_DIRS_KEY);
        yarnRMAddress = configuration.getString(HadoopUnitConfig.YARN_RESOURCE_MANAGER_ADDRESS_KEY);
        yarnRMHostname = configuration.getString(HadoopUnitConfig.YARN_RESOURCE_MANAGER_HOSTNAME_KEY);
        yarnRMSchedulerAddress = configuration.getString(HadoopUnitConfig.YARN_RESOURCE_MANAGER_SCHEDULER_ADDRESS_KEY);
        yarnRMResourceTrackerAddress = configuration.getString(HadoopUnitConfig.YARN_RESOURCE_MANAGER_RESOURCE_TRACKER_ADDRESS_KEY);
        yarnRMWebappAddress = configuration.getString(HadoopUnitConfig.YARN_RESOURCE_MANAGER_WEBAPP_ADDRESS_KEY);
        inJvmContainer = configuration.getBoolean(HadoopUnitConfig.YARN_USE_IN_JVM_CONTAINER_EXECUTOR_KEY);
    }

    @Override
    public void loadConfig(Map<String, String> configs) {
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.YARN_NUM_NODE_MANAGERS_KEY))) {
            yarnNumNodeManagers = Integer.parseInt(configs.get(HadoopUnitConfig.YARN_NUM_NODE_MANAGERS_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.YARN_NUM_LOCAL_DIRS_KEY))) {
            yarnNumLocalDirs = Integer.parseInt(configs.get(HadoopUnitConfig.YARN_NUM_LOCAL_DIRS_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.YARN_NUM_LOG_DIRS_KEY))) {
            yarnNumLogDirs = Integer.parseInt(configs.get(HadoopUnitConfig.YARN_NUM_LOG_DIRS_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.YARN_RESOURCE_MANAGER_ADDRESS_KEY))) {
            yarnRMAddress = configs.get(HadoopUnitConfig.YARN_RESOURCE_MANAGER_ADDRESS_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.YARN_RESOURCE_MANAGER_HOSTNAME_KEY))) {
            yarnRMHostname = configs.get(HadoopUnitConfig.YARN_RESOURCE_MANAGER_HOSTNAME_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.YARN_RESOURCE_MANAGER_SCHEDULER_ADDRESS_KEY))) {
            yarnRMSchedulerAddress = configs.get(HadoopUnitConfig.YARN_RESOURCE_MANAGER_SCHEDULER_ADDRESS_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.YARN_RESOURCE_MANAGER_RESOURCE_TRACKER_ADDRESS_KEY))) {
            yarnRMResourceTrackerAddress = configs.get(HadoopUnitConfig.YARN_RESOURCE_MANAGER_RESOURCE_TRACKER_ADDRESS_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.YARN_RESOURCE_MANAGER_WEBAPP_ADDRESS_KEY))) {
            yarnRMWebappAddress = configs.get(HadoopUnitConfig.YARN_RESOURCE_MANAGER_WEBAPP_ADDRESS_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.YARN_USE_IN_JVM_CONTAINER_EXECUTOR_KEY))) {
            inJvmContainer = Boolean.parseBoolean(configs.get(HadoopUnitConfig.YARN_USE_IN_JVM_CONTAINER_EXECUTOR_KEY));
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
