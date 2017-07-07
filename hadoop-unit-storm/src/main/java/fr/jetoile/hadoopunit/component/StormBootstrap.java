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

import com.github.sakserv.minicluster.impl.StormLocalCluster;
import fr.jetoile.hadoopunit.Component;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.storm.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class StormBootstrap implements Bootstrap {
    final public static String NAME = Component.STORM.name();

    static final private Logger LOGGER = LoggerFactory.getLogger(StormBootstrap.class);

    private State state = State.STOPPED;

    private org.apache.commons.configuration.Configuration configuration;

    private StormLocalCluster stormLocalCluster;


    private long zkPort;
    private String zkHost;
    private boolean debug;
    private int nbWorker;
    private String host;
    private int port;
    private String workingDir;

    public StormBootstrap() {
        try {
            loadConfig();
        } catch (BootstrapException e) {
            LOGGER.error("unable to load configuration", e);
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getProperties() {
        return "[" +
                "host:" + host +
                ", port:" + port +
                ", workingDir:" + workingDir +
                ", debug:" + debug +
                ", nbWorker:" + nbWorker +
                "]";
    }

    private void loadConfig() throws BootstrapException {
        try {
            configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }

        host = configuration.getString(HadoopUnitConfig.STORM_HOST_KEY);
        port = configuration.getInt(HadoopUnitConfig.STORM_PORT_KEY);
        workingDir = configuration.getString(HadoopUnitConfig.STORM_WORKING_DIR_KEY);
        zkPort = configuration.getLong(HadoopUnitConfig.ZOOKEEPER_PORT_KEY);
        zkHost = configuration.getString(HadoopUnitConfig.ZOOKEEPER_HOST_KEY);
        debug = configuration.getBoolean(HadoopUnitConfig.STORM_DEBUG_KEY);
        nbWorker = configuration.getInt(HadoopUnitConfig.STORM_NB_WORKER_KEY);
    }

    @Override
    public void loadConfig(Map<String, String> configs) {
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.STORM_PORT_KEY))) {
            port = Integer.parseInt(configs.get(HadoopUnitConfig.STORM_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.STORM_HOST_KEY))) {
            host = configs.get(HadoopUnitConfig.STORM_HOST_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.STORM_WORKING_DIR_KEY))) {
            workingDir = configs.get(HadoopUnitConfig.STORM_WORKING_DIR_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.ZOOKEEPER_PORT_KEY))) {
            zkPort = Long.parseLong(configs.get(HadoopUnitConfig.ZOOKEEPER_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.ZOOKEEPER_HOST_KEY))) {
            zkHost = configs.get(HadoopUnitConfig.ZOOKEEPER_HOST_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.STORM_NB_WORKER_KEY))) {
            nbWorker = Integer.parseInt(configs.get(HadoopUnitConfig.STORM_NB_WORKER_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.STORM_DEBUG_KEY))) {
            debug = Boolean.parseBoolean(configs.get(HadoopUnitConfig.STORM_DEBUG_KEY));
        }
    }

    public void build() {
        stormLocalCluster = new StormLocalCluster.Builder()
                .setHost(host)
                .setPort(port)
                .setWorkingDir(workingDir)
                .setZookeeperHost(zkHost)
                .setZookeeperPort(zkPort)
                .setEnableDebug(debug)
                .setNumWorkers(nbWorker)
                .setStormConfig(new Config())
                .build();
    }

    @Override
    public Bootstrap start() {
        if (state == State.STOPPED) {
            state = State.STARTING;
            LOGGER.info("{} is starting", this.getClass().getName());
            try {
                build();
                stormLocalCluster.start();
            } catch (Exception e) {
                LOGGER.error("unable to add storm", e);
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
                stormLocalCluster.stop();
                stormLocalCluster.cleanUp();
            } catch (Exception e) {
                LOGGER.error("unable to stop storm", e);
            }
            state = State.STOPPED;
            LOGGER.info("{} is stopped", this.getClass().getName());
        }
        return this;
    }

    @Override
    public Configuration getConfiguration() {
        throw new UnsupportedOperationException("the method getConfiguration can not be called on StormBootstrap");
    }

    public StormLocalCluster getStormLocalCluster() {
        return stormLocalCluster;
    }

}
