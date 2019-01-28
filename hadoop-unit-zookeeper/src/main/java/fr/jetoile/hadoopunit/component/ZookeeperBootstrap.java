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

import com.github.sakserv.minicluster.impl.ZookeeperLocalCluster;
import fr.jetoile.hadoopunit.*;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import fr.jetoile.hadoopunit.exception.NotFoundServiceException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class ZookeeperBootstrap implements Bootstrap {
    final private static Logger LOGGER = LoggerFactory.getLogger(ZookeeperBootstrap.class);

    private ZookeeperLocalCluster zookeeperLocalCluster;

    private State state = State.STOPPED;

    private Configuration configuration;

    private int port;
    private String localDir;
    private String host;

    public ZookeeperBootstrap() {
        if (zookeeperLocalCluster == null) {
            try {
                configuration = HadoopUtils.INSTANCE.loadConfigFile(null);
                loadConfig();
            } catch (BootstrapException e) {
                LOGGER.error("unable to load configuration", e);
            }
        }
    }

    public ZookeeperBootstrap(URL url) {
        if (zookeeperLocalCluster == null) {
            try {
                configuration = HadoopUtils.INSTANCE.loadConfigFile(url);
                loadConfig();
            } catch (BootstrapException e) {
                LOGGER.error("unable to load configuration", e);
            }
        }
    }

    @Override
    public ComponentMetadata getMetadata() {
        return new ZookeeperMetadata();
    }

    @Override
    public String getProperties() {
        return "\n \t\t\t host:" + host +
                "\n \t\t\t port:" + port;
    }

    private void loadConfig() throws BootstrapException {
        port = configuration.getInt(ZookeeperConfig.ZOOKEEPER_PORT_KEY);
        localDir = configuration.getString(ZookeeperConfig.ZOOKEEPER_TEMP_DIR_KEY);
        host = configuration.getString(ZookeeperConfig.ZOOKEEPER_HOST_KEY);

    }

    @Override
    public void loadConfig(Map<String, String> configs) {
        if (StringUtils.isNotEmpty(configs.get(ZookeeperConfig.ZOOKEEPER_PORT_KEY))) {
            port = Integer.parseInt(configs.get(ZookeeperConfig.ZOOKEEPER_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(ZookeeperConfig.ZOOKEEPER_TEMP_DIR_KEY))) {
            localDir = configs.get(ZookeeperConfig.ZOOKEEPER_TEMP_DIR_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(ZookeeperConfig.ZOOKEEPER_HOST_KEY))) {
            host = configs.get(ZookeeperConfig.ZOOKEEPER_HOST_KEY);
        }
    }

    private void init() {
        Path path = Paths.get(localDir);
        try {
            Files.createDirectories(path);
        } catch (IOException e) {
            LOGGER.error("unable to create mandatory directory", e);
        }

    }

    private void build() {
        zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
                .setPort(port)
                .setZookeeperConnectionString(host + ":" + port)
                .setTempDir(localDir)
                .build();
    }

    @Override
    public Bootstrap start() {
        if (state == State.STOPPED) {
            state = State.STARTING;
            LOGGER.info("{} is starting", this.getClass().getName());
            init();
            build();
            try {
                zookeeperLocalCluster.start();
            } catch (Exception e) {
                LOGGER.error("unable to add zookeeper", e);
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
                zookeeperLocalCluster.stop(true);
            } catch (Exception e) {
                LOGGER.error("unable to stop zookeeper", e);
            }
            state = State.STOPPED;
            LOGGER.info("{} is stopped", this.getClass().getName());
        }
        return this;
    }
}
