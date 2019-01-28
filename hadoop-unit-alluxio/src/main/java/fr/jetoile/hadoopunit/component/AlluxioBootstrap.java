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

import alluxio.PropertyKey;
import alluxio.master.LocalAlluxioCluster;
import alluxio.security.LoginUserTestUtils;
import alluxio.security.authentication.AuthenticatedClientUser;
import fr.jetoile.hadoopunit.ComponentMetadata;
import fr.jetoile.hadoopunit.HadoopUtils;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class AlluxioBootstrap implements Bootstrap {
    static final private Logger LOGGER = LoggerFactory.getLogger(AlluxioBootstrap.class);

    private LocalAlluxioCluster alluxioLocalCluster;
    private Map<PropertyKey, String> configMap = new HashMap<>();
    private org.apache.commons.configuration.Configuration configuration;

    private String workDirectory;
    private String hostname;
    private int masterRpcPort;
    private int masterWebPort;
    private int proxyWebPort;
    private int workerRpcPort;
    private int workerDataPort;
    private int workerWebPort;
    private String webappDirectory;


    private State state = State.STOPPED;

    public AlluxioBootstrap() {
        try {
            configuration = HadoopUtils.INSTANCE.loadConfigFile(null);
            loadConfig();
        } catch (BootstrapException e) {
            LOGGER.error("unable to load configuration", e);
        }
    }

    public AlluxioBootstrap(URL url) {
        try {
            configuration = HadoopUtils.INSTANCE.loadConfigFile(url);
            loadConfig();
        } catch (BootstrapException e) {
            LOGGER.error("unable to load configuration", e);
        }
    }

    @Override
    public ComponentMetadata getMetadata() {
        return new AlluxioMetadata();
    }

    @Override
    public String getProperties() {
        return "\n \t\t\t ip:" + hostname +
                "\n \t\t\t port:" + masterRpcPort +
                "\n \t\t\t WebPort:" + masterWebPort +
                "\n \t\t\t RpcPort:" + masterRpcPort;
    }

    private void init() {
        configMap.put(PropertyKey.WORK_DIR, workDirectory);
        configMap.put(PropertyKey.MASTER_HOSTNAME, hostname);
        configMap.put(PropertyKey.MASTER_BIND_HOST, hostname);
        configMap.put(PropertyKey.MASTER_WEB_BIND_HOST, hostname);
        configMap.put(PropertyKey.WORKER_BIND_HOST, hostname);
        configMap.put(PropertyKey.WORKER_DATA_BIND_HOST, hostname);
        configMap.put(PropertyKey.WORKER_WEB_BIND_HOST, hostname);

        configMap.put(PropertyKey.MASTER_RPC_PORT, String.valueOf(masterRpcPort));
        configMap.put(PropertyKey.MASTER_WEB_PORT, String.valueOf(masterWebPort));
        configMap.put(PropertyKey.PROXY_WEB_PORT, String.valueOf(proxyWebPort));
        configMap.put(PropertyKey.WORKER_RPC_PORT, String.valueOf(workerRpcPort));
        configMap.put(PropertyKey.WORKER_DATA_PORT, String.valueOf(workerDataPort));
        configMap.put(PropertyKey.WORKER_WEB_PORT, String.valueOf(workerWebPort));

        configMap.put(PropertyKey.WEB_RESOURCES, webappDirectory);

    }

    private void build() {
        AuthenticatedClientUser.remove();
        LoginUserTestUtils.resetLoginUser();

        alluxioLocalCluster = new LocalAlluxioCluster(1);
        try {
            alluxioLocalCluster.initConfiguration();

            for (Map.Entry<PropertyKey, String> entry : configMap.entrySet()) {
                alluxio.Configuration.set(entry.getKey(), entry.getValue());
            }
        } catch (IOException e) {
            LOGGER.error("unable to init configuration for alluxio", e);
        }
    }

    private void loadConfig() throws BootstrapException {
        this.workDirectory = configuration.getString(AlluxioConfig.ALLUXIO_WORK_DIR);
        this.hostname = configuration.getString(AlluxioConfig.ALLUXIO_HOSTNAME);
        this.masterRpcPort = configuration.getInt(AlluxioConfig.ALLUXIO_MASTER_RPC_PORT);
        this.masterWebPort = configuration.getInt(AlluxioConfig.ALLUXIO_MASTER_WEB_PORT);
        this.proxyWebPort = configuration.getInt(AlluxioConfig.ALLUXIO_PROXY_WEB_PORT);
        this.workerRpcPort = configuration.getInt(AlluxioConfig.ALLUXIO_WORKER_RPC_PORT);
        this.workerDataPort = configuration.getInt(AlluxioConfig.ALLUXIO_WORKER_DATA_PORT);
        this.workerWebPort = configuration.getInt(AlluxioConfig.ALLUXIO_WORKER_WEB_PORT);
        this.webappDirectory = configuration.getString(AlluxioConfig.ALLUXIO_WEBAPP_DIRECTORY);
    }

    @Override
    public void loadConfig(Map<String, String> configs) {
        if (StringUtils.isNotEmpty(configs.get(AlluxioConfig.ALLUXIO_WORK_DIR))) {
            this.workDirectory = configuration.getString(AlluxioConfig.ALLUXIO_WORK_DIR);
        }
        if (StringUtils.isNotEmpty(configs.get(AlluxioConfig.ALLUXIO_HOSTNAME))) {
            this.hostname = configuration.getString(AlluxioConfig.ALLUXIO_HOSTNAME);
        }
        if (StringUtils.isNotEmpty(configs.get(AlluxioConfig.ALLUXIO_MASTER_RPC_PORT))) {
            this.masterRpcPort = configuration.getInt(AlluxioConfig.ALLUXIO_MASTER_RPC_PORT);
        }
        if (StringUtils.isNotEmpty(configs.get(AlluxioConfig.ALLUXIO_MASTER_WEB_PORT))) {
            this.masterWebPort = configuration.getInt(AlluxioConfig.ALLUXIO_MASTER_WEB_PORT);
        }
        if (StringUtils.isNotEmpty(configs.get(AlluxioConfig.ALLUXIO_PROXY_WEB_PORT))) {
            this.proxyWebPort = configuration.getInt(AlluxioConfig.ALLUXIO_PROXY_WEB_PORT);
        }
        if (StringUtils.isNotEmpty(configs.get(AlluxioConfig.ALLUXIO_WORKER_RPC_PORT))) {
            this.workerRpcPort = configuration.getInt(AlluxioConfig.ALLUXIO_WORKER_RPC_PORT);
        }
        if (StringUtils.isNotEmpty(configs.get(AlluxioConfig.ALLUXIO_WORKER_DATA_PORT))) {
            this.workerDataPort = configuration.getInt(AlluxioConfig.ALLUXIO_WORKER_DATA_PORT);
        }
        if (StringUtils.isNotEmpty(configs.get(AlluxioConfig.ALLUXIO_WORKER_WEB_PORT))) {
            this.workerWebPort = configuration.getInt(AlluxioConfig.ALLUXIO_WORKER_WEB_PORT);
        }
        if (StringUtils.isNotEmpty(configs.get(AlluxioConfig.ALLUXIO_HOSTNAME))) {
            this.webappDirectory = configuration.getString(AlluxioConfig.ALLUXIO_WEBAPP_DIRECTORY);
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
                alluxioLocalCluster.start();
            } catch (Exception e) {
                LOGGER.error("unable to add alluxio", e);
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
            } catch (Exception e) {
                LOGGER.error("unable to stop hdfs", e);
            }
            state = State.STOPPED;
            LOGGER.info("{} is stopped", this.getClass().getName());
        }
        return this;
    }

}
