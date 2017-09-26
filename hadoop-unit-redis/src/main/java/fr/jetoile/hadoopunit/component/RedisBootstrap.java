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

import fr.jetoile.hadoopunit.Component;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.HadoopUtils;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import fr.jetoile.hadoopunit.redis.EmbeddedRedisInstaller;
import fr.jetoile.hadoopunit.redis.RedisType;
import net.ishiis.redis.unit.RedisCluster;
import net.ishiis.redis.unit.RedisMasterSlave;
import net.ishiis.redis.unit.RedisSentinel;
import net.ishiis.redis.unit.RedisServer;
import net.ishiis.redis.unit.config.RedisClusterConfig;
import net.ishiis.redis.unit.config.RedisMasterSlaveConfig;
import net.ishiis.redis.unit.config.RedisSentinelConfig;
import net.ishiis.redis.unit.config.RedisServerConfig;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RedisBootstrap implements Bootstrap {
    final public static String NAME = Component.REDIS.name();

    final private Logger LOGGER = LoggerFactory.getLogger(RedisBootstrap.class);

    private int masterPort;
    private String downloadUrl;
    private String version;
    private boolean cleanupInstallation;
    private RedisType type = RedisType.SERVER;
    private List<Integer> slavePorts = new ArrayList<>();
    private List<Integer> sentinelPorts = new ArrayList<>();
    private String tmpDir;

    private RedisServer redisServer;
    private RedisCluster redisCluster;
    private RedisMasterSlave redisMasterSlave;

    private RedisSentinel redisSentinel;
    private Configuration configuration;
    private State state = State.STOPPED;


    public RedisBootstrap() {

        if (!System.getProperty("os.name").startsWith("Windows")) {
            try {
                configuration = HadoopUtils.INSTANCE.loadConfigFile(null);
                loadConfig();
            } catch (BootstrapException e) {
                LOGGER.error("unable to load configuration", e);
            }
        } else {
            throw new IllegalArgumentException("Sorry redis is not supported on windows...");
        }
    }

    public RedisBootstrap(URL url) {

        if (!System.getProperty("os.name").startsWith("Windows")) {
            try {
                configuration = HadoopUtils.INSTANCE.loadConfigFile(url);
                loadConfig();
            } catch (BootstrapException e) {
                LOGGER.error("unable to load configuration", e);
            }
        } else {
            throw new IllegalArgumentException("Sorry redis is not supported on windows...");
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getProperties() {
        return
 			 "\n \t\t\t masterPort:" + masterPort +
                "\n \t\t\t version:" + version +
                "\n \t\t\t type:" + type.name() +
                (slavePorts.size() != 0 && type!=RedisType.SERVER ? "\n \t\t\t slavePorts: " + slavePorts : "") +
                (sentinelPorts.size() != 0 && type == RedisType.SENTINEL ? "\n \t\t\t sentinelPorts: " + sentinelPorts : "");
    }

    private void loadConfig() throws BootstrapException {
        masterPort = configuration.getInt(HadoopUnitConfig.REDIS_PORT_KEY);
        version = configuration.getString(HadoopUnitConfig.REDIS_VERSION_KEY);
        downloadUrl = configuration.getString(HadoopUnitConfig.REDIS_DOWNLOAD_URL_KEY);
        cleanupInstallation = configuration.getBoolean(HadoopUnitConfig.REDIS_CLEANUP_INSTALLATION_KEY);
        type = RedisType.valueOf(configuration.getString(HadoopUnitConfig.REDIS_TYPE_KEY, RedisType.SERVER.name()));
        tmpDir = configuration.getString(HadoopUnitConfig.REDIS_TMP_DIR_KEY);

        if (configuration.containsKey(HadoopUnitConfig.REDIS_SLAVE_PORT_KEY)) {
            slavePorts = configuration.getList(HadoopUnitConfig.REDIS_SLAVE_PORT_KEY).stream().map(s -> Integer.valueOf(((String) s).trim())).collect(Collectors.toList());
        }
        if (configuration.containsKey(HadoopUnitConfig.REDIS_SENTINEL_PORT_KEY)) {
            sentinelPorts = configuration.getList(HadoopUnitConfig.REDIS_SENTINEL_PORT_KEY).stream().map(s -> Integer.valueOf(((String) s).trim())).collect(Collectors.toList());
        }

    }

    @Override
    public void loadConfig(Map<String, String> configs) {
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.REDIS_PORT_KEY))) {
            masterPort = Integer.parseInt(configs.get(HadoopUnitConfig.REDIS_PORT_KEY));
        }


        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.REDIS_PORT_KEY))) {
            version = configs.get(HadoopUnitConfig.REDIS_VERSION_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.REDIS_DOWNLOAD_URL_KEY))) {
            downloadUrl = configs.get(HadoopUnitConfig.REDIS_DOWNLOAD_URL_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.REDIS_CLEANUP_INSTALLATION_KEY))) {
            cleanupInstallation = Boolean.parseBoolean(configs.get(HadoopUnitConfig.REDIS_CLEANUP_INSTALLATION_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.REDIS_TYPE_KEY))) {
            type = RedisType.valueOf(configs.get(HadoopUnitConfig.REDIS_TYPE_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.REDIS_TMP_DIR_KEY))) {
            tmpDir = configs.get(HadoopUnitConfig.REDIS_TMP_DIR_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.REDIS_SLAVE_PORT_KEY))) {
            List<String> ports = Arrays.asList(configs.get(HadoopUnitConfig.REDIS_SLAVE_PORT_KEY).split(","));
            slavePorts = ports.stream().map(s -> Integer.valueOf(s.trim())).collect(Collectors.toList());
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.REDIS_SENTINEL_PORT_KEY))) {
            List<String> ports = Arrays.asList(configs.get(HadoopUnitConfig.REDIS_SENTINEL_PORT_KEY).split(","));
            sentinelPorts = ports.stream().map(s -> Integer.valueOf(s.trim())).collect(Collectors.toList());
        }
    }

    private void build() {
        EmbeddedRedisInstaller installer = EmbeddedRedisInstaller.builder()
                .downloadUrl(downloadUrl)
                .forceCleanupInstallationDirectory(cleanupInstallation)
                .version(version)
                .tmpDir(tmpDir)
                .build();

        try {
            installer.install();
        } catch (IOException | InterruptedException e) {
            LOGGER.error("unable to install redis", e);
        }

        switch (type) {
            case SERVER:
                redisServer = new RedisServer(
                        new RedisServerConfig.ServerBuilder(masterPort)
                                .redisBinaryPath(installer.getExecutableFile().getAbsolutePath())
                                .build()
                );
                break;
            case CLUSTER:
                redisCluster = new RedisCluster(
                        slavePorts.stream().map(p ->
                                new RedisClusterConfig.ClusterBuilder(p)
                                        .redisBinaryPath(installer.getExecutableFile().getAbsolutePath())
                                        .build()
                        ).collect(Collectors.toList())
                );
                break;
            case MASTER_SLAVE:
                redisMasterSlave = new RedisMasterSlave(
                        new RedisMasterSlaveConfig.MasterBuilder(masterPort)
                                .redisBinaryPath(installer.getExecutableFile().getAbsolutePath())
                                .build(),
                        slavePorts.stream().map(p ->
                                new RedisMasterSlaveConfig.SlaveBuilder(p, masterPort)
                                        .redisBinaryPath(installer.getExecutableFile().getAbsolutePath())
                                        .build()
                        ).collect(Collectors.toList())
                );
                break;
            case SENTINEL:
                redisSentinel = new RedisSentinel(
                        new RedisMasterSlaveConfig.MasterBuilder(masterPort)
                                .redisBinaryPath(installer.getExecutableFile().getAbsolutePath())
                                .build(),
                        slavePorts.stream().map(p ->
                                new RedisMasterSlaveConfig.SlaveBuilder(p, masterPort)
                                        .redisBinaryPath(installer.getExecutableFile().getAbsolutePath())
                                        .build()
                        ).collect(Collectors.toList()),
                        sentinelPorts.stream().map(p ->
                                new RedisSentinelConfig.SentinelBuilder(p, masterPort)
                                        .redisBinaryPath(installer.getExecutableFile().getAbsolutePath())
                                        .build()
                        ).collect(Collectors.toList())
                );
                break;
        }
    }

    @Override
    public Bootstrap start() {
        if (state == State.STOPPED) {
            state = State.STARTING;
            LOGGER.info("{} is starting", this.getClass().getName());
            try {
                build();
                switch (type) {
                    case SERVER:
                        redisServer.start();
                        break;
                    case CLUSTER:
                        redisCluster.start();
                        break;
                    case MASTER_SLAVE:
                        redisMasterSlave.start();
                        break;
                    case SENTINEL:
                        redisSentinel.start();
                        break;
                }
            } catch (Exception e) {
                LOGGER.error("unable to add redis", e);
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
                switch (type) {
                    case SERVER:
                        redisServer.stop();
                        break;
                    case CLUSTER:
                        redisCluster.stop();
                        break;
                    case MASTER_SLAVE:
                        redisMasterSlave.stop();
                        break;
                    case SENTINEL:
                        redisSentinel.stop();
                        break;
                }
                cleanup();
            } catch (Exception e) {
                LOGGER.error("unable to stop redis", e);
            }
            state = State.STOPPED;
            LOGGER.info("{} is stopped", this.getClass().getName());
        }
        return this;
    }

    private void cleanup() {
        try {
            FileUtils.deleteDirectory(new File(tmpDir));
        } catch (IOException e) {
            LOGGER.error("unable to delete {}", tmpDir, e);
        }
    }


}
