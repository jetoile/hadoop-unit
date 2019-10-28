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

import fr.jetoile.hadoopunit.ComponentMetadata;
import fr.jetoile.hadoopunit.HadoopUtils;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.PulsarStandalone;
import org.apache.pulsar.PulsarStandaloneBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

public class PulsarStandaloneBootstrap implements Bootstrap {
    static final private Logger LOGGER = LoggerFactory.getLogger(PulsarStandaloneBootstrap.class);
    private State state = State.STOPPED;
    private Configuration configuration;
    private int port;
    private String ip;
    private String tmpDirPath;

    private String zookeeperDir;
    private int zookeeperPort;

    private PulsarStandalone pulsarStandalone;

    public PulsarStandaloneBootstrap() {
        try {
            configuration = HadoopUtils.INSTANCE.loadConfigFile(null);
            loadConfig();
        } catch (BootstrapException e) {
            LOGGER.error("unable to load configuration", e);
        }
    }

    public PulsarStandaloneBootstrap(URL url) {
        try {
            configuration = HadoopUtils.INSTANCE.loadConfigFile(url);
            loadConfig();
        } catch (BootstrapException e) {
            LOGGER.error("unable to load configuration", e);
        }
    }

    @Override
    public ComponentMetadata getMetadata() {
        return new PulsarStandaloneMetadata();
    }

    @Override
    public String getProperties() {
        return "\n \t\t\t ip:" + ip +
                "\n \t\t\t port:" + port +
                "\n \t\t\t zookeeper port:" + zookeeperPort;
    }

    private void loadConfig() {
        port = configuration.getInt(PulsarStandaloneConfig.PULSAR_PORT_KEY);
        ip = configuration.getString(PulsarStandaloneConfig.PULSAR_IP_KEY);
        tmpDirPath = getTmpDirPath(configuration, PulsarStandaloneConfig.PULSAR_TEMP_DIR_KEY);

        zookeeperDir = getTmpDirPath(configuration, PulsarStandaloneConfig.PULSAR_ZOOKEEPER_TEMP_DIR_KEY);
        zookeeperPort = configuration.getInt(PulsarStandaloneConfig.PULSAR_ZOOKEEPER_PORT_KEY);
    }

    @Override
    public void loadConfig(Map<String, String> configs) {
        if (StringUtils.isNotEmpty(configs.get(PulsarStandaloneConfig.PULSAR_PORT_KEY))) {
            port = Integer.parseInt(configs.get(PulsarStandaloneConfig.PULSAR_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(PulsarStandaloneConfig.PULSAR_IP_KEY))) {
            ip = configs.get(PulsarStandaloneConfig.PULSAR_IP_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(PulsarStandaloneConfig.PULSAR_TEMP_DIR_KEY))) {
            tmpDirPath = getTmpDirPath(configs, PulsarStandaloneConfig.PULSAR_TEMP_DIR_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(PulsarStandaloneConfig.PULSAR_ZOOKEEPER_TEMP_DIR_KEY))) {
            zookeeperDir = getTmpDirPath(configs, PulsarStandaloneConfig.PULSAR_ZOOKEEPER_TEMP_DIR_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(PulsarStandaloneConfig.PULSAR_ZOOKEEPER_PORT_KEY))) {
            zookeeperPort = Integer.parseInt(configs.get(PulsarStandaloneConfig.PULSAR_ZOOKEEPER_PORT_KEY));
        }
    }

    private void build() throws IOException {
        File tmpDirectory = new File(tmpDirPath);
        tmpDirectory.mkdirs();

        pulsarStandalone = PulsarStandaloneBuilder.instance()
                .withAdvertisedAddress(ip)
                .withZkDir(zookeeperDir)
                .withZkPort(zookeeperPort)
                .withBkDir(tmpDirPath + "/data/standalone/bookkeeper")
                .build();
        File tempFile = File.createTempFile("standalone-", "-embedded.conf", tmpDirectory);

//        FileWriter writer = new FileWriter(tempFile);
//        writer.write("journalDirectory=" + tmpDirPath + "/data/bookkeeper/journal\n");
//        writer.write("ledgerDirectories=" + tmpDirPath + "/data/bookkeeper/ledger\n");
//        writer.write("indexDirectories=" + tmpDirPath + "/data/bookkeeper/index\n");
//        writer.close();

        pulsarStandalone.getConfig().setBrokerServicePort(Optional.of(Integer.valueOf(port)));
        pulsarStandalone.setConfigFile(tempFile.getAbsolutePath());
    }

    @Override
    public Bootstrap start() {
        if (state == State.STOPPED) {
            state = State.STARTING;
            LOGGER.info("{} is starting", this.getClass().getName());
            try {
                build();
                pulsarStandalone.start();
            } catch (Exception e) {
                LOGGER.error("unable to add pulsar", e);
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
                pulsarStandalone.close();
                cleanup();
            } catch (Exception e) {
                LOGGER.error("unable to stop pulsar", e);
            }
            state = State.STOPPED;
            LOGGER.info("{} is stopped", this.getClass().getName());
        }
        return this;
    }

    private void cleanup() {
        try {
            FileUtils.deleteDirectory(Paths.get(tmpDirPath).toFile());
        } catch (IOException e) {
            LOGGER.error("unable to delete {}", tmpDirPath, e);
        }
    }


}
