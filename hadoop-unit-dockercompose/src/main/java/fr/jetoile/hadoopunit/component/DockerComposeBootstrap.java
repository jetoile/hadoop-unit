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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DockerComposeBootstrap implements Bootstrap {
    static final private Logger LOGGER = LoggerFactory.getLogger(DockerComposeBootstrap.class);

    private State state = State.STOPPED;
    private Configuration configuration;

    private DockerComposeContainer container;

    private File dockerComposeFile;
    private Map<String, Integer> exposedPorts = new HashMap<>();
    private boolean local = false;

    public DockerComposeBootstrap() {
        try {
            configuration = HadoopUtils.INSTANCE.loadConfigFile(null);
            loadConfig();
        } catch (BootstrapException e) {
            LOGGER.error("unable to load configuration", e);
        }
    }

    public DockerComposeBootstrap(URL url) {
        try {
            configuration = HadoopUtils.INSTANCE.loadConfigFile(url);
            loadConfig();
        } catch (BootstrapException e) {
            LOGGER.error("unable to load configuration", e);
        }
    }

    @Override
    public ComponentMetadata getMetadata() {
        return new DockerComposeMetadata();
    }

    @Override
    public String getProperties() {
        return "\n \t\t\t dockerComposeFile:" + dockerComposeFile.getAbsolutePath() +
                "\n \t\t\t exposedPorts:" + exposedPorts;
    }

    private void loadConfig() {
        try {
            dockerComposeFile = Paths.get(DockerComposeBootstrap.class.getClassLoader().getResource(configuration.getString(DockerComposeConfig.DOCKERCOMPOSE_FILENAME_KEY)).getFile()).toFile();

            String[] exposedPortsString = configuration.getStringArray(DockerComposeConfig.DOCKERCOMPOSE_EXPOSEDPORTS_KEY);
            exposedPorts = Arrays.asList(exposedPortsString).stream().collect(Collectors.toMap(c -> c.split(":")[0], c -> Integer.valueOf(c.split(":")[1])));
            local = configuration.getBoolean(DockerComposeConfig.DOCKERCOMPOSE_LOCAL_KEY, false);
        } catch (Exception e) {
            //NOTHING TO DO
        }
    }


    @Override
    public void loadConfig(Map<String, String> configs) {
        if (StringUtils.isNotEmpty(configs.get(DockerComposeConfig.DOCKERCOMPOSE_FILENAME_KEY))) {
            dockerComposeFile = new File(configs.get(DockerComposeConfig.DOCKERCOMPOSE_FILENAME_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(DockerComposeConfig.DOCKERCOMPOSE_EXPOSEDPORTS_KEY))) {
            String exposedPortsList = configs.get(DockerComposeConfig.DOCKERCOMPOSE_EXPOSEDPORTS_KEY);
            String[] exposedPortsString = exposedPortsList.split(",");
            exposedPorts = Arrays.asList(exposedPortsString).stream().collect(Collectors.toMap(c -> c.split(":")[0], c -> Integer.valueOf(c.split(":")[1])));
        }
        if (StringUtils.isNotEmpty(configs.get(DockerComposeConfig.DOCKERCOMPOSE_LOCAL_KEY))) {
            local = Boolean.parseBoolean(configs.get(DockerComposeConfig.DOCKERCOMPOSE_LOCAL_KEY));
        }
    }

    private void build() {
        container = new DockerComposeContainer(dockerComposeFile);
        if (!exposedPorts.isEmpty()) {
            exposedPorts.entrySet().stream().forEach(entry -> {
                container.withExposedService(entry.getKey(), entry.getValue(), Wait.forListeningPort());
            });
        }
        container.withLocalCompose(local);
    }

    @Override
    public Bootstrap start() {
        if (state == State.STOPPED) {
            state = State.STARTING;
            LOGGER.info("{} is starting", this.getClass().getName());
            try {
                build();
                container.start();
            } catch (Throwable e) {
                LOGGER.error("unable to add docker compose", e);
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
                container.stop();
            } catch (Exception e) {
                LOGGER.error("unable to stop docker compose", e);
            }
            state = State.STOPPED;
            LOGGER.info("{} is stopped", this.getClass().getName());
        }
        return this;
    }

    public DockerComposeContainer getContainer() {
        return container;
    }
}
