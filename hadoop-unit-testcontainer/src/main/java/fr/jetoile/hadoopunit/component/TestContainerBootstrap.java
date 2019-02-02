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
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TestContainerBootstrap implements Bootstrap {
    static final private Logger LOGGER = LoggerFactory.getLogger(TestContainerBootstrap.class);

    private State state = State.STOPPED;
    private Configuration configuration;

    private FixedHostPortGenericContainer container;

    private String imageName;
    private List<Integer> exposedPorts;
    private Map<String, String> envs;
    private Map<String, String> labels;
    private List<String> command;
    private Map<Integer, Integer> fixedExposedPorts;
    private Map<String, String> classpathResourceMappings;


    public TestContainerBootstrap() {
        try {
            configuration = HadoopUtils.INSTANCE.loadConfigFile(null);
            loadConfig();
        } catch (BootstrapException e) {
            LOGGER.error("unable to load configuration", e);
        }
    }

    public TestContainerBootstrap(URL url) {
        try {
            configuration = HadoopUtils.INSTANCE.loadConfigFile(url);
            loadConfig();
        } catch (BootstrapException e) {
            LOGGER.error("unable to load configuration", e);
        }
    }

    @Override
    public ComponentMetadata getMetadata() {
        return new TestContainerMetadata();
    }

    @Override
    public String getProperties() {
        return "\n \t\t\t imageName:" + imageName +
                "\n \t\t\t exposedPorts:" + exposedPorts +
                "\n \t\t\t fixedExposedPortsList:" + fixedExposedPorts +
                "\n \t\t\t envs:" + envs +
                "\n \t\t\t labels:" + labels +
                "\n \t\t\t command:" + command +
                "\n \t\t\t classpathResourceMappings:" + classpathResourceMappings;
    }

    private void loadConfig() {
        imageName = configuration.getString(TestContainerConfig.TESTCONTAINER_IMAGENAME_KEY);

        String[] portsAsString = configuration.getStringArray(TestContainerConfig.TESTCONTAINER_EXPOSEDPORTS_KEY);
        exposedPorts = Arrays.asList(portsAsString).stream()
                .map(Integer::valueOf)
                .collect(Collectors.toList());

        String[] commandAsString = configuration.getStringArray(TestContainerConfig.TESTCONTAINER_COMMAND_KEY);
        command = Arrays.asList(commandAsString).stream()
                .collect(Collectors.toList());

        String[] envsAsString = configuration.getStringArray(TestContainerConfig.TESTCONTAINER_ENVS_KEY);
        envs = Arrays.asList(envsAsString).stream().collect(Collectors.toMap(c -> c.split(":")[0], c -> c.split(":")[1]));

        String[] labelsAsString = configuration.getStringArray(TestContainerConfig.TESTCONTAINER_LABELS_KEY);
        labels = Arrays.asList(labelsAsString).stream().collect(Collectors.toMap(c -> c.split(":")[0], c -> c.split(":")[1]));

        String[] fixedExposedPortsString = configuration.getStringArray(TestContainerConfig.TESTCONTAINER_FIXED_EXPOSEDPORTS_KEY);
        fixedExposedPorts = Arrays.asList(fixedExposedPortsString).stream().collect(Collectors.toMap(c -> Integer.valueOf(c.split(":")[0]), c -> Integer.valueOf(c.split(":")[1])));

        String[] classpathResourceMappingsList = configuration.getStringArray(TestContainerConfig.TESTCONTAINER_CLASSPATH_RESOURCES_MAPPING_KEY);
        classpathResourceMappings = Arrays.asList(classpathResourceMappingsList).stream().collect(Collectors.toMap(c -> c.split(":")[0], c -> c.substring(c.indexOf(":") + 1)));
    }


    @Override
    public void loadConfig(Map<String, String> configs) {
        if (StringUtils.isNotEmpty(configs.get(TestContainerConfig.TESTCONTAINER_IMAGENAME_KEY))) {
            imageName = configs.get(TestContainerConfig.TESTCONTAINER_IMAGENAME_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(TestContainerConfig.TESTCONTAINER_EXPOSEDPORTS_KEY))) {
            String ports = configs.get(TestContainerConfig.TESTCONTAINER_EXPOSEDPORTS_KEY);
            String[] portsAsString = ports.split(",");
            exposedPorts = Arrays.asList(portsAsString).stream()
                    .map(Integer::valueOf)
                    .collect(Collectors.toList());
        }
        if (StringUtils.isNotEmpty(configs.get(TestContainerConfig.TESTCONTAINER_COMMAND_KEY))) {
            String commandList = configs.get(TestContainerConfig.TESTCONTAINER_COMMAND_KEY);
            String[] commandAsString = commandList.split(",");
            command = Arrays.asList(commandAsString).stream()
                    .collect(Collectors.toList());
        }
        if (StringUtils.isNotEmpty(configs.get(TestContainerConfig.TESTCONTAINER_ENVS_KEY))) {
            String envsList = configs.get(TestContainerConfig.TESTCONTAINER_ENVS_KEY);
            String[] envsAsString = envsList.split(",");
            envs = Arrays.asList(envsAsString).stream().collect(Collectors.toMap(c -> c.split(":")[0], c -> c.split(":")[1]));
        }
        if (StringUtils.isNotEmpty(configs.get(TestContainerConfig.TESTCONTAINER_LABELS_KEY))) {
            String labelsList = configs.get(TestContainerConfig.TESTCONTAINER_LABELS_KEY);
            String[] labelsAsString = labelsList.split(",");
            labels = Arrays.asList(labelsAsString).stream().collect(Collectors.toMap(c -> c.split(":")[0], c -> c.split(":")[1]));
        }
        if (StringUtils.isNotEmpty(configs.get(TestContainerConfig.TESTCONTAINER_FIXED_EXPOSEDPORTS_KEY))) {
            String fixedExposedPortsList = configs.get(TestContainerConfig.TESTCONTAINER_FIXED_EXPOSEDPORTS_KEY);
            String[] fixedExposedPortsAsString = fixedExposedPortsList.split(",");
            fixedExposedPorts = Arrays.asList(fixedExposedPortsAsString).stream().collect(Collectors.toMap(c -> Integer.valueOf(c.split(":")[0]), c -> Integer.valueOf(c.split(":")[1])));
        }
        if (StringUtils.isNotEmpty(configs.get(TestContainerConfig.TESTCONTAINER_CLASSPATH_RESOURCES_MAPPING_KEY))) {
            String classpathResourceMappingsList = configs.get(TestContainerConfig.TESTCONTAINER_CLASSPATH_RESOURCES_MAPPING_KEY);
            String[] classpathResourceMappingsAsString = classpathResourceMappingsList.split(",");
            classpathResourceMappings = Arrays.asList(classpathResourceMappingsAsString).stream().collect(Collectors.toMap(c -> c.split(":")[0], c -> c.substring(c.indexOf(":") + 1)));
        }
    }

    private void build() {
        container = new FixedHostPortGenericContainer(imageName);
        if (!exposedPorts.isEmpty()) {
            container.withExposedPorts(exposedPorts.toArray(new Integer[0]));
        }
        if (!envs.isEmpty()) {
            container.withEnv(envs);
        }
        if (!labels.isEmpty()) {
            container.withLabels(labels);
        }
        if (!command.isEmpty()) {
            container.withCommand(command.toArray(new String[0]));
        }
        fixedExposedPorts.entrySet().stream().forEach(entry -> {
            container.withFixedExposedPort(entry.getKey(), entry.getValue());
        });

        if (!classpathResourceMappings.isEmpty()) {
            classpathResourceMappings.entrySet().stream().forEach(entry -> {
                String local = entry.getKey();
                String[] value = entry.getValue().split(":");
                String remote = value[0];
                String bindMode = value[1];
                container.withClasspathResourceMapping(local, remote, BindMode.valueOf(bindMode));
            });
        }
    }

    @Override
    public Bootstrap start() {
        if (state == State.STOPPED) {
            state = State.STARTING;
            LOGGER.info("{} is starting", this.getClass().getName());
            try {
                build();
                container.start();
                container.followOutput(new Slf4jLogConsumer(LOGGER));
            } catch (Exception e) {
                LOGGER.error("unable to add testcontainer", e);
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
                LOGGER.error("unable to stop testcontainer", e);
            }
            state = State.STOPPED;
            LOGGER.info("{} is stopped", this.getClass().getName());
        }
        return this;
    }

    public GenericContainer getContainer() {
        return container;
    }
}
