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

import com.google.common.collect.Sets;
import fr.jetoile.hadoopunit.ComponentMetadata;
import fr.jetoile.hadoopunit.HadoopUtils;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

public class PulsarBootstrap implements Bootstrap {
    static final private Logger LOGGER = LoggerFactory.getLogger(PulsarBootstrap.class);
    private State state = State.STOPPED;
    private Configuration configuration;
    private int port;
    private int httpPort;
    private String ip;
    private String tmpDirPath;
    private int streamerStoragePort;
    private boolean workerEnabled;
    private String workerClientAuthenticationParameters;
    private String workerClientAuthenticationPlugin;

    private boolean authenticationEnabled;
    private String authenticationProviders;
    private boolean authorizationEnabled;
    private String authorizationProviders;
    private Map<String, String> extraConf = new HashMap<>();

    private int zookeeperPort;
    private String zookeeperHost;

    private String name;

    private String ZOOKEEPER_PORT_KEY = "zookeeper.port";
    private String ZOOKEEPER_HOST_CLIENT_KEY = "zookeeper.client.host";

    private PulsarService pulsarService;
    private ServiceConfiguration serviceConfiguration;
    private WorkerService functionsWorkerService;
    private WorkerConfig workerConfig;

    public PulsarBootstrap() {
        try {
            configuration = HadoopUtils.INSTANCE.loadConfigFile(null);
            loadConfig();
        } catch (BootstrapException e) {
            LOGGER.error("unable to load configuration", e);
        }
    }

    public PulsarBootstrap(URL url) {
        try {
            configuration = HadoopUtils.INSTANCE.loadConfigFile(url);
            loadConfig();
        } catch (BootstrapException e) {
            LOGGER.error("unable to load configuration", e);
        }
    }

    @Override
    public ComponentMetadata getMetadata() {
        return new PulsarMetadata();
    }

    @Override
    public String getProperties() {
        return "\n \t\t\t name:" + name +
                "\n \t\t\t ip:" + ip +
                "\n \t\t\t port:" + port +
                "\n \t\t\t httpPort:" + httpPort +
                "\n \t\t\t zookeeper port:" + zookeeperPort;
    }

    private void loadConfig() {
        name = configuration.getString(PulsarConfig.PULSAR_NAME_KEY);
        port = configuration.getInt(PulsarConfig.PULSAR_PORT_KEY);
        httpPort = configuration.getInt(PulsarConfig.PULSAR_HTTP_PORT_KEY);
        ip = configuration.getString(PulsarConfig.PULSAR_IP_KEY);
        tmpDirPath = getTmpDirPath(configuration, PulsarConfig.PULSAR_TEMP_DIR_KEY);
        streamerStoragePort = configuration.getInt(PulsarConfig.PULSAR_STREAMER_STORAGE_PORT_KEY);

        authenticationEnabled = configuration.getBoolean(PulsarConfig.PULSAR_AUTHENTICATION_ENABLED_KEY, false);
        authenticationProviders = configuration.getString(PulsarConfig.PULSAR_AUTHENTICATION_PROVIDERS_KEY,"");
        authorizationEnabled = configuration.getBoolean(PulsarConfig.PULSAR_AUTHORIZATION_ENABLED_KEY, false);
        authorizationProviders = configuration.getString(PulsarConfig.PULSAR_AUTHORIZATION_PROVIDER_KEY, "");

        String[] extraConfsAsList = configuration.getStringArray(PulsarConfig.PULSAR_EXTRA_CONF_KEY);
        extraConf = Arrays.asList(extraConfsAsList).stream().collect(Collectors.toMap(c -> c.split("=")[0], c -> c.split("=")[1]));

        workerEnabled = configuration.getBoolean(PulsarConfig.PULSAR_WORKER_ENABLED_KEY, true);
        workerClientAuthenticationParameters = configuration.getString(PulsarConfig.PULSAR_WORKER_CLIENT_AUTHENTICATION_PARAMETERS_KEY);
        workerClientAuthenticationPlugin = configuration.getString(PulsarConfig.PULSAR_WORKER_CLIENT_AUTHENTICATION_PLUGIN_KEY);

        zookeeperHost = configuration.getString(ZOOKEEPER_HOST_CLIENT_KEY);
        zookeeperPort = configuration.getInt(ZOOKEEPER_PORT_KEY);
    }

    @Override
    public void loadConfig(Map<String, String> configs) {
        if (StringUtils.isNotEmpty(configs.get(PulsarConfig.PULSAR_NAME_KEY))) {
            name = configs.get(PulsarConfig.PULSAR_NAME_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(PulsarConfig.PULSAR_PORT_KEY))) {
            port = Integer.parseInt(configs.get(PulsarConfig.PULSAR_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(PulsarConfig.PULSAR_HTTP_PORT_KEY))) {
            httpPort = Integer.parseInt(configs.get(PulsarConfig.PULSAR_HTTP_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(PulsarConfig.PULSAR_IP_KEY))) {
            ip = configs.get(PulsarConfig.PULSAR_IP_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(PulsarConfig.PULSAR_TEMP_DIR_KEY))) {
            tmpDirPath = getTmpDirPath(configs, PulsarConfig.PULSAR_TEMP_DIR_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(PulsarConfig.PULSAR_STREAMER_STORAGE_PORT_KEY))) {
            streamerStoragePort = Integer.parseInt(configs.get(PulsarConfig.PULSAR_STREAMER_STORAGE_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(PulsarConfig.PULSAR_AUTHENTICATION_ENABLED_KEY))) {
            authenticationEnabled = Boolean.parseBoolean(configs.get(PulsarConfig.PULSAR_AUTHENTICATION_ENABLED_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(PulsarConfig.PULSAR_AUTHENTICATION_PROVIDERS_KEY))) {
            authenticationProviders = configs.get(PulsarConfig.PULSAR_AUTHENTICATION_PROVIDERS_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(PulsarConfig.PULSAR_AUTHORIZATION_ENABLED_KEY))) {
            authorizationEnabled = Boolean.parseBoolean(configs.get(PulsarConfig.PULSAR_AUTHORIZATION_ENABLED_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(PulsarConfig.PULSAR_AUTHORIZATION_PROVIDER_KEY))) {
            authorizationProviders = configs.get(PulsarConfig.PULSAR_AUTHORIZATION_PROVIDER_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(PulsarConfig.PULSAR_EXTRA_CONF_KEY))) {
            String extraConfList = configs.get(PulsarConfig.PULSAR_EXTRA_CONF_KEY);
            String[] extraConfsString = extraConfList.split(",");
            extraConf = Arrays.asList(extraConfsString).stream().collect(Collectors.toMap(c -> c.split("=")[0], c -> c.split("=")[1]));
        }
        if (StringUtils.isNotEmpty(configs.get(PulsarConfig.PULSAR_WORKER_ENABLED_KEY))) {
            workerEnabled = Boolean.parseBoolean(configs.get(PulsarConfig.PULSAR_WORKER_ENABLED_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(PulsarConfig.PULSAR_WORKER_CLIENT_AUTHENTICATION_PLUGIN_KEY))) {
            workerClientAuthenticationPlugin = configs.get(PulsarConfig.PULSAR_WORKER_CLIENT_AUTHENTICATION_PLUGIN_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(PulsarConfig.PULSAR_WORKER_CLIENT_AUTHENTICATION_PARAMETERS_KEY))) {
            workerClientAuthenticationParameters = configs.get(PulsarConfig.PULSAR_WORKER_CLIENT_AUTHENTICATION_PARAMETERS_KEY);
        }

        if (StringUtils.isNotEmpty(configs.get(ZOOKEEPER_PORT_KEY))) {
            zookeeperPort = Integer.parseInt(configs.get(ZOOKEEPER_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(ZOOKEEPER_HOST_CLIENT_KEY))) {
            zookeeperHost = configs.get(ZOOKEEPER_HOST_CLIENT_KEY);
        }
    }

    private void build() throws IOException {
        String zookeeperServers = zookeeperHost + ":" + zookeeperPort;

        serviceConfiguration = new ServiceConfiguration();

        serviceConfiguration.setBindAddress(ip);
        serviceConfiguration.setAdvertisedAddress(ip);
        serviceConfiguration.setZookeeperServers(zookeeperServers);
        serviceConfiguration.setConfigurationStoreServers(zookeeperServers);
        serviceConfiguration.setClusterName(name);
        serviceConfiguration.setBrokerServicePort(Optional.of(port));
        serviceConfiguration.setWebServicePort(Optional.of(httpPort));
        serviceConfiguration.setManagedLedgerDefaultEnsembleSize(1);
        serviceConfiguration.setManagedLedgerDefaultWriteQuorum(1);
        serviceConfiguration.setManagedLedgerDefaultAckQuorum(1);
        serviceConfiguration.setAllowAutoTopicCreation(true);

        if (authenticationEnabled) {
            serviceConfiguration.setAuthenticationEnabled(authenticationEnabled);
            serviceConfiguration.setAuthenticationProviders(Collections.singleton(authenticationProviders));
        }
        if (authorizationEnabled) {
            serviceConfiguration.setAuthorizationEnabled(authorizationEnabled);
            serviceConfiguration.setAuthorizationProvider(authorizationProviders);
        }

        if (!extraConf.isEmpty()) {
            Properties properties = new Properties();
            extraConf.entrySet().forEach(e -> {
                properties.setProperty(e.getKey(), e.getValue());
            });
            serviceConfiguration.setProperties(properties);
        }

        if (workerEnabled) {
            workerConfig = new WorkerConfig();

            workerConfig.setPulsarServiceUrl("pulsar://127.0.0.1:" + port);
            workerConfig.setPulsarWebServiceUrl("http://127.0.0.1:" + httpPort);
            workerConfig.setWorkerHostname(ip);
            workerConfig.setWorkerPort(httpPort);
            workerConfig.setWorkerId("c-" + name + "-fw-" + serviceConfiguration.getAdvertisedAddress() + "-" + workerConfig.getWorkerPort());
            workerConfig.setConfigurationStoreServers(zookeeperServers);
            workerConfig.setZooKeeperSessionTimeoutMillis(10000);
            workerConfig.setZooKeeperOperationTimeoutSeconds(10000);
            workerConfig.setDownloadDirectory(tmpDirPath + "/pulsar_functions");
            workerConfig.setPulsarFunctionsNamespace("public/functions");
            workerConfig.setPulsarFunctionsCluster(name);
            workerConfig.setSchedulerClassName("org.apache.pulsar.functions.worker.scheduler.RoundRobinScheduler");
            workerConfig.setRescheduleTimeoutMs(60000);
            workerConfig.setFailureCheckFreqMs(30000);
            workerConfig.setInitialBrokerReconnectMaxRetries(60);
            workerConfig.setAssignmentWriteMaxRetries(60);
            workerConfig.setInstanceLivenessCheckFreqMs(30000);
            workerConfig.setTopicCompactionFrequencySec(1800);
            workerConfig.setFunctionAssignmentTopicName("assignments");
            workerConfig.setFunctionMetadataTopicName("metadata");
            workerConfig.setClusterCoordinationTopicName("coordinate");
            workerConfig.setProcessContainerFactory(new WorkerConfig.ProcessContainerFactory()
                    .setExtraFunctionDependenciesDir(tmpDirPath + "/extraFunctionDependencies")
                    .setJavaInstanceJarLocation(tmpDirPath + "/javaInstanceJar")
                    .setLogDirectory(tmpDirPath + "/log"));
            workerConfig.setThreadContainerFactory(new WorkerConfig.ThreadContainerFactory().setThreadGroupName("functions-thread"));
            workerConfig.setStateStorageServiceUrl("bk://127.0.0.1:" + streamerStoragePort);

            if (StringUtils.isNotEmpty(workerClientAuthenticationPlugin)) {
                workerConfig.setClientAuthenticationPlugin(workerClientAuthenticationPlugin);
            }
            if (StringUtils.isNotEmpty(workerClientAuthenticationParameters)) {
                workerConfig.setClientAuthenticationParameters(workerClientAuthenticationParameters);
            }

            functionsWorkerService = new WorkerService(workerConfig);

            pulsarService = new PulsarService(serviceConfiguration, Optional.ofNullable(functionsWorkerService));
        } else {
            pulsarService = new PulsarService(serviceConfiguration, Optional.empty());
        }

    }

    @Override
    public Bootstrap start() {
        if (state == State.STOPPED) {
            state = State.STARTING;
            LOGGER.info("{} is starting", this.getClass().getName());
            try {
                build();
                pulsarService.start();
                setupCluster();
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
                functionsWorkerService.stop();
                pulsarService.getShutdownService().run();
            } catch (Exception e) {
                LOGGER.error("unable to stop pulsar", e);
            }
            state = State.STOPPED;
            LOGGER.info("{} is stopped", this.getClass().getName());
        }
        return this;
    }

    private void setupCluster() throws PulsarClientException, MalformedURLException {
        URL webServiceUrl = new URL(String.format("http://%s:%d", serviceConfiguration.getAdvertisedAddress(), serviceConfiguration.getWebServicePort().get()));
        final String brokerServiceUrl = String.format("pulsar://%s:%d", serviceConfiguration.getAdvertisedAddress(), serviceConfiguration.getBrokerServicePort().get());
        PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl(webServiceUrl.toString())
                .authentication(serviceConfiguration.getBrokerClientAuthenticationPlugin(), serviceConfiguration.getBrokerClientAuthenticationParameters())
                .build();

        final String cluster = serviceConfiguration.getClusterName();

        createSampleNameSpace(webServiceUrl, brokerServiceUrl, cluster, admin);
        createDefaultNameSpace(cluster, admin);;
    }

    private void createDefaultNameSpace(String cluster, PulsarAdmin admin) {
        // Create a public tenant and default namespace
        final String publicTenant = TopicName.PUBLIC_TENANT;
        final String defaultNamespace = TopicName.PUBLIC_TENANT + "/" + TopicName.DEFAULT_NAMESPACE;
        try {
            if (!admin.tenants().getTenants().contains(publicTenant)) {
                admin.tenants().createTenant(
                        publicTenant,
                        new TenantInfo(Sets.newHashSet(serviceConfiguration.getSuperUserRoles()), Sets.newHashSet(cluster)));
            }
            if (!admin.namespaces().getNamespaces(publicTenant).contains(defaultNamespace)) {
                admin.namespaces().createNamespace(defaultNamespace);
                admin.namespaces().setNamespaceReplicationClusters(defaultNamespace, Sets.newHashSet(serviceConfiguration.getClusterName()));
            }
        } catch (PulsarAdminException e) {
            LOGGER.info(e.getMessage());
        }
    }

    private void createSampleNameSpace(URL webServiceUrl, String brokerServiceUrl, String cluster, PulsarAdmin admin) {
        // Create a sample namespace
        final String property = "sample";
        final String globalCluster = "global";
        final String namespace = property + "/" + cluster + "/ns1";
        try {
            ClusterData clusterData = new ClusterData(webServiceUrl.toString(), null /* serviceUrlTls */,
                    brokerServiceUrl, null /* brokerServiceUrlTls */);
            if (!admin.clusters().getClusters().contains(cluster)) {
                admin.clusters().createCluster(cluster, clusterData);
            } else {
                admin.clusters().updateCluster(cluster, clusterData);
            }

            // Create marker for "global" cluster
            if (!admin.clusters().getClusters().contains(globalCluster)) {
                admin.clusters().createCluster(globalCluster, new ClusterData(null, null));
            }

            if (!admin.tenants().getTenants().contains(property)) {
                admin.tenants().createTenant(property,
                        new TenantInfo(Sets.newHashSet(serviceConfiguration.getSuperUserRoles()), Sets.newHashSet(cluster)));
            }

            if (!admin.namespaces().getNamespaces(property).contains(namespace)) {
                admin.namespaces().createNamespace(namespace);
            }
        } catch (PulsarAdminException e) {
            LOGGER.info(e.getMessage());
        }
    }


}
