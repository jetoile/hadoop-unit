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
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.IndexSettings;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class ElasticSearchBootstrap implements Bootstrap {
    final public static String NAME = Component.ELASTICSEARCH.name();

    static final private Logger LOGGER = LoggerFactory.getLogger(ElasticSearchBootstrap.class);

    private State state = State.STOPPED;

    private Configuration configuration;

    private EmbeddedElastic elasticsearchCluster;

    private String ip;
    private int httpPort;
    private int tcpPort;
    private String version;
    private String indexName;
    private String clusterName;
    private String downloadUrl;

    public ElasticSearchBootstrap() {
        try {
            configuration = HadoopUtils.INSTANCE.loadConfigFile(null);
            loadConfig();
        } catch (BootstrapException e) {
            LOGGER.error("unable to load configuration", e);
        }
    }

    public ElasticSearchBootstrap(URL url) {
        try {
            configuration = HadoopUtils.INSTANCE.loadConfigFile(url);
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
        return "\n \t\t\t clusterName:" + clusterName +
                "\n \t\t\t ip:" + ip +
                "\n \t\t\t httpPort:" + httpPort +
                "\n \t\t\t tcpPort:" + tcpPort +
                "\n \t\t\t indexName:" + indexName +
                "\n \t\t\t version:" + version +
                (StringUtils.isNotEmpty(downloadUrl) ? "\n \t\t\t downloadUrl: " + downloadUrl : "");
    }

    private void loadConfig() throws BootstrapException {
        version = configuration.getString(HadoopUnitConfig.ELASTICSEARCH_VERSION);
        httpPort = configuration.getInt(HadoopUnitConfig.ELASTICSEARCH_HTTP_PORT_KEY);
        tcpPort = configuration.getInt(HadoopUnitConfig.ELASTICSEARCH_TCP_PORT_KEY);
        ip = configuration.getString(HadoopUnitConfig.ELASTICSEARCH_IP_KEY);
        indexName = configuration.getString(HadoopUnitConfig.ELASTICSEARCH_INDEX_NAME);
        clusterName = configuration.getString(HadoopUnitConfig.ELASTICSEARCH_CLUSTER_NAME);
        downloadUrl = configuration.getString(HadoopUnitConfig.ELASTICSEARCH_DOWNLOAD_URL, null);
    }

    @Override
    public void loadConfig(Map<String, String> configs) {
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.ELASTICSEARCH_HTTP_PORT_KEY))) {
            httpPort = Integer.parseInt(configs.get(HadoopUnitConfig.ELASTICSEARCH_HTTP_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.ELASTICSEARCH_TCP_PORT_KEY))) {
            tcpPort = Integer.parseInt(configs.get(HadoopUnitConfig.ELASTICSEARCH_TCP_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.ELASTICSEARCH_IP_KEY))) {
            ip = configs.get(HadoopUnitConfig.ELASTICSEARCH_IP_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.ELASTICSEARCH_VERSION))) {
            version = configs.get(HadoopUnitConfig.ELASTICSEARCH_VERSION);
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.ELASTICSEARCH_INDEX_NAME))) {
            indexName = configs.get(HadoopUnitConfig.ELASTICSEARCH_INDEX_NAME);
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.ELASTICSEARCH_CLUSTER_NAME))) {
            clusterName = configs.get(HadoopUnitConfig.ELASTICSEARCH_CLUSTER_NAME);
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.ELASTICSEARCH_DOWNLOAD_URL))) {
            downloadUrl = configs.get(HadoopUnitConfig.ELASTICSEARCH_DOWNLOAD_URL);
        }
    }

    private void build() throws IOException, InterruptedException {

        Path esInstallationPath = Paths.get(System.getProperty("user.home") + "/.elasticsearch");
        esInstallationPath.toFile().mkdirs();


        EmbeddedElastic.Builder esBuilder = EmbeddedElastic.builder()
                .withElasticVersion(version)
                .withSetting(PopularProperties.TRANSPORT_TCP_PORT, tcpPort)
                .withSetting(PopularProperties.HTTP_PORT, httpPort)
                .withSetting(PopularProperties.CLUSTER_NAME, clusterName)
                .withSetting("network.host", ip)
                .withIndex(indexName, IndexSettings.builder().build())
                .withInstallationDirectory(esInstallationPath.toFile())
                .withCleanInstallationDirectoryOnStop(true);

        if (StringUtils.isNotEmpty(downloadUrl)) {
            esBuilder.withDownloadUrl(new URL(downloadUrl));
        }

        elasticsearchCluster = esBuilder.build().start();
    }


    @Override
    public Bootstrap start() {
        if (state == State.STOPPED) {
            state = State.STARTING;
            LOGGER.info("{} is starting", this.getClass().getName());
            try {
                build();
            } catch (Exception e) {
                LOGGER.error("unable to add elastic", e);
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
                elasticsearchCluster.stop();
            } catch (Exception e) {
                LOGGER.error("unable to stop elastic", e);
            }
            state = State.STOPPED;
            LOGGER.info("{} is stopped", this.getClass().getName());
        }
        return this;
    }

}
