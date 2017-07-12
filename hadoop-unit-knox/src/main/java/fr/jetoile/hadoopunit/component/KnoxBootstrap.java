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

import com.github.sakserv.minicluster.impl.KnoxLocalCluster;
import com.mycila.xmltool.XMLDoc;
import com.mycila.xmltool.XMLTag;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KnoxBootstrap implements Bootstrap {
    final public static String NAME = Component.KNOX.name();

    static final private Logger LOGGER = LoggerFactory.getLogger(KnoxBootstrap.class);

    private KnoxLocalCluster knoxLocalCluster;

    private State state = State.STOPPED;

    private Configuration configuration;

    private String host;
    private int port;
    private String path;
    private String clusterName;
    private String tempDirectory;
    private List<KnoxService> services = new ArrayList<>();

    public KnoxBootstrap() {
        if (knoxLocalCluster == null) {
            try {
                loadConfig();
            } catch (BootstrapException e) {
                LOGGER.error("unable to load configuration", e);
            }
        }
    }


    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getProperties() {
        return "[" +
                "port:" + port +
                ", path:" + path +
                ", cluster:" + clusterName +
                "]";
    }

    private void init() {

    }

    private void build() {
        knoxLocalCluster = new KnoxLocalCluster.Builder()
                .setHost(host)
                .setPort(port)
                .setPath(path)
                .setCluster(clusterName)
                .setHomeDir(tempDirectory)
                .setTopology(getTopology(services))
                .build();
    }

    String getTopology(List<KnoxService> services) {

        XMLTag xmlTag = XMLDoc.newDocument(true)
            .addRoot("topology")
                .addTag("gateway")
                    .addTag("provider")
                        .addTag("role").addText("authentication")
                        .addTag("enabled").addText("false")
                        .gotoParent()
                    .addTag("provider")
                        .addTag("role").addText("identity-assertion")
                        .addTag("enabled").addText("false")
                    .gotoParent().gotoParent();

        for (KnoxService service : services) {
            switch (service) {
                case NAMENODE:
                    xmlTag
                            .addTag("service")
                                .addTag("role").addText(service.name())
                                .addTag("url").addText("hdfs://" + configuration.getString(HadoopUnitConfig.HDFS_NAMENODE_HOST_KEY) + ":" + configuration.getString(HadoopUnitConfig.HDFS_NAMENODE_PORT_KEY))
                                .gotoParent();
                    break;
                case WEBHDFS:
                    xmlTag
                            .addTag("service")
                                .addTag("role").addText(service.name())
                                .addTag("url").addText("http://" + configuration.getString(HadoopUnitConfig.HDFS_NAMENODE_HOST_KEY) + ":" + configuration.getString(HadoopUnitConfig.HDFS_NAMENODE_HTTP_PORT_KEY) + "/webhdfs")
                                .gotoParent();
                    break;
                case WEBHBASE:
                xmlTag
                            .addTag("service")
                                .addTag("role").addText(service.name())
                                .addTag("url").addText("http://" + configuration.getString(HadoopUnitConfig.HBASE_REST_HOST_KEY) + ":" + configuration.getString(HadoopUnitConfig.HBASE_REST_PORT_KEY))
                                .gotoParent();
                break;
            }
        }

        return xmlTag.toString();
    }

    private void loadConfig() throws BootstrapException {
        try {
            configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }

        port = configuration.getInt(HadoopUnitConfig.KNOX_PORT_KEY);
        host = configuration.getString(HadoopUnitConfig.KNOX_HOST_KEY);
        path = configuration.getString(HadoopUnitConfig.KNOX_PATH_KEY);
        clusterName = configuration.getString(HadoopUnitConfig.KNOX_CLUSTER_KEY);
        tempDirectory = configuration.getString(HadoopUnitConfig.KNOX_HOME_DIR_KEY);

        List<String> servicesList = Arrays.asList(configuration.getStringArray(HadoopUnitConfig.KNOX_SERVICE_KEY));
        services = Arrays.asList(KnoxService.values()).stream().filter(s -> servicesList.contains(s.getName())).collect(Collectors.toList());
    }

    @Override
    public void loadConfig(Map<String, String> configs) {
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.KNOX_PORT_KEY))) {
            port = Integer.parseInt(configs.get(HadoopUnitConfig.KNOX_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.KNOX_HOST_KEY))) {
            host = configuration.getString(HadoopUnitConfig.KNOX_HOST_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.KNOX_PATH_KEY))) {
            path = configuration.getString(HadoopUnitConfig.KNOX_PATH_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.KNOX_CLUSTER_KEY))) {
            clusterName = configuration.getString(HadoopUnitConfig.KNOX_CLUSTER_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.KNOX_HOME_DIR_KEY))) {
            tempDirectory = configuration.getString(HadoopUnitConfig.KNOX_HOME_DIR_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(HadoopUnitConfig.KNOX_SERVICE_KEY))) {
            List<String> servicesList = Arrays.asList(configuration.getStringArray(HadoopUnitConfig.KNOX_SERVICE_KEY));
            services = Arrays.asList(KnoxService.values()).stream().filter(s -> servicesList.contains(s.getName())).collect(Collectors.toList());
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
                knoxLocalCluster.start();
            } catch (Exception e) {
                LOGGER.error("unable to add knox", e);
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
                knoxLocalCluster.stop(true);
            } catch (Exception e) {
                LOGGER.error("unable to stop knox", e);
            }
            state = State.STOPPED;
            LOGGER.info("{} is stopped", this.getClass().getName());
        }
        return this;

    }
}
