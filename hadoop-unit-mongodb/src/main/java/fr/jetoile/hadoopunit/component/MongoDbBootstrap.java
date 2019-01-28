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

import com.github.sakserv.minicluster.impl.MongodbLocalServer;
import fr.jetoile.hadoopunit.ComponentMetadata;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.HadoopUtils;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Map;

public class MongoDbBootstrap implements Bootstrap {
    static final private Logger LOGGER = LoggerFactory.getLogger(MongoDbBootstrap.class);

    private State state = State.STOPPED;

    private Configuration configuration;

    private MongodbLocalServer mongodbLocalServer;

    private int port;
    private String ip;

    public MongoDbBootstrap() {
        if (mongodbLocalServer == null) {
            try {
                configuration = HadoopUtils.INSTANCE.loadConfigFile(null);
                loadConfig();
            } catch (BootstrapException e) {
                LOGGER.error("unable to load configuration", e);
            }
        }
    }

    public MongoDbBootstrap(URL url) {
        if (mongodbLocalServer == null) {
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
        return new MongoDbMetadata();
    }

    @Override
    public String getProperties() {
        return "\n \t\t\t ip:" + ip +
                "\n \t\t\t port:" + port;
    }

    private void loadConfig() {
        port = configuration.getInt(MongoDbConfig.MONGO_PORT_KEY);
        ip = configuration.getString(MongoDbConfig.MONGO_IP_KEY);
    }

    @Override
    public void loadConfig(Map<String, String> configs) {
        if (StringUtils.isNotEmpty(configs.get(MongoDbConfig.MONGO_PORT_KEY))) {
            port = Integer.parseInt(configs.get(MongoDbConfig.MONGO_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(MongoDbConfig.MONGO_IP_KEY))) {
            ip = configs.get(MongoDbConfig.MONGO_IP_KEY);
        }
    }

    private void build() {
        mongodbLocalServer = new MongodbLocalServer.Builder()
                .setIp(ip)
                .setPort(port)
                .build();
    }

    @Override
    public Bootstrap start() {
        if (state == State.STOPPED) {
            state = State.STARTING;
            LOGGER.info("{} is starting", this.getClass().getName());
            build();
            try {
                mongodbLocalServer.start();
            } catch (Exception e) {
                LOGGER.error("unable to add mongodb", e);
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
                mongodbLocalServer.stop(true);
            } catch (Exception e) {
                LOGGER.error("unable to stop mongdb", e);
            }
            state = State.STOPPED;
            LOGGER.info("{} is stopped", this.getClass().getName());
        }
        return this;
    }


}
