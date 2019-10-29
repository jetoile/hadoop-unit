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
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.common.component.ComponentStarter;
import org.apache.bookkeeper.common.component.LifecycleComponent;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Map;

import static org.apache.bookkeeper.server.Main.buildBookieServer;

public class BookkeeperBootstrap implements Bootstrap {
    static final private Logger LOGGER = LoggerFactory.getLogger(BookkeeperBootstrap.class);
    private State state = State.STOPPED;
    private Configuration configuration;

    private LifecycleComponent server;
    private ServerConfiguration bookkeeperConf;

    private int port;
    private String ip;
    private String tmpDirPath;
    private int httpPort;

    private int zookeeperPort;
    private String zookeeperHost;

    public BookkeeperBootstrap() {
        try {
            configuration = HadoopUtils.INSTANCE.loadConfigFile(null);
            loadConfig();
        } catch (BootstrapException e) {
            LOGGER.error("unable to load configuration", e);
        }
    }

    public BookkeeperBootstrap(URL url) {
        try {
            configuration = HadoopUtils.INSTANCE.loadConfigFile(url);
            loadConfig();
        } catch (BootstrapException e) {
            LOGGER.error("unable to load configuration", e);
        }
    }

    @Override
    public ComponentMetadata getMetadata() {
        return new BookkeeperMetadata();
    }

    @Override
    public String getProperties() {
        return "\n \t\t\t ip:" + ip +
                "\n \t\t\t port:" + port +
                "\n \t\t\t http port:" + httpPort;
    }

    private void loadConfig() {
        port = configuration.getInt(BookkeeperConfig.BOOKKEEPER_PORT_KEY);
        ip = configuration.getString(BookkeeperConfig.BOOKKEEPER_IP_KEY);
        httpPort = configuration.getInt(BookkeeperConfig.BOOKKEEPER_HTTP_PORT_KEY);
        tmpDirPath = getTmpDirPath(configuration, BookkeeperConfig.BOOKKEEPER_TEMP_DIR_KEY);

        zookeeperPort = configuration.getInt(ZookeeperConfig.ZOOKEEPER_PORT_KEY);
        zookeeperHost = configuration.getString(ZookeeperConfig.ZOOKEEPER_HOST_KEY);
    }

    @Override
    public void loadConfig(Map<String, String> configs) {
        if (StringUtils.isNotEmpty(configs.get(BookkeeperConfig.BOOKKEEPER_PORT_KEY))) {
            port = Integer.parseInt(configs.get(BookkeeperConfig.BOOKKEEPER_PORT_KEY));
        }
        if (StringUtils.isNotEmpty(configs.get(BookkeeperConfig.BOOKKEEPER_IP_KEY))) {
            ip = configs.get(BookkeeperConfig.BOOKKEEPER_IP_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(BookkeeperConfig.BOOKKEEPER_TEMP_DIR_KEY))) {
            tmpDirPath = getTmpDirPath(configs, BookkeeperConfig.BOOKKEEPER_TEMP_DIR_KEY);
        }
    }

    private void build() throws Exception {
        bookkeeperConf = new ServerConfiguration();
        bookkeeperConf.setAdvertisedAddress(ip);
        bookkeeperConf.setBookiePort(port);
        bookkeeperConf.setHttpServerEnabled(true);
        bookkeeperConf.setHttpServerPort(httpPort);
        bookkeeperConf.setMetadataServiceUri("zk+hierarchical://" + zookeeperHost + ":" + zookeeperPort + "/ledgers");
        bookkeeperConf.setJournalDirName(tmpDirPath + "/bk-txn");
        bookkeeperConf.setLedgerDirNames(new String[] {tmpDirPath + "/ledger"});
        bookkeeperConf.setIndexDirName(new String[] {tmpDirPath + "/index"});

        bookkeeperConf.addProperty("httpServerClass", "org.apache.bookkeeper.http.vertx.VertxHttpServer");

        bookkeeperConf.setWriteBufferBytes(65536);

        bookkeeperConf.setZkEnableSecurity(false);
        bookkeeperConf.setZkTimeout(10000);

        BookKeeperAdmin.format(bookkeeperConf, false, true);

        server = buildBookieServer(new BookieConfiguration(bookkeeperConf));

    }

    @Override
    public Bootstrap start() {
        if (state == State.STOPPED) {
            state = State.STARTING;
            LOGGER.info("{} is starting", this.getClass().getName());
            try {
                build();

                StartBookkeeper startBookkeeper = new StartBookkeeper(server);
                Thread t = new Thread(startBookkeeper);
                t.setDaemon(true);
                t.start();
            } catch (Exception e) {
                LOGGER.error("unable to add bookkeeper", e);
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
                cleanup();
            } catch (Exception e) {
                LOGGER.error("unable to stop bookkeeper", e);
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

    private static class StartBookkeeper implements Runnable {

        private LifecycleComponent server;

        public StartBookkeeper(LifecycleComponent server) {
            this.server = server;
        }

        @Override
        public void run() {
            try {
                ComponentStarter.startComponent(server).get();
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }
}


