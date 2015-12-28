package fr.jetoile.sample.component;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.impl.ZookeeperLocalCluster;
import fr.jetoile.sample.BootstrapException;
import lombok.Setter;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public enum ZookeeperBootstrap implements Bootstrap {
    INSTANCE;

    final private Logger LOGGER = LoggerFactory.getLogger(ZookeeperBootstrap.class);

    private ZookeeperLocalCluster zookeeperLocalCluster;

    private Configuration configuration;

    private int port;
    private String localDir;
    private String host;

    ZookeeperBootstrap() {
        if (zookeeperLocalCluster == null) {
            try {
                loadConfig();
            } catch (BootstrapException e) {
                LOGGER.error("unable to load configuration", e);
            }
            init();
            build();
        }
    }

    private void loadConfig() throws BootstrapException {
        try {
            configuration = new PropertiesConfiguration("default.properties");
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }
        port = configuration.getInt(ConfigVars.ZOOKEEPER_PORT_KEY);
        localDir = configuration.getString(ConfigVars.ZOOKEEPER_TEMP_DIR_KEY);
        host = configuration.getString(ConfigVars.ZOOKEEPER_HOST_KEY);

    }


    private void init() {
        Path path = Paths.get(localDir);
        try {
            Files.createDirectories(path);
        } catch (IOException e) {
            LOGGER.error("unable to create mandatory directory", e);
        }

    }

    private void build() {
        zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
                .setPort(port)
                .setZookeeperConnectionString(host + ":" + port)
                .setTempDir(localDir)
                .build();
    }

    @Override
    public Bootstrap start() {
        try {
            zookeeperLocalCluster.start();
        } catch (Exception e) {
            LOGGER.error("unable to start zookeeper", e);
        }
        return this;
    }

    @Override
    public Bootstrap stop() {
        try {
            zookeeperLocalCluster.stop(true);
        } catch (Exception e) {
            LOGGER.error("unable to stop zookeeper", e);
        }
        return this;
    }

    @Override
    public org.apache.hadoop.conf.Configuration getConfiguration() {
        throw new UnsupportedOperationException("the method getConfiguration can not be called on ZookeeperBootstrap");
    }


}
