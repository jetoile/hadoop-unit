package fr.jetoile.hadoop.test.hdfs;


import fr.jetoile.sample.Config;
import fr.jetoile.sample.exception.ConfigException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

public enum HdfsUtils {
    INSTANCE;

    private final Logger LOGGER = LoggerFactory.getLogger(HdfsUtils.class);

    private FileSystem fileSystem = null;

    private Configuration configuration;
    private int port;
    private int httpPort;

    HdfsUtils() {
        try {
            loadConfig();
        } catch (ConfigException e) {
            System.exit(-1);
        }
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.default.name", "hdfs://127.0.0.1:" + configuration.getInt(Config.HDFS_NAMENODE_PORT_KEY));

        URI uri = URI.create("hdfs://127.0.0.1:" + configuration.getInt(Config.HDFS_NAMENODE_PORT_KEY));

        try {
            fileSystem = FileSystem.get(uri, conf);
        } catch (IOException e) {
            LOGGER.error("unable to create FileSystem", e);
        }
    }

    private void loadConfig() throws ConfigException {
        try {
            configuration = new PropertiesConfiguration("default.properties");
        } catch (ConfigurationException e) {
            throw new ConfigException("bad config", e);
        }

        port = configuration.getInt(Config.HDFS_NAMENODE_PORT_KEY);
        httpPort = configuration.getInt(Config.HDFS_NAMENODE_HTTP_PORT_KEY);
    }


    public FileSystem getFileSystem() {
        return fileSystem;
    }

}
