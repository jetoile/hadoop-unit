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

package fr.jetoile.hadoopunit.test.hdfs;


import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.exception.ConfigException;
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
    private String hdfsHost;

    HdfsUtils() {
        try {
            loadConfig();
        } catch (ConfigException e) {
            System.exit(-1);
        }
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.default.name", "hdfs://" + configuration.getString(HadoopUnitConfig.HDFS_NAMENODE_HOST_KEY) + ":" + configuration.getInt(HadoopUnitConfig.HDFS_NAMENODE_PORT_KEY));

        URI uri = URI.create("hdfs://" + configuration.getString(HadoopUnitConfig.HDFS_NAMENODE_HOST_KEY) + ":" + configuration.getInt(HadoopUnitConfig.HDFS_NAMENODE_PORT_KEY));

        try {
            fileSystem = FileSystem.get(uri, conf);
        } catch (IOException e) {
            LOGGER.error("unable to create FileSystem", e);
        }
    }

    private void loadConfig() throws ConfigException {
        try {
            configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
        } catch (ConfigurationException e) {
            throw new ConfigException("bad config", e);
        }

        port = configuration.getInt(HadoopUnitConfig.HDFS_NAMENODE_PORT_KEY);
        httpPort = configuration.getInt(HadoopUnitConfig.HDFS_NAMENODE_HTTP_PORT_KEY);
        hdfsHost = configuration.getString(HadoopUnitConfig.HDFS_NAMENODE_HOST_KEY);
    }


    public FileSystem getFileSystem() {
        return fileSystem;
    }

}
