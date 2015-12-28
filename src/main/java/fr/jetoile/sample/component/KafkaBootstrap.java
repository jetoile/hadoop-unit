package fr.jetoile.sample.component;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.impl.HdfsLocalCluster;
import com.github.sakserv.minicluster.impl.KafkaLocalBroker;
import fr.jetoile.sample.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public enum KafkaBootstrap implements Bootstrap {
    INSTANCE;

    final private Logger LOGGER = LoggerFactory.getLogger(KafkaBootstrap.class);

    private KafkaLocalBroker kafkaLocalCluster;

    private Configuration configuration;
    private String zookeeperConnectionString;
    private String host;
    private int port;
    private int brokerId;
    private String tmpDirectory;


    KafkaBootstrap() {
        if (kafkaLocalCluster == null) {
            try {
                loadConfig();
            } catch (BootstrapException e) {
                LOGGER.error("unable to load configuration", e);
            }
            init();
            build();
        }
    }

    private void init() {

    }

    private void build() {
        kafkaLocalCluster = new KafkaLocalBroker.Builder()
                .setKafkaHostname(host)
                .setKafkaPort(port)
                .setKafkaBrokerId(brokerId)
                .setKafkaProperties(new Properties())
                .setKafkaTempDir(tmpDirectory)
                .setZookeeperConnectionString(zookeeperConnectionString)
                .build();
    }

    private void loadConfig() throws BootstrapException {
        try {
            configuration = new PropertiesConfiguration("default.properties");
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }
        host = configuration.getString(ConfigVars.KAFKA_HOSTNAME_KEY);
        port = configuration.getInt(ConfigVars.KAFKA_PORT_KEY);
        brokerId = configuration.getInt(ConfigVars.KAFKA_TEST_BROKER_ID_KEY);
        tmpDirectory = configuration.getString(ConfigVars.KAFKA_TEST_TEMP_DIR_KEY);
        zookeeperConnectionString = configuration.getString(ConfigVars.ZOOKEEPER_HOST_KEY) + ":" + configuration.getInt(ConfigVars.ZOOKEEPER_PORT_KEY);

    }

    @Override
    public Bootstrap start() {
        try {
            kafkaLocalCluster.start();
        } catch (Exception e) {
            LOGGER.error("unable to start kafka", e);
        }
        return this;
    }

    @Override
    public Bootstrap stop() {
        try {
            kafkaLocalCluster.stop(true);
        } catch (Exception e) {
            LOGGER.error("unable to stop kafka", e);
        }
        return this;

    }

    @Override
    public org.apache.hadoop.conf.Configuration getConfiguration() {
        throw new UnsupportedOperationException("the method getConfiguration can not be called on KafkaBootstrap");
    }

}
