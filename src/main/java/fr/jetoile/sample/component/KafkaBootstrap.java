package fr.jetoile.sample.component;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.impl.KafkaLocalBroker;
import fr.jetoile.sample.HadoopUtils;
import fr.jetoile.sample.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public enum KafkaBootstrap implements Bootstrap {
    INSTANCE;

    final private Logger LOGGER = LoggerFactory.getLogger(KafkaBootstrap.class);

    private KafkaLocalBroker kafkaLocalCluster;

    private State state = State.STOPPED;

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
        HadoopUtils.setHadoopHome();
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
        if (state == State.STOPPED) {
            state = State.STARTING;
            LOGGER.info("{} is starting", this.getClass().getName());

            init();
            build();
            try {
                kafkaLocalCluster.start();
            } catch (Exception e) {
                LOGGER.error("unable to start kafka", e);
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
                kafkaLocalCluster.stop(true);
            } catch (Exception e) {
                LOGGER.error("unable to stop kafka", e);
            }
            state = State.STOPPED;
            LOGGER.info("{} is stopped", this.getClass().getName());
        }

        return this;

    }

    @Override
    public org.apache.hadoop.conf.Configuration getConfiguration() {
        throw new UnsupportedOperationException("the method getConfiguration can not be called on KafkaBootstrap");
    }

}
