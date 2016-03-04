package fr.jetoile.hadoopunit.test.hive;

import com.ninja_squad.dbsetup.destination.Destination;
import com.ninja_squad.dbsetup.destination.DriverManagerDestination;
import fr.jetoile.hadoopunit.Config;
import fr.jetoile.hadoopunit.exception.ConfigException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Utility class when using a default.properties file which contains connection's parameters
 */
public enum HiveConnectionUtils {
    INSTANCE;

    final private Logger LOGGER = LoggerFactory.getLogger(HiveConnectionUtils.class);

    private DriverManagerDestination driverManagerDestination;
    private Configuration configuration;
    private String databaseName;
    private String host;
    private int port;

    HiveConnectionUtils() {
        try {
            loadConfig();
        } catch (ConfigException e) {
            System.exit(-1);
        }
        driverManagerDestination = new DriverManagerDestination(
                "jdbc:hive2://" + host + ":" + port + "/" + databaseName,
                "user",
                "pass");
    }

    private void loadConfig() throws ConfigException {
        try {
            configuration = new PropertiesConfiguration("default.properties");
        } catch (ConfigurationException e) {
            throw new ConfigException("bad config", e);
        }

        port = configuration.getInt(Config.HIVE_SERVER2_PORT_KEY);
        host = configuration.getString(Config.HIVE_SERVER2_HOSTNAME_KEY);
        databaseName = configuration.getString(Config.HIVE_TEST_DATABASE_NAME_KEY);
    }

    public Destination getDestination() {
        return driverManagerDestination;
    }

    public Connection getConnection() {
        try {
            return driverManagerDestination.getConnection();
        } catch (SQLException e) {
            LOGGER.error("unable to create hive connection", e);
            return null;
        }
    }
}
