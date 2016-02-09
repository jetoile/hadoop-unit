package fr.jetoile.sample.component;


import fr.jetoile.sample.Config;
import fr.jetoile.sample.HadoopBootstrap;
import fr.jetoile.sample.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

@Ignore
public class HiveServer2BootstrapTest {

    static private Logger LOGGER = LoggerFactory.getLogger(HiveServer2Bootstrap.class);

    static private Configuration configuration;

    @BeforeClass
    public static void setup() throws Exception {
        try {
            configuration = new PropertiesConfiguration("default.properties");
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }

        HadoopBootstrap.INSTANCE.startAll();

    }

    @AfterClass
    public static void tearDown() throws Exception {
        HadoopBootstrap.INSTANCE.stopAll();
    }

    @Test
    public void hiveServer2ShouldStart() throws InterruptedException, ClassNotFoundException, SQLException {

//        assertThat(Utils.available("127.0.0.1", 20103)).isFalse();

        // Load the Hive JDBC driver
        LOGGER.info("HIVE: Loading the Hive JDBC Driver");
        Class.forName("org.apache.hive.jdbc.HiveDriver");

        //
        // Create an ORC table and describe it
        //
        // Get the connection
        Connection con = DriverManager.getConnection("jdbc:hive2://" +
                        configuration.getString(Config.HIVE_SERVER2_HOSTNAME_KEY) + ":" +
                        configuration.getInt(Config.HIVE_SERVER2_PORT_KEY) + "/" +
                        configuration.getString(Config.HIVE_TEST_DATABASE_NAME_KEY),
                "user",
                "pass");

        // Create the DB
        Statement stmt;
        try {
            String createDbDdl = "CREATE DATABASE IF NOT EXISTS " +
                    configuration.getString(Config.HIVE_TEST_DATABASE_NAME_KEY);
            stmt = con.createStatement();
            LOGGER.info("HIVE: Running Create Database Statement: {}", createDbDdl);
            stmt.execute(createDbDdl);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Drop the table incase it still exists
        String dropDdl = "DROP TABLE " + configuration.getString(Config.HIVE_TEST_DATABASE_NAME_KEY) + "." +
                configuration.getString(Config.HIVE_TEST_TABLE_NAME_KEY);
        stmt = con.createStatement();
        LOGGER.info("HIVE: Running Drop Table Statement: {}", dropDdl);
        stmt.execute(dropDdl);

        // Create the ORC table
        String createDdl = "CREATE TABLE IF NOT EXISTS " +
                configuration.getString(Config.HIVE_TEST_DATABASE_NAME_KEY) + "." +
                configuration.getString(Config.HIVE_TEST_TABLE_NAME_KEY) + " (id INT, msg STRING) " +
                "PARTITIONED BY (dt STRING) " +
                "CLUSTERED BY (id) INTO 16 BUCKETS " +
                "STORED AS ORC tblproperties(\"orc.compress\"=\"NONE\")";
        stmt = con.createStatement();
        LOGGER.info("HIVE: Running Create Table Statement: {}", createDdl);
        stmt.execute(createDdl);

        // Issue a describe on the new table and display the output
        LOGGER.info("HIVE: Validating Table was Created: ");
        ResultSet resultSet = stmt.executeQuery("DESCRIBE FORMATTED " +
                configuration.getString(Config.HIVE_TEST_TABLE_NAME_KEY));
        int count = 0;
        while (resultSet.next()) {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                System.out.print(resultSet.getString(i));
            }
            System.out.println();
            count++;
        }
        assertEquals(33, count);

        // Drop the table
        dropDdl = "DROP TABLE " + configuration.getString(Config.HIVE_TEST_DATABASE_NAME_KEY) + "." +
                configuration.getString(Config.HIVE_TEST_TABLE_NAME_KEY);
        stmt = con.createStatement();
        LOGGER.info("HIVE: Running Drop Table Statement: {}", dropDdl);
        stmt.execute(dropDdl);
    }
}
