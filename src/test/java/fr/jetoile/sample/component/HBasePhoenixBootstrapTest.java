package fr.jetoile.sample.component;


import com.github.sakserv.minicluster.config.ConfigVars;
import fr.jetoile.sample.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.Jdbc7Shim;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.sql.Connection;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class HBasePhoenixBootstrapTest {
    static private Logger LOGGER = LoggerFactory.getLogger(HBasePhoenixBootstrapTest.class);

    static private Bootstrap zookeeper;
    static private Bootstrap hbase;
    static private Configuration configuration;


    @BeforeClass
    public static void setup() throws Exception {
        try {
            configuration = new PropertiesConfiguration("default.properties");
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }
//
//        zookeeper = ZookeeperBootstrap.INSTANCE.start();
//        hbase = HBaseBootstrap.INSTANCE.start();
    }
//
//    @AfterClass
//    public static void tearDown() throws Exception {
//        hbase.stop();
//        zookeeper.stop();
//    }


    @Test
    public void hBaseShouldStart() throws Exception {

        HBaseTestingUtility testingUtility = new HBaseTestingUtility();
        testingUtility.getConfiguration().setStrings(
                CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
                HutReadEndpoint.class.getName());

        String zkConfig = configuration.getString(ConfigVars.ZOOKEEPER_HOST_KEY) + ":" + configuration.getInt(ConfigVars.ZOOKEEPER_PORT_KEY);
//        System.setProperty("hbase.zookeeper.property.clientPort", configuration.getString(ConfigVars.ZOOKEEPER_PORT_KEY));

        org.apache.hadoop.conf.Configuration hbaseConfiguration = HBaseConfiguration.create();
        hbaseConfiguration.set("hbase.zookeeper.quorum", configuration.getString(ConfigVars.ZOOKEEPER_HOST_KEY));
        hbaseConfiguration.setInt("hbase.zookeeper.property.clientPort", configuration.getInt(ConfigVars.ZOOKEEPER_PORT_KEY));
        hbaseConfiguration.set("hbase.master", "127.0.0.1:" + configuration.getInt(ConfigVars.HBASE_MASTER_PORT_KEY));
        hbaseConfiguration.set("zookeeper.znode.parent", configuration.getString(ConfigVars.HBASE_ZNODE_PARENT_KEY));
        final HBaseAdmin admin = new HBaseAdmin(hbaseConfiguration);

//        admin.enableTable("SYSTEM.CATALOG");
//        admin.disableTable("SYSTEM.CATALOG");

        Statement stmt = null;
        ResultSet rset = null;

        Properties prop = new Properties();
        prop.setProperty("hbase.zookeeper.quorum", configuration.getString(ConfigVars.ZOOKEEPER_HOST_KEY));
        prop.setProperty("hbase.zookeeper.property.clientPort", configuration.getString(ConfigVars.ZOOKEEPER_PORT_KEY));
        prop.setProperty("hbase.master", "127.0.0.1:" + configuration.getInt(ConfigVars.HBASE_MASTER_PORT_KEY));
        prop.setProperty("zookeeper.znode.parent", configuration.getString(ConfigVars.HBASE_ZNODE_PARENT_KEY));

        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        Connection con = DriverManager.getConnection("jdbc:phoenix:" + zkConfig + ":" +  configuration.getString(ConfigVars.HBASE_ZNODE_PARENT_KEY), prop);

        stmt = con.createStatement();

        stmt.executeUpdate("create table test (mykey integer not null primary key, mycolumn varchar)");
        stmt.executeUpdate("upsert into test values (1,'Hello')");
        stmt.executeUpdate("upsert into test values (2,'World!')");
        con.commit();

        PreparedStatement statement = con.prepareStatement("select * from test");
        rset = statement.executeQuery();
        while (rset.next()) {
            System.out.println(rset.getString("mycolumn"));
        }
        statement.close();
        con.close();



    }

}
