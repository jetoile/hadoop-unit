package fr.jetoile.hadoop.test.hive;

import com.ninja_squad.dbsetup.Operations;
import com.ninja_squad.dbsetup.destination.Destination;
import com.ninja_squad.dbsetup.operation.Operation;
import org.junit.After;
import org.junit.Before;

import java.sql.Connection;
import java.sql.SQLException;

import static com.ninja_squad.dbsetup.Operations.sequenceOf;
import static com.ninja_squad.dbsetup.Operations.sql;

/**
 * User: khanh
 * To change this template use File | Settings | File Templates.
 */
public class HiveSetupTest {
    public static final Operation CREATE_TABLES =
            sequenceOf(sql("CREATE EXTERNAL TABLE IF NOT EXISTS default.test(id INT, value STRING) " +
                    " ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'" +
                    " STORED AS TEXTFILE" +
                    " LOCATION 'hdfs://localhost:20112/khanh/test'"));

    public static Destination DESTINATION = new HiveDriverManagerDestination(
            "jdbc:hive2://localhost:20103/default",
            "user",
            "pass"
    );

    private Connection connection;

    @Before
    public void prepare() throws SQLException {
        new HiveSetup(DESTINATION, Operations.sequenceOf(CREATE_TABLES)).launch();
        connection = DESTINATION.getConnection();
    }

    @After
    public void cleanup() throws SQLException {
        connection.close();
    }

}