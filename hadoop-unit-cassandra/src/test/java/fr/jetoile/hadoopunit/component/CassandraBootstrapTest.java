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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import fr.jetoile.hadoopunit.Component;
import fr.jetoile.hadoopunit.Config;
import fr.jetoile.hadoopunit.HadoopBootstrap;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import fr.jetoile.hadoopunit.exception.NotFoundServiceException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CassandraBootstrapTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraBootstrapTest.class);

    static private Configuration configuration;


    @BeforeClass
    public static void setup() throws BootstrapException {
        HadoopBootstrap.INSTANCE.startAll();

        try {
            configuration = new PropertiesConfiguration(Config.DEFAULT_PROPS_FILE);
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }
    }


    @AfterClass
    public static void tearDown() throws BootstrapException {
        HadoopBootstrap.INSTANCE.stopAll();
    }

    @Before
    public void setUp() throws NotFoundServiceException {
        Bootstrap cassandra = HadoopBootstrap.INSTANCE.getService(Component.CASSANDRA);
        Session session = ((CassandraBootstrap) cassandra).getCassandraSession();

        session.execute("create KEYSPACE test WITH replication = {'class': 'SimpleStrategy' , 'replication_factor': '1' }");
        session.execute("CREATE TABLE test.test (user text, value text, PRIMARY KEY (user))");
        session.execute("insert into test.test(user, value) values('user1', 'value1')");
    }

    @After
    public void teardown() throws NotFoundServiceException {
        Bootstrap cassandra = HadoopBootstrap.INSTANCE.getService(Component.CASSANDRA);
        Session session = ((CassandraBootstrap) cassandra).getCassandraSession();

        session.execute("drop table test.test");
        session.execute("drop keyspace test");
    }

    @Test
    public void cassandraShouldStart() throws NotFoundServiceException {
        Bootstrap cassandra = HadoopBootstrap.INSTANCE.getService(Component.CASSANDRA);
        Session session = ((CassandraBootstrap) cassandra).getCassandraSession();

        ResultSet execute = session.execute("select * from test.test");

        List<Row> res = execute.all();
        assertEquals(res.size(), 1);
        assertEquals(res.get(0).getString("user"), "user1");
        assertEquals(res.get(0).getString("value"), "value1");
    }

    @Test
    public void cassandraShouldStartWithRealDriver() throws NotFoundServiceException {
        Cluster cluster = Cluster.builder()
                .addContactPoints(configuration.getString(Config.CASSANDRA_IP_KEY)).withPort(configuration.getInt(Config.CASSANDRA_PORT_KEY)).build();
        Session session = cluster.connect();

        session.execute("insert into test.test(user, value) values('user2', 'value2')");

        ResultSet execute = session.execute("select * from test.test");

        List<Row> res = execute.all();
        assertEquals(res.size(), 2);
        assertEquals(res.get(0).getString("user"), "user2");
        assertEquals(res.get(0).getString("value"), "value2");
        assertEquals(res.get(1).getString("user"), "user1");

    }
}