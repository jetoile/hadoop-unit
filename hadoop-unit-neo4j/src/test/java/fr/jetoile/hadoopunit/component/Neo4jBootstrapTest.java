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

import fr.jetoile.hadoopunit.Component;
import fr.jetoile.hadoopunit.HadoopBootstrap;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import fr.jetoile.hadoopunit.exception.NotFoundServiceException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.*;
import org.neo4j.driver.v1.*;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Neo4jBootstrapTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jBootstrapTest.class);

    private enum RelTypes implements RelationshipType {
        KNOWS
    }

    static private Configuration configuration;

    private GraphDatabaseService graphDb;


    @BeforeClass
    public static void setup() throws BootstrapException {
        HadoopBootstrap.INSTANCE.startAll();

        try {
            configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
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
        Bootstrap neo4j = HadoopBootstrap.INSTANCE.getService(Component.NEO4J);
        graphDb = ((Neo4jBootstrap) neo4j).getNeo4jGraph();
    }

    @After
    public void teardown() throws NotFoundServiceException {
    }

    @Test
    public void neo4jShouldStart() {
        try (Transaction tx = graphDb.beginTx()) {
            Node firstNode = graphDb.createNode();
            firstNode.setProperty("message", "Hello, ");
            Node secondNode = graphDb.createNode();
            secondNode.setProperty("message", "World!");

            Relationship relationship = firstNode.createRelationshipTo(secondNode, RelTypes.KNOWS);
            relationship.setProperty("message", "brave Neo4j ");

            tx.success();

            assertEquals("Hello, brave Neo4j World!", "" + firstNode.getProperty("message") + relationship.getProperty("message") + secondNode.getProperty("message"));
        }
    }

    @Test
    public void traversal_query_should_success() {
        int numberOfFriends = 0;

        try (Transaction tx = graphDb.beginTx()) {

            Node neoNode = graphDb.createNode();
            neoNode.setProperty("name", "Hello, ");
            Node secondNode = graphDb.createNode();
            secondNode.setProperty("name", "World!");
            Node thirdNode = graphDb.createNode();
            secondNode.setProperty("name", "World2!");
            Relationship relationship = neoNode.createRelationshipTo(secondNode, RelTypes.KNOWS);
            Relationship relationship2 = neoNode.createRelationshipTo(thirdNode, RelTypes.KNOWS);
            relationship.setProperty("message", "brave Neo4j ");
            relationship2.setProperty("message", "brave Neo4j2 ");
            tx.success();

            Traverser friendsTraverser = getFriends(graphDb, neoNode);
            for (Path friendPath : friendsTraverser) {
                numberOfFriends++;
            }

            assertEquals(2, numberOfFriends);
        }
    }

    @Test
    public void cypher_query_should_sucess() {

        try (Transaction tx = graphDb.beginTx()) {
            Node myNode = graphDb.createNode();
            myNode.setProperty("name", "my node");
            tx.success();
        }

        List<String> res = new ArrayList<>();
        try (Transaction ignored = graphDb.beginTx();
             Result result = graphDb.execute("match (n {name: 'my node'}) return n, n.name")) {
            while (result.hasNext()) {
                Map<String, Object> row = result.next();
                for (Map.Entry<String, Object> column : row.entrySet()) {
                    res.add(column.getKey() + ": " + column.getValue());
                    LOGGER.debug(column.getKey() + ": " + column.getValue());
                }
            }
        }

        assertEquals(2, res.size());
        assertTrue(res.toString().contains("n.name: my node"));
    }

    @Test
    public void neo4jShouldStartWithRealDriver() {

        Driver driver = GraphDatabase.driver(
                "bolt://localhost:13533",
                Config.build()
                        .withEncryptionLevel(Config.EncryptionLevel.NONE)
                        .toConfig()
        );

        List<Record> results = new ArrayList<>();
        try (Session session = driver.session()) {
            session.run("CREATE (person:Person {name: {name}, title:'King'})", Values.parameters("name", "Arthur"));

            StatementResult result = session.run("MATCH (a:Person) WHERE a.name = 'Arthur' RETURN a.name AS name, a.title AS title");
            while (result.hasNext()) {
                Record record = result.next();
                results.add(record);
                LOGGER.debug(record.get("title").asString() + " " + record.get("name").asString());
            }
        }

        assertEquals(1, results.size());
        assertEquals("King", results.get(0).get("title").asString());
        assertEquals("Arthur", results.get(0).get("name").asString());
    }

    private Traverser getFriends(GraphDatabaseService graphDb, final Node person) {
        TraversalDescription td = graphDb.traversalDescription()
                .breadthFirst()
                .relationships(RelTypes.KNOWS, Direction.OUTGOING)
                .evaluator(Evaluators.excludeStartPosition());
        return td.traverse(person);
    }


}


