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

package fr.jetoile.hadoopunit.component.mongodb;

import com.mongodb.*;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.HadoopBootstrap;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.Date;

import static org.junit.Assert.assertEquals;


public class MongoDbBootstrapTest {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDbBootstrapTest.class);

    static private Configuration configuration;

    @BeforeClass
    public static void setUp() throws Exception {
        HadoopBootstrap.INSTANCE.startAll();

        try {
            configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        HadoopBootstrap.INSTANCE.stopAll();
    }

    @Test
    public void mongodbShouldStart() throws UnknownHostException {
        MongoClient mongo = new MongoClient(configuration.getString(HadoopUnitConfig.MONGO_IP_KEY), configuration.getInt(HadoopUnitConfig.MONGO_PORT_KEY));

        DB db = mongo.getDB(configuration.getString(HadoopUnitConfig.MONGO_DATABASE_NAME_KEY));
        DBCollection col = db.createCollection(configuration.getString(HadoopUnitConfig.MONGO_COLLECTION_NAME_KEY),
                new BasicDBObject());

        col.save(new BasicDBObject("testDoc", new Date()));
        LOG.info("MONGODB: Number of items in collection: {}", col.count());
        assertEquals(1, col.count());

        DBCursor cursor = col.find();
        while(cursor.hasNext()) {
            LOG.info("MONGODB: Document output: {}", cursor.next());
        }
        cursor.close();
    }
}