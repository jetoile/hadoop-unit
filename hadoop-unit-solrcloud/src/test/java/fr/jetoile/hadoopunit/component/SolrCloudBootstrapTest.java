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


import fr.jetoile.hadoopunit.HadoopBootstrap;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import fr.jetoile.hadoopunit.exception.NotFoundServiceException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static junit.framework.TestCase.assertNotNull;
import static org.fest.assertions.Assertions.assertThat;

public class SolrCloudBootstrapTest {
    static private Logger LOGGER = LoggerFactory.getLogger(SolrCloudBootstrapTest.class);

    static private Configuration configuration;

    @BeforeClass
    public static void setup() throws Exception {
        try {
            configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
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
    public void solrCloudShouldStart() throws IOException, SolrServerException, KeeperException, InterruptedException, NotFoundServiceException {

        String collectionName = configuration.getString(SolrCloudConfig.SOLR_COLLECTION_NAME);

//        String zkHostString = configuration.getString(Config.ZOOKEEPER_HOST_KEY) + ":" + configuration.getInt(Config.ZOOKEEPER_PORT_KEY);
//        CloudSolrClient client = new CloudSolrClient(zkHostString);
        CloudSolrClient client = ((SolrCloudBootstrap) HadoopBootstrap.INSTANCE.getService("SOLRCLOUD")).getClient();

        for (int i = 0; i < 1000; ++i) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("cat", "book");
            doc.addField("id", "book-" + i);
            doc.addField("name", "The Legend of the Hobbit part " + i);
            client.add(collectionName, doc);
            if (i % 100 == 0) client.commit(collectionName);  // periodically flush
        }
        client.commit("collection1");

        SolrDocument collection1 = client.getById(collectionName, "book-1");

        assertNotNull(collection1);

        assertThat(collection1.getFieldValue("name")).isEqualTo("The Legend of the Hobbit part 1");


        client.close();
    }

}
