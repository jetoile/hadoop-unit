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

package fr.jetoile.hadoopunit.component.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.HadoopBootstrap;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import fr.jetoile.hadoopunit.exception.NotFoundServiceException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ElasticSearchBootstrapTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchBootstrapTest.class);

    static private Configuration configuration;


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

    @Test
    public void elasticSearchShouldStart() throws NotFoundServiceException, IOException {

        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(configuration.getString(HadoopUnitConfig.ELASTICSEARCH_IP_KEY)), configuration.getInt(HadoopUnitConfig.ELASTICSEARCH_TCP_PORT_KEY)));

        ObjectMapper mapper = new ObjectMapper();

        Sample sample = new Sample("value", 0.33, 3);

        String jsonString = mapper.writeValueAsString(sample);

        // indexing document
        IndexResponse ir = client.prepareIndex("test_index", "type").setSource(jsonString).setId("1").execute().actionGet();
        client.admin().indices().prepareRefresh("test_index").execute().actionGet();

        assertNotNull(ir);

        GetResponse gr = client.prepareGet("test_index", "type", "1").execute().actionGet();

        assertNotNull(gr);
        assertEquals(gr.getSourceAsString(), "{\"value\":\"value\",\"size\":0.33,\"price\":3.0}");

    }

}

class Sample implements Serializable {
    private static final long serialVersionUID = 1L;

    private String value;
    private double size;
    private double price;

    public Sample(String value, double size, double price) {
        this.value = value;
        this.size = size;
        this.price = price;
    }

    public double getSize() {
        return size;
    }

    public void setSize(double size) {
        this.size = size;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object obj) {

        if (obj == null) return false;
        if (!(obj instanceof Sample)) return false;

        Sample sample = (Sample) obj;

        if (this.value != sample.value && this.value != null && !this.value.equals(sample.value)) return false;

        if (this.size != sample.size ) return false;
        if (this.price != sample.price) return false;

        return true;
    }
}
