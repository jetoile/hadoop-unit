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

import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class KnoxBootstrapTopologyTest {

    static private Configuration configuration;

    @BeforeClass
    public static void setup() throws BootstrapException {
        try {
            configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }
    }

    @Test
    public void generatedTopology_should_be_ok_with_3_services() throws IOException {
        KnoxBootstrap knoxBootstrap = new KnoxBootstrap();
        List<KnoxService> services = Arrays.asList(KnoxService.NAMENODE, KnoxService.WEBHDFS, KnoxService.WEBHBASE);
        String topology = knoxBootstrap.getTopology(services);

        assertEquals(topology.trim(), "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n" +
                "<topology>\n" +
                "    <gateway>\n" +
                "        <provider>\n" +
                "            <role>authentication</role>\n" +
                "            <enabled>false</enabled>\n" +
                "        </provider>\n" +
                "        <provider>\n" +
                "            <role>identity-assertion</role>\n" +
                "            <enabled>false</enabled>\n" +
                "        </provider>\n" +
                "    </gateway>\n" +
                "    <service>\n" +
                "        <role>NAMENODE</role>\n" +
                "        <url>hdfs://" + configuration.getString(HadoopUnitConfig.HDFS_NAMENODE_HOST_KEY) + ":20112</url>\n" +
                "    </service>\n" +
                "    <service>\n" +
                "        <role>WEBHDFS</role>\n" +
                "        <url>http://" + configuration.getString(HadoopUnitConfig.HDFS_NAMENODE_HOST_KEY) + ":50070/webhdfs</url>\n" +
                "    </service>\n" +
                "    <service>\n" +
                "        <role>WEBHBASE</role>\n" +
                "        <url>http://" + configuration.getString(HadoopUnitConfig.HBASE_REST_HOST_KEY) + ":28000</url>\n" +
                "    </service>\n" +
                "</topology>");
    }

    @Test
    public void generatedTopology_should_be_ok_with_1_service() throws IOException {
        KnoxBootstrap knoxBootstrap = new KnoxBootstrap();
        List<KnoxService> services = Arrays.asList(KnoxService.WEBHBASE);
        String topology = knoxBootstrap.getTopology(services);

        assertEquals(topology.trim(), "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n" +
                "<topology>\n" +
                "    <gateway>\n" +
                "        <provider>\n" +
                "            <role>authentication</role>\n" +
                "            <enabled>false</enabled>\n" +
                "        </provider>\n" +
                "        <provider>\n" +
                "            <role>identity-assertion</role>\n" +
                "            <enabled>false</enabled>\n" +
                "        </provider>\n" +
                "    </gateway>\n" +
                "    <service>\n" +
                "        <role>WEBHBASE</role>\n" +
                "        <url>http://" + configuration.getString(HadoopUnitConfig.HBASE_REST_HOST_KEY) + ":28000</url>\n" +
                "    </service>\n" +
                "</topology>");
    }
}