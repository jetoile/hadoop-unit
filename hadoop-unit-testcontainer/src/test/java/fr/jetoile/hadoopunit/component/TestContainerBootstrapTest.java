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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import static org.assertj.core.api.Assertions.assertThat;


public class TestContainerBootstrapTest {


    private static final Logger LOGGER = LoggerFactory.getLogger(TestContainerBootstrapTest.class);

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
    public static void tearDown() {
        HadoopBootstrap.INSTANCE.stopAll();
    }

    @Test
    public void testContainerShouldStart() throws NotFoundServiceException, IOException {
        TestContainerBootstrap testcontainer = (TestContainerBootstrap) HadoopBootstrap.INSTANCE.getService("TESTCONTAINER");

        String containerIpAddress = testcontainer.getContainer().getContainerIpAddress();
        Integer firstMappedPort = testcontainer.getContainer().getFirstMappedPort();

        Socket pingSocket = new Socket("localhost", firstMappedPort);
        PrintWriter out = new PrintWriter(pingSocket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(pingSocket.getInputStream()));

        out.println("ping");
        String response = in.readLine();
        out.close();
        in.close();
        pingSocket.close();

        assertThat(response).isEqualTo("42");
        assertThat(containerIpAddress).isEqualTo("localhost");
        assertThat(firstMappedPort).isEqualTo(21300);
    }
}