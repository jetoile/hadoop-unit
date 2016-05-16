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
package fr.jetoile.hadoopunit;

import fr.jetoile.hadoopunit.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;

public class HadoopStandaloneBootstrap {

    final private static Logger LOGGER = LoggerFactory.getLogger(HadoopStandaloneBootstrap.class);
    static private Configuration configuration;


    public static void main(String[] args) throws BootstrapException {
        try {
            configuration = new PropertiesConfiguration("hadoop.properties");
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }

        HadoopBootstrap bootstrap = HadoopBootstrap.INSTANCE;


        bootstrap.componentsToStart = bootstrap.componentsToStart.stream().filter(c ->
                configuration.containsKey(c.getName().toLowerCase()) && configuration.getBoolean(c.getName().toLowerCase())
        ).collect(Collectors.toList());

        bootstrap.componentsToStop = bootstrap.componentsToStop.stream().filter(c ->
                configuration.containsKey(c.getName().toLowerCase()) && configuration.getBoolean(c.getName().toLowerCase())
        ).collect(Collectors.toList());

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                LOGGER.info("All services are going to be stopped");
                bootstrap.stopAll();
            }
        });

        bootstrap.startAll();


    }
}
