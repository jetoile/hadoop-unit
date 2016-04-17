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

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class HadoopUtils {


    private static final Logger LOG = LoggerFactory.getLogger(HadoopUtils.class);
    private static Configuration configuration;

    public static void setHadoopHome() {

        if (StringUtils.isEmpty(System.getenv("HADOOP_HOME"))) {

            try {
                configuration = new PropertiesConfiguration("default.properties");
            } catch (ConfigurationException e) {
                LOG.error("unable to load default.properties", e);
            }

            String hadoop_home = configuration.getString("HADOOP_HOME");

            LOG.info("Setting hadoop.home.dir: {}", hadoop_home);
            System.setProperty("HADOOP_HOME", hadoop_home);

        } else {
            System.setProperty("HADOOP_HOME", System.getenv("HADOOP_HOME"));
        }
    }

    public static void printBanner(PrintStream out) {
        try {
            InputStream banner = HadoopUtils.class.getResourceAsStream("/banner.txt");

            BufferedReader br = new BufferedReader(new InputStreamReader(banner));
            String line = null;
            while ((line = br.readLine()) != null) {
                out.println(line);
            }
        }
        catch (Exception ex) {
            LOG.warn("Banner not printable", ex);
        }
    }
}
