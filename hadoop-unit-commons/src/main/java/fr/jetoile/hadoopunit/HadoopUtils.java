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
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class HadoopUtils {


    private static final Logger LOG = LoggerFactory.getLogger(HadoopUtils.class);
    private static Configuration configuration;

    public static void setHadoopHome() {

        // Set hadoop.home.dir to point to the windows lib dir
        if (System.getProperty("os.name").startsWith("Windows")) {

            if (StringUtils.isEmpty(System.getenv("HADOOP_HOME"))) {

                try {
                    configuration = new PropertiesConfiguration(Config.DEFAULT_PROPS_FILE);
                } catch (ConfigurationException e) {
                    LOG.error("unable to load {}", Config.DEFAULT_PROPS_FILE, e);
                }

                String hadoop_home = configuration.getString("HADOOP_HOME");

                LOG.info("Setting hadoop.home.dir: {}", hadoop_home);
                if (hadoop_home == null) {
                    LOG.error("HADOOP_HOME should be set or informed into hadoop-unit-default.properties");
                    System.exit(-1);
                } else {
                    System.setProperty("HADOOP_HOME", hadoop_home);
                }

            } else {
                System.setProperty("HADOOP_HOME", System.getenv("HADOOP_HOME"));
            }

            String windowsLibDir = System.getenv("HADOOP_HOME");

            LOG.info("WINDOWS: Setting hadoop.home.dir: {}", windowsLibDir);
            System.setProperty("hadoop.home.dir", windowsLibDir);
            System.load(new File(windowsLibDir + Path.SEPARATOR + "bin" + Path.SEPARATOR + "hadoop.dll").getAbsolutePath());
            System.load(new File(windowsLibDir + Path.SEPARATOR + "bin" + Path.SEPARATOR + "hdfs.dll").getAbsolutePath());
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
