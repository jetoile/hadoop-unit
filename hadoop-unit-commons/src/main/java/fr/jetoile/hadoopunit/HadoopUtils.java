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
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.fusesource.jansi.Ansi;
import org.fusesource.jansi.AnsiConsole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.util.UUID;

import static org.fusesource.jansi.Ansi.Color.BLUE;
import static org.fusesource.jansi.Ansi.ansi;

public enum HadoopUtils {
    INSTANCE;

    public static final String WINUTILS_EXE = "winutils.exe";
    public static final String HADOOP_HOME = "HADOOP_HOME";
    public static final String HADOOP_DLL = "/hadoop.dll";
    public static final String LIBWINUTILS_LIB = "/libwinutils.lib";
    public static final String HDFS_DLL = "/hdfs.dll";

    private final Logger LOGGER = LoggerFactory.getLogger(HadoopUtils.class);
    private Configuration configuration;

    private HadoopUtils() {
        // Set hadoop.home.dir to point to the windows lib dir
        if (System.getProperty("os.name").startsWith("Windows")) {

            if (StringUtils.isEmpty(System.getenv(HADOOP_HOME))) {

                try {
                    configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
                } catch (ConfigurationException e) {
                    LOGGER.error("unable to load {}", HadoopUnitConfig.DEFAULT_PROPS_FILE, e);
                }

                String hadoop_home = configuration.getString(HADOOP_HOME);

                LOGGER.info("Setting hadoop.home.dir: {}", hadoop_home);
                if (hadoop_home == null) {
                    LOGGER.error("HADOOP_HOME should be set or informed into hadoop-unit-default.properties");
                    System.exit(-1);
                } else {
                    System.setProperty(HADOOP_HOME, hadoop_home);
                }

            } else {
                System.setProperty(HADOOP_HOME, System.getenv(HADOOP_HOME));
            }

            String windowsLibDir = System.getenv(HADOOP_HOME);

            try {
                extractAndMoveWinUtils(windowsLibDir);
                extractAndLoadDll(HADOOP_DLL);
                extractAndLoadDll(HDFS_DLL);
                extractAndLoadDll(LIBWINUTILS_LIB);
            } catch (IOException e) {
                LOGGER.error("unable to load windows dll", e);
            }
        }
    }

    public static void printColorLine(PrintStream out, Ansi.Color color, String line) {
        AnsiConsole.systemInstall();
        out.println(ansi().fg(color).a(line).reset());
        AnsiConsole.systemUninstall();
    }

    private void extractAndLoadDll(String lib) throws IOException {
        InputStream in = HadoopUtils.class.getResourceAsStream(lib);
        // always write to different location
        File fileOut = new File(System.getProperty("java.io.tmpdir") + File.separator + UUID.randomUUID() + File.separator + lib);
        System.out.println("Writing dll to: " + fileOut.getAbsolutePath());

        OutputStream out = FileUtils.openOutputStream(fileOut);
        IOUtils.copy(in, out);
        in.close();
        out.close();
        try {
            System.load(fileOut.getAbsolutePath());
        } catch (UnsatisfiedLinkError e) {
            //ignore it
        }
    }

    private void extractAndMoveWinUtils(String path) throws IOException {
        InputStream in = HadoopUtils.class.getResourceAsStream("/" + WINUTILS_EXE);
        // always write to different location
        File fileOut = new File(path + File.separator + "bin" + File.separator + WINUTILS_EXE);
        LOGGER.info("Writing {} to: {}", WINUTILS_EXE, fileOut.getAbsolutePath());

        OutputStream out = FileUtils.openOutputStream(fileOut);
        IOUtils.copy(in, out);
        in.close();
        out.close();
    }

    public void setHadoopHome() {

    }

    public void printBanner(PrintStream out) {
        try {
            InputStream banner = HadoopUtils.class.getResourceAsStream("/banner.txt");

            BufferedReader br = new BufferedReader(new InputStreamReader(banner));
            String line = null;
            while ((line = br.readLine()) != null) {
                printColorLine(out, BLUE, line);
            }
        } catch (Exception ex) {
            LOGGER.warn("Banner not printable", ex);
        }
    }

    public Configuration loadConfigFile(URL url) throws BootstrapException {
        try {
            if (url == null) {
                configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
            } else {
                configuration = new PropertiesConfiguration(url);
            }
            return configuration;
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }
    }
}
