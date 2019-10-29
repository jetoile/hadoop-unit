package fr.jetoile.hadoopunit.component;

import fr.jetoile.hadoopunit.HadoopBootstrap;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.component.simpleyarnapp.Client;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import fr.jetoile.hadoopunit.exception.NotFoundServiceException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public class YarnBootstrapTest {

    private static Configuration configuration;

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
    public void testYarnLocalClusterIntegrationTest() {

        String[] args = new String[7];
        args[0] = "whoami";
        args[1] = "1";
        args[2] = getClass().getClassLoader().getResource("simple-yarn-app-1.1.0.jar").toString();
        args[3] = configuration.getString(YarnConfig.YARN_RESOURCE_MANAGER_ADDRESS_CLIENT_KEY);
        args[4] = configuration.getString(YarnConfig.YARN_RESOURCE_MANAGER_HOSTNAME_CLIENT_KEY);
        args[5] = configuration.getString(YarnConfig.YARN_RESOURCE_MANAGER_SCHEDULER_ADDRESS_CLIENT_KEY);
        args[6] = configuration.getString(YarnConfig.YARN_RESOURCE_MANAGER_RESOURCE_TRACKER_ADDRESS_CLIENT_KEY);


        try {
            Client.main(args);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // simple yarn app running "whoami",
        // validate the container contents matches the java user.name
        assertEquals(System.getProperty("user.name"), getStdoutContents());

    }

    public String getStdoutContents() {
        String contents = "";
        try {
            byte[] encoded = Files.readAllBytes(Paths.get(getStdoutPath()));
            contents = new String(encoded, Charset.defaultCharset()).trim();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NotFoundServiceException e) {
            e.printStackTrace();
        }
        return contents;
    }

    public String getStdoutPath() throws NotFoundServiceException {
        BootstrapHadoop yarn = (BootstrapHadoop) HadoopBootstrap.INSTANCE.getService("YARN");
        String yarnName = ((YarnBootstrap) yarn).getTestName();

        File dir = new File("./target/" + yarnName);
        String[] nmDirs = dir.list();
        for (String nmDir : nmDirs) {
            if (nmDir.contains("logDir")) {
                String[] appDirs = new File(dir.toString() + "/" + nmDir).list();
                for (String appDir : appDirs) {
                    if (appDir.contains("0001")) {
                        String[] containerDirs = new File(dir.toString() + "/" + nmDir + "/" + appDir).list();
                        for (String containerDir : containerDirs) {
                            if (containerDir.contains("000002")) {
                                return dir.toString() + "/" + nmDir + "/" + appDir + "/" + containerDir + "/stdout";
                            }
                        }
                    }
                }
            }
        }
        return "";
    }
}