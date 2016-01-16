package fr.jetoile.sample;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopUtils {


    private static final Logger LOG = LoggerFactory.getLogger(HadoopUtils.class);
    private static Configuration configuration;

    public static void setHadoopHome() {

        if (StringUtils.isEmpty(System.getProperty("HADOOP_HOME"))) {

            try {
                configuration = new PropertiesConfiguration("default.properties");
            } catch (ConfigurationException e) {
                LOG.error("unable to load default.properties", e);
            }

            String hadoop_home = configuration.getString("HADOOP_HOME");

            LOG.info("Setting hadoop.home.dir: {}", hadoop_home);
            System.setProperty("HADOOP_HOME", hadoop_home);

        }
    }
}
