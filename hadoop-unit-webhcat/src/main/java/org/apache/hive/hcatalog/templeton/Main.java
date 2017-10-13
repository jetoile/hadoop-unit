package org.apache.hive.hcatalog.templeton;


import fr.jetoile.hadoopunit.component.WebHCatBootstrap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    final private static Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private static AppConfig conf = WebHCatBootstrap.getAppConfigInstance();


    /**
     * Retrieve the config singleton.
     */
    public static synchronized AppConfig getAppConfigInstance() {
        if (conf == null)
            LOGGER.error("Bug: configuration not yet loaded");
        return conf;
    }
}
