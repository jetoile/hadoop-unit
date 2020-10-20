package fr.jetoile.hadoopunit.redis;

import fr.jetoile.hadoopunit.HadoopBootstrap;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.component.RedisConfig;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.fest.assertions.Assertions.assertThat;

public class EmbeddedRedisTest {

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
    public void redis_installation_should_succeed() throws IOException, InterruptedException {
        String version = configuration.getString(RedisConfig.REDIS_VERSION_KEY);
        String downloadUrl = configuration.getString(RedisConfig.REDIS_DOWNLOAD_URL_KEY);
        String tmpDir = getTmpDirPath(configuration, RedisConfig.REDIS_TMP_DIR_KEY);

        EmbeddedRedisInstaller.builder()
                .version(version)
                .downloadUrl(downloadUrl)
                .forceCleanupInstallationDirectory(false)
                .tmpDir(tmpDir)
                .build()
        .install();

        assertThat(new File(System.getProperty("user.home") + "/.redis/redis-" + version + "/src/redis-server")).exists();
    }

    private String getTmpDirPath(Configuration configuration, String componentTmpPathKey) {
        return configuration.getString(HadoopUnitConfig.TMP_DIR_PATH_KEY, "/tmp") + configuration.getString(componentTmpPathKey);
    }
}