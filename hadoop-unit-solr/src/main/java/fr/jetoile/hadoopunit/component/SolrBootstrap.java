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

import fr.jetoile.hadoopunit.*;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import fr.jetoile.hadoopunit.exception.NotFoundServiceException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.file.Paths;
import java.util.Map;

public class SolrBootstrap implements Bootstrap {
    public static final String SOLR_DIR_KEY = "solr.dir";
    public static final String SOLR_COLLECTION_INTERNAL_NAME = "solr.collection.internal.name";
    final private Logger LOGGER = LoggerFactory.getLogger(SolrBootstrap.class);

    private State state = State.STOPPED;

    private Configuration configuration;
    private EmbeddedSolrServer solrServer;
    private String solrDirectory;
    private String solrCollectionInternalName;


    public SolrBootstrap() {
        if (solrServer == null) {
            try {
                configuration = HadoopUtils.INSTANCE.loadConfigFile(null);
                loadConfig();
            } catch (BootstrapException e) {
                LOGGER.error("unable to load configuration", e);
            }
        }
    }

    public SolrBootstrap(URL url) {
        if (solrServer == null) {
            try {
                configuration = HadoopUtils.INSTANCE.loadConfigFile(url);
                loadConfig();
            } catch (BootstrapException e) {
                LOGGER.error("unable to load configuration", e);
            }
        }
    }

    @Override
    public ComponentMetadata getMetadata() {
        return new SolrMetadata();
    }

    @Override
    public String getProperties() {
        return  "\n \t\t\t collection:" + solrCollectionInternalName;
    }

    private void loadConfig() throws BootstrapException {
        solrDirectory = configuration.getString(SOLR_DIR_KEY);
        solrCollectionInternalName = configuration.getString(SOLR_COLLECTION_INTERNAL_NAME);
    }

    @Override
    public void loadConfig(Map<String, String> configs) {
        if (StringUtils.isNotEmpty(configs.get(SolrConfig.SOLR_DIR_KEY))) {
            solrDirectory = configs.get(SolrConfig.SOLR_DIR_KEY);
        }
        if (StringUtils.isNotEmpty(configs.get(SOLR_COLLECTION_INTERNAL_NAME))) {
            solrCollectionInternalName = configs.get(SOLR_COLLECTION_INTERNAL_NAME);
        }
    }

    @Override
    public Bootstrap start() {
        if (state == State.STOPPED) {
            state = State.STARTING;
            LOGGER.info("{} is starting", this.getClass().getName());

            try {
                String path = getClass().getClassLoader().getResource(solrDirectory).getFile();
                if (System.getProperty("os.name").startsWith("Windows")) {
                    path = path.substring(1);
                }

                this.solrServer = createPathConfiguredSolrServer(path);
            } catch (ParserConfigurationException | IOException | SAXException | BootstrapException e) {
                LOGGER.error("unable to bootstrap solr", e);
            }
            state = State.STARTED;
            LOGGER.info("{} is started", this.getClass().getName());
        }
        return this;
    }

    @Override
    public Bootstrap stop() {
        if (state == State.STARTED) {
            state = State.STOPPING;
            LOGGER.info("{} is stopping", this.getClass().getName());
            if (this.solrServer != null && solrServer.getCoreContainer() != null) {
                solrServer.getCoreContainer().shutdown();
            }
            state = State.STOPPED;
            LOGGER.info("{} is stopped", this.getClass().getName());
        }
        return this;
    }

    public EmbeddedSolrServer getClient() {
        return solrServer;
    }


    private final EmbeddedSolrServer createPathConfiguredSolrServer(String path) throws ParserConfigurationException,
            IOException, SAXException, BootstrapException {

        String solrHomeDirectory = path;
        if (System.getProperty("os.name").startsWith("Windows")) {
//            solrHomeDirectory = solrHomeDirectory.substring(1);
        }

        solrHomeDirectory = URLDecoder.decode(solrHomeDirectory, "utf-8");
        return new EmbeddedSolrServer(createCoreContainer(solrHomeDirectory), solrCollectionInternalName);
    }

    private CoreContainer createCoreContainer(String solrHomeDirectory) throws BootstrapException {
        File solrXmlFile = new File(solrHomeDirectory + "/solr.xml");
        return createCoreContainer(solrHomeDirectory, solrXmlFile);
    }


    private CoreContainer createCoreContainer(String solrHomeDirectory, File solrXmlFile) {
        return CoreContainer.createAndLoad(Paths.get(solrHomeDirectory), solrXmlFile.toPath());
    }

}
