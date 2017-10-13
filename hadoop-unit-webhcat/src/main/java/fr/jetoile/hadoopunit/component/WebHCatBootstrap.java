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

import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import fr.jetoile.hadoopunit.Component;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.HadoopUtils;
import fr.jetoile.hadoopunit.exception.BootstrapException;
import org.apache.commons.configuration.*;
import org.apache.hadoop.hdfs.web.AuthFilter;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hive.hcatalog.templeton.AppConfig;
import org.eclipse.jetty.rewrite.handler.RedirectPatternRule;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.FilterMapping;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class WebHCatBootstrap implements Bootstrap {
    final public static String NAME = Component.WEBHCAT.name();
    public static final String SERVLET_PATH = "templeton";

    final private static Logger LOGGER = LoggerFactory.getLogger(WebHCatBootstrap.class);

    private static volatile AppConfig conf;

    private Configuration configuration;

    private State state = State.STOPPED;

    private Server server;

    private int port = 9090;

    public WebHCatBootstrap() {
        try {
            configuration = HadoopUtils.INSTANCE.loadConfigFile(null);
            loadConfig();
        } catch (BootstrapException e) {
            LOGGER.error("unable to load configuration", e);
        }
    }

    public WebHCatBootstrap(URL url) {
        try {
            configuration = HadoopUtils.INSTANCE.loadConfigFile(url);
            loadConfig();
        } catch (BootstrapException e) {
            LOGGER.error("unable to load configuration", e);
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getProperties() {
        return "\n \t\t\t port:" + port;
    }

    public FilterHolder makeAuthFilter() {
        FilterHolder authFilter = new FilterHolder(AuthFilter.class);
        UserNameHandler.allowAnonymous(authFilter);
        if (UserGroupInformation.isSecurityEnabled()) {
            //http://hadoop.apache.org/docs/r1.1.1/api/org/apache/hadoop/security/authentication/server/AuthenticationFilter.html
//            authFilter.setInitParameter("dfs.web.authentication.signature.secret",
//                    conf.kerberosSecret());
//            //https://svn.apache.org/repos/asf/hadoop/common/branches/branch-1.2/src/packages/templates/conf/hdfs-site.xml
//            authFilter.setInitParameter("dfs.web.authentication.kerberos.principal",
//                    conf.kerberosPrincipal());
//            //http://https://svn.apache.org/repos/asf/hadoop/common/branches/branch-1.2/src/packages/templates/conf/hdfs-site.xml
//            authFilter.setInitParameter("dfs.web.authentication.kerberos.keytab",
//                    conf.kerberosKeytab());
        }
        return authFilter;
    }

    private void build() {
        Server server = new Server(port);
        ServletContextHandler root = new ServletContextHandler(server, "/");

        // Add the Auth filter
        FilterHolder fHolder = makeAuthFilter();

    /*
     * We add filters for each of the URIs supported by templeton.
     * If we added the entire sub-structure using '/*', the mapreduce
     * notification cannot give the callback to templeton in secure mode.
     * This is because mapreduce does not use secure credentials for
     * callbacks. So jetty would fail the request as unauthorized.
     */
        root.addFilter(fHolder, "/" + SERVLET_PATH + "/v1/ddl/*",
                FilterMapping.REQUEST);
        root.addFilter(fHolder, "/" + SERVLET_PATH + "/v1/pig/*",
                FilterMapping.REQUEST);
        root.addFilter(fHolder, "/" + SERVLET_PATH + "/v1/hive/*",
                FilterMapping.REQUEST);
        root.addFilter(fHolder, "/" + SERVLET_PATH + "/v1/sqoop/*",
                FilterMapping.REQUEST);
        root.addFilter(fHolder, "/" + SERVLET_PATH + "/v1/queue/*",
                FilterMapping.REQUEST);
        root.addFilter(fHolder, "/" + SERVLET_PATH + "/v1/jobs/*",
                FilterMapping.REQUEST);
        root.addFilter(fHolder, "/" + SERVLET_PATH + "/v1/mapreduce/*",
                FilterMapping.REQUEST);
        root.addFilter(fHolder, "/" + SERVLET_PATH + "/v1/status/*",
                FilterMapping.REQUEST);
        root.addFilter(fHolder, "/" + SERVLET_PATH + "/v1/version/*",
                FilterMapping.REQUEST);

        // Connect Jersey
        ServletHolder h = new ServletHolder(new ServletContainer(makeJerseyConfig()));
        root.addServlet(h, "/" + SERVLET_PATH + "/*");
        // Add any redirects
        addRedirects(server);

        this.server = server;

    }

    private void loadConfig() throws BootstrapException {
        try {
            configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);
        } catch (ConfigurationException e) {
            throw new BootstrapException("bad config", e);
        }
        port = configuration.getInt(HadoopUnitConfig.WEBHCAT_PORT_KEY);

        conf = new AppConfig();


        conf.startCleanup();

        conf.set("templeton.hive.properties", "hive.metastore.uris=thrift://localhost:20102,hive.metastore.sasl.enabled=false,hive.metastore.execute.setugi=true,hive.execution.engine=mr");
        conf.set("templeton.zookeeper.hosts", "127.0.0.1:22010");
        conf.set("hadoop.registry.zk.quorum", "127.0.0.1:22010");

        /*
jvm 1    | 16:06:27.206 INFO  AppConfig:279 - Attempting to load config file: null/webhcat-site.xml
jvm 1    | 16:06:27.241 INFO  AppConfig:279 - Attempting to load config file: ${env.HADOOP_CONF_DIR}/core-default.xml
jvm 1    | 16:06:27.242 INFO  AppConfig:279 - Attempting to load config file: ${env.HADOOP_CONF_DIR}/core-site.xml
jvm 1    | 16:06:27.242 INFO  AppConfig:279 - Attempting to load config file: ${env.HADOOP_CONF_DIR}/mapred-default.xml
jvm 1    | 16:06:27.242 INFO  AppConfig:279 - Attempting to load config file: ${env.HADOOP_CONF_DIR}/mapred-site.xml
jvm 1    | 16:06:27.242 INFO  AppConfig:279 - Attempting to load config file: ${env.HADOOP_CONF_DIR}/hdfs-site.xml


<property>
    <name>templeton.streaming.jar</name>
    <value>hdfs:///user/templeton/hadoop-streaming.jar</value>
    <description>The hdfs path to the Hadoop streaming jar file.</description>
  </property>

  <property>
    <name>templeton.hadoop</name>
    <value>${env.HADOOP_PREFIX}/bin/hadoop</value>
    <description>The path to the Hadoop executable.</description>
  </property>

  <property>
    <name>templeton.python</name>
    <value>${env.PYTHON_CMD}</value>
    <description>The path to the python executable.</description>
  </property>

  <property>
    <name>templeton.pig.archive</name>
    <value></value>
    <description>The path to the Pig archive.</description>
  </property>

  <property>
    <name>templeton.pig.path</name>
    <value>pig-0.11.1.tar.gz/pig-0.11.1/bin/pig</value>
    <description>The path to the Pig executable.</description>
  </property>

  <property>
    <name>templeton.hcat</name>
    <value>${env.HCAT_PREFIX}/bin/hcat.py</value>
    <description>The path to the hcatalog executable.</description>
  </property>

  <property>
    <name>templeton.hive.archive</name>
    <value></value>
    <description>The path to the Hive archive.</description>
  </property>

  <property>
    <name>templeton.hive.path</name>
    <value>hive-0.11.0.tar.gz/hive-0.11.0/bin/hive</value>
    <description>The path to the Hive executable.</description>
  </property>

  <property>
    <name>templeton.hive.home</name>
    <value>hive-0.14.0-SNAPSHOT-bin.tar.gz/hive-0.14.0-SNAPSHOT-bin</value>
    <description>
      The path to the Hive home within the tar.  This is needed if Hive is not installed on all
      nodes in the cluster and needs to be shipped to the target node in the cluster to execute Pig
      job which uses HCat, Hive query, etc.  Has no effect if templeton.hive.archive is not set.
    </description>
  </property>
  <property>
    <name>templeton.hcat.home</name>
    <value>hive-0.14.0-SNAPSHOT-bin.tar.gz/hive-0.14.0-SNAPSHOT-bin/hcatalog</value>
    <description>
      The path to the HCat home within the tar.  This is needed if Hive is not installed on all
      nodes in the cluster and needs to be shipped to the target node in the cluster to execute Pig
      job which uses HCat, Hive query, etc.  Has no effect if templeton.hive.archive is not set.
    </description>
  </property>

  <property>
    <name>templeton.hive.properties</name>
    <value>hive.metastore.uris=thrift://localhost:9933,hive.metastore.sasl.enabled=false</value>
    <description>Properties to set when running hive (during job sumission).  This is expected to
        be a comma-separated prop=value list.  If some value is itself a comma-separated list the
        escape character is '\'</description>
  </property>

  <property>
    <name>templeton.sqoop.path</name>
    <value>${env.SQOOP_HOME}/bin/sqoop.cmd</value>
    <description>The path to the Sqoop executable.</description>
  </property>

         */

        LOGGER.debug("Loaded conf " + conf);
    }

    @Override
    public void loadConfig(Map<String, String> configs) {

    }

    @Override
    public Bootstrap start() {
        if (state == State.STOPPED) {
            state = State.STARTING;
            LOGGER.info("{} is starting", this.getClass().getName());

            build();

            try {
                server.start();
            } catch (Exception e) {
                LOGGER.error("unable to add WebHCat", e);
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
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
            try {
                this.server.stop();
            } catch (Exception e) {
                LOGGER.error("unable to stop WebHCat", e);
            }
            state = State.STOPPED;
            LOGGER.info("{} is stopped", this.getClass().getName());
        }
        return this;
    }

    public PackagesResourceConfig makeJerseyConfig() {
        PackagesResourceConfig rc
                = new PackagesResourceConfig("org.apache.hive.hcatalog.templeton");
        HashMap<String, Object> props = new HashMap<String, Object>();
        props.put("com.sun.jersey.api.json.POJOMappingFeature", "true");
        props.put("com.sun.jersey.config.property.WadlGeneratorConfig",
                "org.apache.hive.hcatalog.templeton.WadlConfig");
        rc.setPropertiesAndFeatures(props);

        return rc;
    }

    public void addRedirects(Server server) {
        RewriteHandler rewrite = new RewriteHandler();

        RedirectPatternRule redirect = new RedirectPatternRule();
        redirect.setPattern("/templeton/v1/application.wadl");
        redirect.setLocation("/templeton/application.wadl");
        rewrite.addRule(redirect);

        HandlerList handlerlist = new HandlerList();
        ArrayList<Handler> handlers = new ArrayList<Handler>();

        // Any redirect handlers need to be added first
        handlers.add(rewrite);

        // Now add all the default handlers
        for (Handler handler : server.getHandlers()) {
            handlers.add(handler);
        }
        Handler[] newlist = new Handler[handlers.size()];
        handlerlist.setHandlers(handlers.toArray(newlist));
        server.setHandler(handlerlist);
    }

    static final class UserNameHandler {
        static void allowAnonymous(FilterHolder authFilter) {
      /*note that will throw if Anonymous mode is not allowed & user.name is not in query string of the request;
      * this ensures that in the context of WebHCat, PseudoAuthenticationHandler allows Anonymous even though
      * WebHCat itself will throw if it can't figure out user.name*/
            authFilter.setInitParameter("dfs.web.authentication." + PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, "true");
        }
        static String getUserName(HttpServletRequest request) {
            if(!UserGroupInformation.isSecurityEnabled() && "POST".equalsIgnoreCase(request.getMethod())) {
      /*as of hadoop 2.3.0, PseudoAuthenticationHandler only expects user.name as a query param
      * (not as a form param in a POST request.  For backwards compatibility, we this logic
      * to get user.name when it's sent as a form parameter.
      * This is added in Hive 0.13 and should be de-supported in 0.15*/
                String userName = request.getParameter(PseudoAuthenticator.USER_NAME);
                if(userName != null) {
                    LOGGER.warn(PseudoAuthenticator.USER_NAME +
                            " is sent as form parameter which is deprecated as of Hive 0.13.  Should send it in the query string.");
                }
                return userName;
            }
            return null;
        }
    }

    /**
     * Retrieve the config singleton.
     */
    public static synchronized AppConfig getAppConfigInstance() {
        if (conf == null)
            LOGGER.error("Bug: configuration not yet loaded");
        return conf;
    }
}
