package fr.jetoile.sample.component.solr;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//import org.apache.commons.io.FileUtils;
//import org.apache.lucene.util.LuceneTestCase;
//import org.apache.lucene.util.TestRuleStoreClassName;
//import org.apache.lucene.util.TestRuleTemporaryFilesCleanup;
//import org.apache.solr.SolrTestCaseJ4;
//import org.apache.solr.client.solrj.SolrClient;
//import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
//import org.apache.solr.client.solrj.embedded.JettyConfig;
//import org.apache.solr.client.solrj.embedded.JettySolrRunner;
//import org.apache.solr.client.solrj.impl.HttpSolrClient;
//import org.apache.solr.common.SolrException;
//import org.apache.solr.core.SolrConfig;
//import org.apache.solr.util.ExternalPaths;
//import org.eclipse.jetty.servlet.ServletHolder;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.File;
//import java.io.IOException;
//import java.io.OutputStreamWriter;
//import java.lang.invoke.MethodHandles;
//import java.net.URL;
//import java.nio.charset.Charset;
//import java.nio.file.Files;
//import java.nio.file.OpenOption;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.nio.file.attribute.FileAttribute;
//import java.util.HashSet;
//import java.util.Properties;
//import java.util.SortedMap;


public class SolrJettyTestBase {
//  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
//
//
//  public static JettySolrRunner jetty;
//  public static int port;
//  public static SolrClient client = null;
//  public static String context;
//
//  public static final String DEFAULT_TEST_CORENAME = "collection1";
//  protected static final String CORE_PROPERTIES_FILENAME = "core.properties";
////  private static final TestRuleStoreClassName classNameRule;
//  protected static String configString;
//  protected static String schemaString;
//  protected static Path testSolrHome;
//
//  private static TestRuleTemporaryFilesCleanup tempFilesCleanupRule;
//
//  private static String coreName = DEFAULT_TEST_CORENAME;
//
//  protected static SolrConfig solrConfig;
//  public static int DEFAULT_CONNECTION_TIMEOUT = 60000;  // default socket connection timeout in ms
//
//
//
//  public static JettySolrRunner createJetty(String solrHome, String configFile, String schemaFile, String context,
//                                            boolean stopAtShutdown, SortedMap<ServletHolder,String> extraServlets)
//      throws Exception {
//    // creates the data dir
//
//    context = context==null ? "/solr" : context;
//    org.apache.solr.SolrJettyTestBase.context = context;
//
//    JettyConfig jettyConfig = JettyConfig.builder()
//        .setContext(context)
//        .stopAtShutdown(stopAtShutdown)
//        .withServlets(extraServlets)
////        .withSSLConfig(sslConfig)
//        .build();
//
//    Properties nodeProps = new Properties();
//    if (configFile != null)
//      nodeProps.setProperty("solrconfig", configFile);
//    if (schemaFile != null)
//      nodeProps.setProperty("schema", schemaFile);
//    if (System.getProperty("solr.data.dir") == null && System.getProperty("solr.hdfs.home") == null) {
//      nodeProps.setProperty("solr.data.dir", createTempDir().toFile().getCanonicalPath());
//    }
//
//    return createJetty(solrHome, nodeProps, jettyConfig);
//  }
//
//  public static JettySolrRunner createJetty(String solrHome, String configFile, String context) throws Exception {
//    return createJetty(solrHome, configFile, null, context, true, null);
//  }
//
//  public static JettySolrRunner createJetty(String solrHome, JettyConfig jettyConfig) throws Exception {
//    return createJetty(solrHome, new Properties(), jettyConfig);
//  }
//
//  public static JettySolrRunner createJetty(String solrHome) throws Exception {
//    return createJetty(solrHome, new Properties(), JettyConfig.builder().build());
//  }
//
//  public static JettySolrRunner createJetty(String solrHome, Properties nodeProperties, JettyConfig jettyConfig) throws Exception {
//
//    initCore(null, null, solrHome);
//
//    Path coresDir = createTempDir().resolve("cores");
//
//    Properties props = new Properties();
//    props.setProperty("name", DEFAULT_TEST_CORENAME);
//    props.setProperty("configSet", "collection1");
//    props.setProperty("config", "${solrconfig:solrconfig.xml}");
//    props.setProperty("schema", "${schema:schema.xml}");
//
//    writeCoreProperties(coresDir.resolve("core"), props, "RestTestBase");
//
//    Properties nodeProps = new Properties(nodeProperties);
//    nodeProps.setProperty("coreRootDirectory", coresDir.toString());
//    nodeProps.setProperty("configSetBaseDir", solrHome);
//
//    ignoreException("maxWarmingSearchers");
//
//    jetty = new JettySolrRunner(solrHome, nodeProps, jettyConfig);
//    jetty.start();
//    port = jetty.getLocalPort();
//    log.info("Jetty Assigned Port#" + port);
//    return jetty;
//  }
//
//  public static void ignoreException(String pattern) {
//    if(SolrException.ignorePatterns == null) {
//      SolrException.ignorePatterns = new HashSet();
//    }
//
//    SolrException.ignorePatterns.add(pattern);
//  }
//
//  public static void afterSolrJettyTestBase() throws Exception {
//    if (jetty != null) {
//      jetty.stop();
//      jetty = null;
//    }
//    if (client != null) client.close();
//    client = null;
//  }
//
//
//  public SolrClient getSolrClient() {
//    {
//      if (client == null) {
//        client = createNewSolrClient();
//      }
//      return client;
//    }
//  }
//
//  /**
//   * Create a new solr client.
//   * If createJetty was called, an http implementation will be created,
//   * otherwise an embedded implementation will be created.
//   * Subclasses should override for other options.
//   */
//  public SolrClient createNewSolrClient() {
//    if (jetty != null) {
//      try {
//        // setup the client...
//        String url = jetty.getBaseUrl().toString() + "/" + "collection1";
//        HttpSolrClient client = new HttpSolrClient( url );
//        client.setConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT);
//        client.setDefaultMaxConnectionsPerHost(100);
//        client.setMaxTotalConnections(100);
//        return client;
//      }
//      catch( Exception ex ) {
//        throw new RuntimeException( ex );
//      }
//    } else {
//      return new EmbeddedSolrServer( h.getCoreContainer(), "collection1" );
//    }
//  }
//
//  // Sets up the necessary config files for Jetty. At least some tests require that the solrconfig from the test
//  // file directory are used, but some also require that the solr.xml file be explicitly there as of SOLR-4817
//  public static void setupJettyTestHome(File solrHome, String collection) throws Exception {
//    copySolrHomeToTemp(solrHome, collection);
//  }
//
//  public static void copySolrHomeToTemp(File dstRoot, String collection) throws IOException {
//    copySolrHomeToTemp(dstRoot, collection, false);
//  }
//
//  public static void copySolrHomeToTemp(File dstRoot, String collection, boolean newStyle) throws IOException {
//    if(!dstRoot.exists()) {
////      assertTrue("Failed to make subdirectory ", dstRoot.mkdirs());
//    }
//
//    if(newStyle) {
//      FileUtils.copyFile(new File(TEST_HOME(), "solr-no-core.xml"), new File(dstRoot, "solr.xml"));
//    } else {
//      FileUtils.copyFile(new File(TEST_HOME(), "solr.xml"), new File(dstRoot, "solr.xml"));
//    }
//
//    File subHome = new File(dstRoot, collection + File.separator + "conf");
//    String top = TEST_HOME() + "/collection1/conf";
//    FileUtils.copyFile(new File(top, "currency.xml"), new File(subHome, "currency.xml"));
//    FileUtils.copyFile(new File(top, "mapping-ISOLatin1Accent.txt"), new File(subHome, "mapping-ISOLatin1Accent.txt"));
//    FileUtils.copyFile(new File(top, "old_synonyms.txt"), new File(subHome, "old_synonyms.txt"));
//    FileUtils.copyFile(new File(top, "open-exchange-rates.json"), new File(subHome, "open-exchange-rates.json"));
//    FileUtils.copyFile(new File(top, "protwords.txt"), new File(subHome, "protwords.txt"));
//    FileUtils.copyFile(new File(top, "schema.xml"), new File(subHome, "schema.xml"));
//    FileUtils.copyFile(new File(top, "enumsConfig.xml"), new File(subHome, "enumsConfig.xml"));
//    FileUtils.copyFile(new File(top, "solrconfig.snippet.randomindexconfig.xml"), new File(subHome, "solrconfig.snippet.randomindexconfig.xml"));
//    FileUtils.copyFile(new File(top, "solrconfig.xml"), new File(subHome, "solrconfig.xml"));
//    FileUtils.copyFile(new File(top, "stopwords.txt"), new File(subHome, "stopwords.txt"));
//    FileUtils.copyFile(new File(top, "synonyms.txt"), new File(subHome, "synonyms.txt"));
//  }
//
//  public static void cleanUpJettyHome(File solrHome) throws Exception {
//    if (solrHome.exists()) {
//      FileUtils.deleteDirectory(solrHome);
//    }
//  }
//
//  public static void initCore() throws Exception {
//    String exampleHome = legacyExampleCollection1SolrHome();
//    String exampleConfig = exampleHome+"/collection1/conf/solrconfig.xml";
//    String exampleSchema = exampleHome+"/collection1/conf/schema.xml";
//    initCore(exampleConfig, exampleSchema, exampleHome);
//  }
//
//  public static String legacyExampleCollection1SolrHome() {
//    String sourceHome = ExternalPaths.SOURCE_HOME;
//    if (sourceHome == null)
//      throw new IllegalStateException("No source home! Cannot create the legacy example solr home directory.");
//
//    String legacyExampleSolrHome = null;
//    try {
//      File tempSolrHome = LuceneTestCase.createTempDir().toFile();
//      org.apache.commons.io.FileUtils.copyFileToDirectory(new File(sourceHome, "server/solr/solr.xml"), tempSolrHome);
//      File collection1Dir = new File(tempSolrHome, "collection1");
//      org.apache.commons.io.FileUtils.forceMkdir(collection1Dir);
//
//      File configSetDir = new File(sourceHome, "server/solr/configsets/sample_techproducts_configs/conf");
//      org.apache.commons.io.FileUtils.copyDirectoryToDirectory(configSetDir, collection1Dir);
//      Properties props = new Properties();
//      props.setProperty("name", "collection1");
//      OutputStreamWriter writer = null;
//      try {
//        writer = new OutputStreamWriter(FileUtils.openOutputStream(new File(collection1Dir, "core.properties")), "UTF-8");
//        props.store(writer, null);
//      } finally {
//        if (writer != null) {
//          try {
//            writer.close();
//          } catch (Exception ignore){}
//        }
//      }
//      legacyExampleSolrHome = tempSolrHome.getAbsolutePath();
//    } catch (Exception exc) {
//      if (exc instanceof RuntimeException) {
//        throw (RuntimeException)exc;
//      } else {
//        throw new RuntimeException(exc);
//      }
//    }
//
//    return legacyExampleSolrHome;
//  }
//
//  /** Call initCore in @BeforeClass to instantiate a solr core in your test class.
//   * deleteCore will be called for you via SolrTestCaseJ4 @AfterClass */
//  public static void initCore(String config, String schema) throws Exception {
//    initCore(config, schema, TEST_HOME());
//  }
//
//  /** Call initCore in @BeforeClass to instantiate a solr core in your test class.
//   * deleteCore will be called for you via SolrTestCaseJ4 @AfterClass */
//  public static void initCore(String config, String schema, String solrHome) throws Exception {
//    configString = config;
//    schemaString = schema;
//    testSolrHome = Paths.get(solrHome);
//    System.setProperty("solr.solr.home", solrHome);
//    initCore();
//  }
//
//  /** Call initCore in @BeforeClass to instantiate a solr core in your test class.
//   * deleteCore will be called for you via SolrTestCaseJ4 @AfterClass */
//  public static void initCore(String config, String schema, String solrHome, String pCoreName) throws Exception {
//    coreName=pCoreName;
//    initCore(config,schema,solrHome);
//  }
//
//  public static String TEST_HOME() {
//    return getFile("solr/collection1").getParent();
//  }
//
//  public static Path TEST_PATH() { return getFile("solr/collection1").getParentFile().toPath(); }
//
//
//  /** Gets a resource from the context classloader as {@link File}. This method should only be used,
//   * if a real file is needed. To get a stream, code should prefer
//   * {@link Class#getResourceAsStream} using {@code this.getClass()}.
//   */
//  public static File getFile(String name) {
//    final URL url = Thread.currentThread().getContextClassLoader().getResource(name.replace(File.separatorChar, '/'));
//    if (url != null) {
//      try {
//        return new File(url.toURI());
//      } catch (Exception e) {
//        throw new RuntimeException("Resource was found on classpath, but cannot be resolved to a " +
//                "normal file (maybe it is part of a JAR file): " + name);
//      }
//    }
//    final File file = new File(name);
//    if (file.exists()) {
//      return file;
//    }
//    throw new RuntimeException("Cannot find resource in classpath or in file-system (relative to CWD): " + name);
//  }
//
//  public static Path createTempDir() {
//    return createTempDir("tempDir");
//  }
//
//  public static Path createTempDir(String prefix) {
//    return tempFilesCleanupRule.createTempDir(prefix);
//  }
//
//  public static Path createTempFile(String prefix, String suffix) throws IOException {
//    return tempFilesCleanupRule.createTempFile(prefix, suffix);
//  }
//
//  public static Path createTempFile() throws IOException {
//    return createTempFile("tempFile", ".tmp");
//  }
//
//  protected void writeCoreProperties(Path coreDirectory, String corename) throws IOException {
//    Properties props = new Properties();
//    props.setProperty("name", corename);
//    props.setProperty("configSet", "collection1");
//    props.setProperty("config", "${solrconfig:solrconfig.xml}");
//    props.setProperty("schema", "${schema:schema.xml}");
//    writeCoreProperties(coreDirectory, props, this.getTestName());
//  }
//
//  public static void writeCoreProperties(Path coreDirectory, Properties properties, String testname) throws IOException {
//    log.info("Writing core.properties file to {}", coreDirectory);
//    Files.createDirectories(coreDirectory, new FileAttribute[0]);
//    OutputStreamWriter writer = new OutputStreamWriter(Files.newOutputStream(coreDirectory.resolve("core.properties"), new OpenOption[0]), Charset.forName("UTF-8"));
//    Throwable var4 = null;
//
//    try {
//      properties.store(writer, testname);
//    } catch (Throwable var13) {
//      var4 = var13;
//      throw var13;
//    } finally {
//      if(writer != null) {
//        if(var4 != null) {
//          try {
//            writer.close();
//          } catch (Throwable var12) {
//            var4.addSuppressed(var12);
//          }
//        } else {
//          writer.close();
//        }
//      }
//
//    }
//
//  }
//  public static Class<?> getTestClass() {
//    return classNameRule.getTestClass();
//  }


}
