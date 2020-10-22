/**
 * Copyright 2019 Confluent Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.rest;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.Metrics;
import org.eclipse.jetty.jmx.MBeanContainer;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public final class ApplicationServer<T extends RestConfig> extends Server {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling
  private final T config;
  private final ApplicationGroup applications;
  private final SslContextFactory sslContextFactory;

  private List<NetworkTrafficServerConnector> connectors = new ArrayList<>();

  private static final Logger log = LoggerFactory.getLogger(ApplicationServer.class);

  public ApplicationServer(T config) {
    this(config, createThreadPool(config));
  }

  public ApplicationServer(T config, ThreadPool threadPool) {
    super(threadPool);

    this.config = config;
    this.applications = new ApplicationGroup(this);

    int gracefulShutdownMs = config.getInt(RestConfig.SHUTDOWN_GRACEFUL_MS_CONFIG);
    if (gracefulShutdownMs > 0) {
      super.setStopTimeout(gracefulShutdownMs);
    }
    super.setStopAtShutdown(true);

    //FIXME : disable JMX to avoid errors
//    MBeanContainer mbContainer = new MBeanContainer(ManagementFactory.getPlatformMBeanServer());
//    super.addEventListener(mbContainer);
//    super.addBean(mbContainer);

    this.sslContextFactory = createSslContextFactory(config);
    configureConnectors(sslContextFactory);
  }

  /**
   * TODO: delete deprecatedPort parameter when `PORT_CONFIG` is deprecated.
   * It's only used to support the deprecated configuration.
   */
  static List<URI> parseListeners(
          List<String> listenersConfig,
          int deprecatedPort,
          List<String> supportedSchemes,
          String defaultScheme
  ) {
    // handle deprecated case, using PORT_CONFIG.
    // TODO: remove this when `PORT_CONFIG` is deprecated, because LISTENER_CONFIG
    // will have a default value which includes the default port.
    if (listenersConfig.isEmpty() || listenersConfig.get(0).isEmpty()) {
      log.warn(
              "DEPRECATION warning: `listeners` configuration is not configured. "
                      + "Falling back to the deprecated `port` configuration."
      );
      listenersConfig = new ArrayList<String>(1);
      listenersConfig.add(defaultScheme + "://0.0.0.0:" + deprecatedPort);
    }

    List<URI> listeners = new ArrayList<URI>(listenersConfig.size());
    for (String listenerStr : listenersConfig) {
      URI uri;
      try {
        uri = new URI(listenerStr);
      } catch (URISyntaxException use) {
        throw new ConfigException(
                "Could not parse a listener URI from the `listener` configuration option."
        );
      }
      String scheme = uri.getScheme();
      if (scheme == null) {
        throw new ConfigException(
                "Found a listener without a scheme. All listeners must have a scheme. The "
                        + "listener without a scheme is: " + listenerStr
        );
      }
      if (uri.getPort() == -1) {
        throw new ConfigException(
                "Found a listener without a port. All listeners must have a port. The "
                        + "listener without a port is: " + listenerStr
        );
      }
      if (!supportedSchemes.contains(scheme)) {
        log.warn(
                "Found a listener with an unsupported scheme (supported: {}). "
                        + "Ignoring listener '{}'",
                supportedSchemes,
                listenerStr
        );
      } else {
        listeners.add(uri);
      }
    }

    if (listeners.isEmpty()) {
      throw new ConfigException("No listeners are configured. Must have at least one listener.");
    }

    return listeners;
  }

  public void registerApplication(Application application) {
    applications.addApplication(application);
  }

  public List<Application<?>> getApplications() {
    return applications.getApplications();
  }

  private void attachMetricsListener(Metrics metrics, Map<String, String> tags) {
    MetricsListener metricsListener = new MetricsListener(metrics, "jetty", tags);
    for (NetworkTrafficServerConnector connector : connectors) {
      connector.addNetworkTrafficListener(metricsListener);
    }
  }

  private void finalizeHandlerCollection(HandlerCollection handlers, HandlerCollection wsHandlers) {
    /* DefaultHandler must come last eo ensure all contexts
     * have a chance to handle a request first */
    handlers.addHandler(new DefaultHandler());
    /* Needed for graceful shutdown as per `setStopTimeout` documentation */
    StatisticsHandler statsHandler = new StatisticsHandler();
    statsHandler.setHandler(handlers);

    ContextHandlerCollection contexts = new ContextHandlerCollection();
    contexts.setHandlers(new Handler[]{
        statsHandler,
        wsHandlers
    });

    super.setHandler(wrapWithGzipHandler(contexts));
  }

  protected void doStop() throws Exception {
    super.doStop();
    applications.doStop();
  }

  protected final void doStart() throws Exception {
    HandlerCollection handlers = new HandlerCollection();
    HandlerCollection wsHandlers = new HandlerCollection();
    for (Application app : applications.getApplications()) {
      attachMetricsListener(app.getMetrics(), app.getMetricsTags());
      handlers.addHandler(app.configureHandler());
      wsHandlers.addHandler(app.configureWebSocketHandler());
    }
    finalizeHandlerCollection(handlers, wsHandlers);
    // Call super.doStart last to ensure that handlers are ready for incoming requests
    super.doStart();
  }

  @SuppressWarnings("deprecation")
  private void configureClientAuth(SslContextFactory sslContextFactory, RestConfig config) {
    String clientAuthentication = config.getString(RestConfig.SSL_CLIENT_AUTHENTICATION_CONFIG);

    if (config.originals().containsKey(RestConfig.SSL_CLIENT_AUTH_CONFIG)) {
      if (config.originals().containsKey(RestConfig.SSL_CLIENT_AUTHENTICATION_CONFIG)) {
        log.warn(
                "The {} configuration is deprecated. Since a value has been supplied for the {} "
                        + "configuration, that will be used instead",
                RestConfig.SSL_CLIENT_AUTH_CONFIG,
                RestConfig.SSL_CLIENT_AUTHENTICATION_CONFIG
        );
      } else {
        log.warn(
                "The configuration {} is deprecated and should be replaced with {}",
                RestConfig.SSL_CLIENT_AUTH_CONFIG,
                RestConfig.SSL_CLIENT_AUTHENTICATION_CONFIG
        );
        clientAuthentication = config.getBoolean(RestConfig.SSL_CLIENT_AUTH_CONFIG)
                ? RestConfig.SSL_CLIENT_AUTHENTICATION_REQUIRED
                : RestConfig.SSL_CLIENT_AUTHENTICATION_NONE;
      }
    }

    switch (clientAuthentication) {
      case RestConfig.SSL_CLIENT_AUTHENTICATION_REQUIRED:
        sslContextFactory.setNeedClientAuth(true);
        break;
      case RestConfig.SSL_CLIENT_AUTHENTICATION_REQUESTED:
        sslContextFactory.setWantClientAuth(true);
        break;
      case RestConfig.SSL_CLIENT_AUTHENTICATION_NONE:
        break;
      default:
        throw new ConfigException(
                "Unexpected value for {} configuration: {}",
                RestConfig.SSL_CLIENT_AUTHENTICATION_CONFIG,
                clientAuthentication
        );
    }
  }

  private Path getWatchLocation(RestConfig config) {
    Path keystorePath = Paths.get(config.getString(RestConfig.SSL_KEYSTORE_LOCATION_CONFIG));
    String watchLocation = config.getString(RestConfig.SSL_KEYSTORE_WATCH_LOCATION_CONFIG);
    if (!watchLocation.isEmpty()) {
      keystorePath = Paths.get(watchLocation);
    }
    return keystorePath;
  }

  private SslContextFactory createSslContextFactory(RestConfig config) {
    SslContextFactory sslContextFactory = new SslContextFactory.Server();
    if (!config.getString(RestConfig.SSL_KEYSTORE_LOCATION_CONFIG).isEmpty()) {
      sslContextFactory.setKeyStorePath(
              config.getString(RestConfig.SSL_KEYSTORE_LOCATION_CONFIG)
      );
      sslContextFactory.setKeyStorePassword(
              config.getPassword(RestConfig.SSL_KEYSTORE_PASSWORD_CONFIG).value()
      );
      sslContextFactory.setKeyManagerPassword(
              config.getPassword(RestConfig.SSL_KEY_PASSWORD_CONFIG).value()
      );
      sslContextFactory.setKeyStoreType(
              config.getString(RestConfig.SSL_KEYSTORE_TYPE_CONFIG)
      );

      if (!config.getString(RestConfig.SSL_KEYMANAGER_ALGORITHM_CONFIG).isEmpty()) {
        sslContextFactory.setKeyManagerFactoryAlgorithm(
                config.getString(RestConfig.SSL_KEYMANAGER_ALGORITHM_CONFIG));
      }

      if (config.getBoolean(RestConfig.SSL_KEYSTORE_RELOAD_CONFIG)) {
        Path watchLocation = getWatchLocation(config);
        try {
          FileWatcher.onFileChange(watchLocation, () -> {
                // Need to reset the key store path for symbolic link case
                sslContextFactory.setKeyStorePath(
                    config.getString(RestConfig.SSL_KEYSTORE_LOCATION_CONFIG)
                );
                sslContextFactory.reload(scf -> log.info("Reloaded SSL cert"));
              }
          );
          log.info("Enabled SSL cert auto reload for: " + watchLocation);
        } catch (java.io.IOException e) {
          log.error("Can not enabled SSL cert auto reload", e);
        }
      }
    }

    configureClientAuth(sslContextFactory, config);

    List<String> enabledProtocols = config.getList(RestConfig.SSL_ENABLED_PROTOCOLS_CONFIG);
    if (!enabledProtocols.isEmpty()) {
      sslContextFactory.setIncludeProtocols(enabledProtocols.toArray(new String[0]));
    }

    List<String> cipherSuites = config.getList(RestConfig.SSL_CIPHER_SUITES_CONFIG);
    if (!cipherSuites.isEmpty()) {
      sslContextFactory.setIncludeCipherSuites(cipherSuites.toArray(new String[0]));
    }

    sslContextFactory.setEndpointIdentificationAlgorithm(
            config.getString(RestConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG));

    if (!config.getString(RestConfig.SSL_TRUSTSTORE_LOCATION_CONFIG).isEmpty()) {
      sslContextFactory.setTrustStorePath(
              config.getString(RestConfig.SSL_TRUSTSTORE_LOCATION_CONFIG)
      );
      sslContextFactory.setTrustStorePassword(
              config.getPassword(RestConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG).value()
      );
      sslContextFactory.setTrustStoreType(
              config.getString(RestConfig.SSL_TRUSTSTORE_TYPE_CONFIG)
      );

      if (!config.getString(RestConfig.SSL_TRUSTMANAGER_ALGORITHM_CONFIG).isEmpty()) {
        sslContextFactory.setTrustManagerFactoryAlgorithm(
                config.getString(RestConfig.SSL_TRUSTMANAGER_ALGORITHM_CONFIG)
        );
      }
    }

    sslContextFactory.setProtocol(config.getString(RestConfig.SSL_PROTOCOL_CONFIG));
    if (!config.getString(RestConfig.SSL_PROVIDER_CONFIG).isEmpty()) {
      sslContextFactory.setProtocol(config.getString(RestConfig.SSL_PROVIDER_CONFIG));
    }

    sslContextFactory.setRenegotiationAllowed(false);

    return sslContextFactory;
  }

  SslContextFactory getSslContextFactory() {
    return this.sslContextFactory;
  }

  private void configureConnectors(SslContextFactory sslContextFactory) {

    final HttpConfiguration httpConfiguration = new HttpConfiguration();
    httpConfiguration.setSendServerVersion(false);

    final HttpConnectionFactory httpConnectionFactory =
            new HttpConnectionFactory(httpConfiguration);

    List<URI> listeners = parseListeners(config.getList(RestConfig.LISTENERS_CONFIG),
            config.getInt(RestConfig.PORT_CONFIG), Arrays.asList("http", "https"), "http");

    for (URI listener : listeners) {
      log.info("Adding listener: " + listener.toString());
      NetworkTrafficServerConnector connector;
      if (listener.getScheme().equals("http")) {
        connector = new NetworkTrafficServerConnector(this, httpConnectionFactory);
      } else {
        connector = new NetworkTrafficServerConnector(this, httpConnectionFactory,
                sslContextFactory);
      }

      connector.setPort(listener.getPort());
      connector.setHost(listener.getHost());
      connector.setIdleTimeout(config.getLong(RestConfig.IDLE_TIMEOUT_MS_CONFIG));

      connectors.add(connector);
      super.addConnector(connector);

    }
  }

  // for testing
  List<URL> getListeners() {
    return Arrays.stream(getServer().getConnectors())
            .filter(connector -> connector instanceof ServerConnector)
            .map(ServerConnector.class::cast)
            .map(connector -> {
              try {
                final String protocol = new HashSet<>(connector.getProtocols())
                        .stream()
                        .map(String::toLowerCase)
                        .anyMatch(s -> s.equals("ssl")) ? "https" : "http";

                final int localPort = connector.getLocalPort();

                return new URL(protocol, "localhost", localPort, "");
              } catch (final Exception e) {
                throw new RuntimeException("Malformed listener", e);
              }
            })
            .collect(Collectors.toList());
  }

  /**
   * For unit testing.
   *
   * @return the total number of threads currently in the pool.
   */
  public int getThreads() {
    return getThreadPool().getThreads();
  }

  /**
   * For unit testing.
   *
   * @return the total number of maximum threads configured in the pool.
   */
  public int getMaxThreads() {
    return config.getInt(RestConfig.THREAD_POOL_MAX_CONFIG);
  }

  /**
   * For unit testing.
   *
   * @return the size of the queue in the pool.
   */
  public int getQueueSize() {
    return ((QueuedThreadPool)getThreadPool()).getQueueSize();
  }

  /**
   * For unit testing.
   *
   * @return the capacity of the queue in the pool.
   */
  public int getQueueCapacity() {
    return config.getInt(RestConfig.REQUEST_QUEUE_CAPACITY_CONFIG);
  }

  static Handler wrapWithGzipHandler(RestConfig config, Handler handler) {
    if (config.getBoolean(RestConfig.ENABLE_GZIP_COMPRESSION_CONFIG)) {
      GzipHandler gzip = new GzipHandler();
      gzip.setIncludedMethods("GET", "POST");
      gzip.setHandler(handler);
      return gzip;
    }
    return handler;
  }

  private Handler wrapWithGzipHandler(Handler handler) {
    return wrapWithGzipHandler(config, handler);
  }

  /**
   * Create the thread pool with request queue.
   *
   * @return thread pool used by the server
   */
  private static ThreadPool createThreadPool(RestConfig config) {
    /* Create blocking queue for the thread pool. */
    int initialCapacity = config.getInt(RestConfig.REQUEST_QUEUE_CAPACITY_INITIAL_CONFIG);
    int growBy = config.getInt(RestConfig.REQUEST_QUEUE_CAPACITY_GROWBY_CONFIG);
    int maxCapacity = config.getInt(RestConfig.REQUEST_QUEUE_CAPACITY_CONFIG);
    log.info("Initial capacity {}, increased by {}, maximum capacity {}.",
            initialCapacity, growBy, maxCapacity);

    BlockingQueue<Runnable> requestQueue =
            new BlockingArrayQueue<>(initialCapacity, growBy, maxCapacity);
    
    return new QueuedThreadPool(config.getInt(RestConfig.THREAD_POOL_MAX_CONFIG),
            config.getInt(RestConfig.THREAD_POOL_MIN_CONFIG),
            requestQueue);
  }
}
