/**
 * Copyright 2014 Confluent Inc.
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.base.JsonParseExceptionMapper;
import io.confluent.common.config.ConfigException;
import io.confluent.common.metrics.JmxReporter;
import io.confluent.common.metrics.MetricConfig;
import io.confluent.common.metrics.Metrics;
import io.confluent.common.metrics.MetricsReporter;
import io.confluent.rest.exceptions.ConstraintViolationExceptionMapper;
import io.confluent.rest.exceptions.GenericExceptionMapper;
import io.confluent.rest.exceptions.WebApplicationExceptionMapper;
import io.confluent.rest.logging.Slf4jRequestLog;
import io.confluent.rest.metrics.MetricsResourceMethodApplicationListener;
import io.confluent.rest.validation.JacksonMessageBodyProvider;
import org.eclipse.jetty.jaas.JAASLoginService;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.security.SecurityHandler;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.NetworkTrafficServerConnector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.AsyncGzipFilter;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.eclipse.jetty.util.resource.ResourceCollection;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.server.validation.ValidationFeature;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.DispatcherType;
import javax.ws.rs.core.Configurable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A REST application. Extend this class and implement setupResources() to register REST
 * resources with the JAX-RS server. Use createServer() to get a fully-configured, ready to run
 * Jetty server.
 */
public abstract class Application<T extends RestConfig> {
    protected T config;
    protected Server server = null;
    protected CountDownLatch shutdownLatch = new CountDownLatch(1);
    protected Metrics metrics;

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    public Application(T config) {
        this.config = config;
        MetricConfig metricConfig = new MetricConfig()
                .samples(config.getInt(RestConfig.METRICS_NUM_SAMPLES_CONFIG))
                .timeWindow(config.getLong(RestConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG),
                        TimeUnit.MILLISECONDS);
        List<MetricsReporter> reporters =
                config.getConfiguredInstances(RestConfig.METRICS_REPORTER_CLASSES_CONFIG,
                        MetricsReporter.class);
        reporters.add(new JmxReporter(config.getString(RestConfig.METRICS_JMX_PREFIX_CONFIG)));
        this.metrics = new Metrics(metricConfig, reporters, config.getTime());
    }

    /**
     * Register resources or additional Providers, ExceptionMappers, and other JAX-RS components with
     * the Jersey application. This, combined with your Configuration class, is where you can
     * customize the behavior of the application.
     */
    public abstract void setupResources(Configurable<?> config, T appConfig);

    /**
     * Returns a list of static resources to serve using the default servlet.
     *
     * <p>For example, static files can be served from class loader resources by returning
     * {@code
     * new ResourceCollection(Resource.newClassPathResource("static"));
     * }
     *
     * <p>For those resources to get served, it is necessary to add a static resources property to the
     * config in @link{{@link #setupResources(Configurable, RestConfig)}}, e.g. using something like
     * {@code
     * config.property(ServletProperties.FILTER_STATIC_CONTENT_REGEX, "/(static/.*|.*\\.html|)");
     * }
     *
     * @return static resource collection
     */
    protected ResourceCollection getStaticResources() {
        return null;
    }

    /**
     * add any servlet filters that should be called after resource
     * handling but before falling back to the default servlet
     */
    protected void configurePostResourceHandling(ServletContextHandler context) {
    }

    /**
     * Returns a map of tag names to tag values to apply to metrics for this application.
     *
     * @return a Map of tags and values
     */
    public Map<String, String> getMetricsTags() {
        return new LinkedHashMap<String, String>();
    }

    /**
     * Configure and create the server.
     */
    public Server createServer() throws RestConfigException {
        // The configuration for the JAX-RS REST service
        ResourceConfig resourceConfig = new ResourceConfig();

        Map<String, String> metricTags = getMetricsTags();

        configureBaseApplication(resourceConfig, metricTags);
        setupResources(resourceConfig, getConfiguration());

        // Configure the servlet container
        ServletContainer servletContainer = new ServletContainer(resourceConfig);
        final FilterHolder servletHolder = new FilterHolder(servletContainer);

        server = new Server() {
            @Override
            protected void doStop() throws Exception {
                super.doStop();
                Application.this.metrics.close();
                Application.this.onShutdown();
                Application.this.shutdownLatch.countDown();
            }
        };


        //FIXME : disable JMX to avoid errors
//    MBeanContainer mbContainer = new MBeanContainer(ManagementFactory.getPlatformMBeanServer());
//    server.addEventListener(mbContainer);
//    server.addBean(mbContainer);

        MetricsListener metricsListener = new MetricsListener(metrics, "jetty", metricTags);

        List<URI> listeners = parseListeners(config.getList(RestConfig.LISTENERS_CONFIG),
                config.getInt(RestConfig.PORT_CONFIG), Arrays.asList("http", "https"), "http");
        for (URI listener : listeners) {
            log.info("Adding listener: " + listener.toString());
            NetworkTrafficServerConnector connector;
            if (listener.getScheme().equals("http")) {
                connector = new NetworkTrafficServerConnector(server);
            } else {
                SslContextFactory sslContextFactory = new SslContextFactory();
                // IMPORTANT: the key's CN, stored in the keystore, must match the FQDN.
                // TODO: investigate this further. Would be better to use SubjectAltNames.
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
                        sslContextFactory.setSslKeyManagerFactoryAlgorithm(
                                config.getString(RestConfig.SSL_KEYMANAGER_ALGORITHM_CONFIG));
                    }
                }

                sslContextFactory.setNeedClientAuth(config.getBoolean(RestConfig.SSL_CLIENT_AUTH_CONFIG));

                List<String> enabledProtocols = config.getList(RestConfig.SSL_ENABLED_PROTOCOLS_CONFIG);
                if (!enabledProtocols.isEmpty()) {
                    sslContextFactory.setIncludeProtocols(enabledProtocols.toArray(new String[0]));
                }

                List<String> cipherSuites = config.getList(RestConfig.SSL_CIPHER_SUITES_CONFIG);
                if (!cipherSuites.isEmpty()) {
                    sslContextFactory.setIncludeCipherSuites(cipherSuites.toArray(new String[0]));
                }

                if (!config.getString(RestConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG).isEmpty()) {
                    sslContextFactory.setEndpointIdentificationAlgorithm(
                            config.getString(RestConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG));
                }

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

                connector = new NetworkTrafficServerConnector(server, sslContextFactory);
            }

            connector.addNetworkTrafficListener(metricsListener);
            connector.setPort(listener.getPort());
            connector.setHost(listener.getHost());
            server.addConnector(connector);
        }

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        ServletHolder defaultHolder = new ServletHolder("default", DefaultServlet.class);
        defaultHolder.setInitParameter("dirAllowed", "false");

        ResourceCollection staticResources = getStaticResources();
        if (staticResources != null) {
            context.setBaseResource(staticResources);
        }

        if (config.getBoolean(RestConfig.ENABLE_GZIP_COMPRESSION_CONFIG)) {
            FilterHolder gzipFilter = new FilterHolder(AsyncGzipFilter.class);
            // do not check if .gz file already exists for the requested resource
            gzipFilter.setInitParameter("checkGzExists", "false");
            gzipFilter.setInitParameter("methods", "GET,POST");
            context.addFilter(gzipFilter, "/*", null);
        }

        String authMethod = config.getString(RestConfig.AUTHENTICATION_METHOD_CONFIG);
        if (enableBasicAuth(authMethod)) {
            String realm = getConfiguration().getString(RestConfig.AUTHENTICATION_REALM_CONFIG);
            List<String> roles = getConfiguration().getList(RestConfig.AUTHENTICATION_ROLES_CONFIG);
            final SecurityHandler securityHandler = createSecurityHandler(realm, roles);
            context.setSecurityHandler(securityHandler);
        }

        String allowedOrigins = getConfiguration().getString(
                RestConfig.ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG
        );
        if (allowedOrigins != null && !allowedOrigins.trim().isEmpty()) {
            FilterHolder filterHolder = new FilterHolder(CrossOriginFilter.class);
            filterHolder.setName("cross-origin");
            filterHolder.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM, allowedOrigins);
            String allowedMethods = getConfiguration().getString(
                    RestConfig.ACCESS_CONTROL_ALLOW_METHODS
            );
            if (allowedMethods != null && !allowedOrigins.trim().isEmpty()) {
                filterHolder.setInitParameter(CrossOriginFilter.ALLOWED_METHODS_PARAM, allowedMethods);
            }
            context.addFilter(filterHolder, "/*", EnumSet.of(DispatcherType.REQUEST));
        }

        context.addFilter(servletHolder, "/*", null);
        configurePostResourceHandling(context);
        context.addServlet(defaultHolder, "/*");

        RequestLogHandler requestLogHandler = new RequestLogHandler();
        Slf4jRequestLog requestLog = new Slf4jRequestLog();
        requestLog.setLoggerName(config.getString(RestConfig.REQUEST_LOGGER_NAME_CONFIG));
        requestLog.setLogLatency(true);
        requestLogHandler.setRequestLog(requestLog);

        HandlerCollection handlers = new HandlerCollection();
        handlers.setHandlers(new Handler[]{context, new DefaultHandler(), requestLogHandler});

        /* Needed for graceful shutdown as per `setStopTimeout` documentation */
        StatisticsHandler statsHandler = new StatisticsHandler();
        statsHandler.setHandler(handlers);

        server.setHandler(statsHandler);

        int gracefulShutdownMs = getConfiguration().getInt(RestConfig.SHUTDOWN_GRACEFUL_MS_CONFIG);
        if (gracefulShutdownMs > 0) {
            server.setStopTimeout(gracefulShutdownMs);
        }
        server.setStopAtShutdown(true);

        return server;
    }

    static boolean enableBasicAuth(String authMethod) {
        return RestConfig.AUTHENTICATION_METHOD_BASIC.equals(authMethod);
    }

    static ConstraintSecurityHandler createSecurityHandler(String realm, List<String> roles) {
        final ConstraintSecurityHandler securityHandler = new ConstraintSecurityHandler();
        Constraint constraint = new Constraint();
        constraint.setAuthenticate(true);
        constraint.setRoles(roles.toArray(new String[0]));
        ConstraintMapping constraintMapping = new ConstraintMapping();
        constraintMapping.setConstraint(constraint);
        constraintMapping.setMethod("*");
        constraintMapping.setPathSpec("/*");
        securityHandler.addConstraintMapping(constraintMapping);
        securityHandler.setAuthenticator(new BasicAuthenticator());
        securityHandler.setLoginService(new JAASLoginService(realm));
        securityHandler.setIdentityService(new DefaultIdentityService());
        securityHandler.setRealmName(realm);
        return securityHandler;
    }

    // TODO: delete deprecatedPort parameter when `PORT_CONFIG` is deprecated.
    // It's only used to support the deprecated configuration.
    public static List<URI> parseListeners(
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
                        "Found a listener with an unsupported scheme (supported: {}). Ignoring listener '{}'",
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

    public void configureBaseApplication(Configurable<?> config) {
        configureBaseApplication(config, null);
    }

    /**
     * Register standard components for a JSON REST application on the given JAX-RS configurable,
     * which can be either an ResourceConfig for a server or a ClientConfig for a Jersey-based REST
     * client.
     */
    public void configureBaseApplication(Configurable<?> config, Map<String, String> metricTags) {
        T restConfig = getConfiguration();

        registerJsonProvider(config, restConfig, true);
        registerFeatures(config, restConfig);
        registerExceptionMappers(config, restConfig);

        config.register(new MetricsResourceMethodApplicationListener(metrics, "jersey",
                metricTags, restConfig.getTime()));

        config.property(ServerProperties.BV_SEND_ERROR_IN_RESPONSE, true);
    }

    /**
     * Register a body provider and optional exception mapper for (de)serializing JSON in
     * request/response entities.
     * @param config The config to register the provider with
     * @param restConfig The application's configuration
     * @param registerExceptionMapper Whether or not to register an additional exception mapper for
     *                                handling errors in (de)serialization
     */
    protected void registerJsonProvider(
            Configurable<?> config,
            T restConfig,
            boolean registerExceptionMapper
    ) {
        ObjectMapper jsonMapper = getJsonMapper();
        JacksonMessageBodyProvider jsonProvider = new JacksonMessageBodyProvider(jsonMapper);
        config.register(jsonProvider);
        if (registerExceptionMapper) {
            config.register(JsonParseExceptionMapper.class);
        }
    }

    /**
     * Register server features
     * @param config The config to register the features with
     * @param restConfig The application's configuration
     */
    protected void registerFeatures(Configurable<?> config, T restConfig) {
        config.register(ValidationFeature.class);
    }

    /**
     * Register handlers for translating exceptions into responses.
     * @param config The config to register the mappers with
     * @param restConfig The application's configuration
     */
    protected void registerExceptionMappers(Configurable<?> config, T restConfig) {
        config.register(ConstraintViolationExceptionMapper.class);
        config.register(new WebApplicationExceptionMapper(restConfig));
        config.register(new GenericExceptionMapper(restConfig));
    }

    public T getConfiguration() {
        return this.config;
    }

    /**
     * Gets a JSON ObjectMapper to use for (de)serialization of request/response entities. Override
     * this to configure the behavior of the serializer. One simple example of customization is to
     * set the INDENT_OUTPUT flag to make the output more readable. The default is a default
     * Jackson ObjectMapper.
     */
    protected ObjectMapper getJsonMapper() {
        return new ObjectMapper();
    }

    /**
     * Start the server (creating it if necessary).
     * @throws Exception If the application fails to start
     */
    public void start() throws Exception {
        if (server == null) {
            createServer();
        }
        server.start();
    }

    /**
     * Wait for the server to exit, allowing existing requests to complete if graceful shutdown is
     * enabled and invoking the shutdown hook before returning.
     * @throws InterruptedException If the internal threadpool fails to stop
     */
    public void join() throws InterruptedException {
        server.join();
        shutdownLatch.await();
    }

    /**
     * Request that the server shutdown.
     * @throws Exception If the application fails to stop
     */
    public void stop() throws Exception {
        server.stop();
    }

    /**
     * Shutdown hook that is invoked after the Jetty server has processed the shutdown request,
     * stopped accepting new connections, and tried to gracefully finish existing requests. At this
     * point it should be safe to clean up any resources used while processing requests.
     */
    public void onShutdown() {
    }
}

