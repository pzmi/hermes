package pl.allegro.tech.hermes.frontend.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.RoutingHandler;
import io.undertow.server.handlers.RequestDumpingHandler;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.config.Configs;
import pl.allegro.tech.hermes.common.metric.HermesMetrics;
import pl.allegro.tech.hermes.frontend.publishing.handlers.ThroughputLimiter;
import pl.allegro.tech.hermes.frontend.publishing.preview.MessagePreviewPersister;
import pl.allegro.tech.hermes.frontend.server.handlers.HttpHandlersPipeline;
import pl.allegro.tech.hermes.frontend.services.HealthCheckService;

import javax.inject.Inject;

import static pl.allegro.tech.hermes.common.config.Configs.FRONTEND_BACKLOG_SIZE;
import static pl.allegro.tech.hermes.common.config.Configs.FRONTEND_HOST;
import static pl.allegro.tech.hermes.common.config.Configs.FRONTEND_PORT;
import static pl.allegro.tech.hermes.common.config.Configs.FRONTEND_REQUEST_DUMPER;
import static pl.allegro.tech.hermes.common.config.Configs.FRONTEND_SSL_PORT;

public class HermesServer {

    private Undertow undertow;
    private HermesShutdownHandler gracefulShutdown;

    private final HermesMetrics hermesMetrics;
    private final ConfigFactory configFactory;
    private final HttpHandler publishingHandler;
    private final HttpHandlersPipeline httpHandlersPipeline;
    private final HealthCheckService healthCheckService;
    private final MessagePreviewPersister messagePreviewPersister;
    private final int port;
    private final int sslPort;
    private final String host;
    private ThroughputLimiter throughputLimiter;
    private final SslContextFactoryProvider sslContextFactoryProvider;

    @Inject
    public HermesServer(
            ConfigFactory configFactory,
            HermesMetrics hermesMetrics,
            HttpHandler publishingHandler,
            HttpHandlersPipeline httpHandlersPipeline,
            HealthCheckService healthCheckService,
            MessagePreviewPersister messagePreviewPersister,
            ThroughputLimiter throughputLimiter,
            SslContextFactoryProvider sslContextFactoryProvider) {

        this.configFactory = configFactory;
        this.hermesMetrics = hermesMetrics;
        this.publishingHandler = publishingHandler;
        this.httpHandlersPipeline = httpHandlersPipeline;
        this.healthCheckService = healthCheckService;
        this.messagePreviewPersister = messagePreviewPersister;
        this.sslContextFactoryProvider = sslContextFactoryProvider;

        this.port = configFactory.getIntProperty(FRONTEND_PORT);
        this.sslPort = configFactory.getIntProperty(FRONTEND_SSL_PORT);
        this.host = configFactory.getStringProperty(FRONTEND_HOST);
        this.throughputLimiter = throughputLimiter;
    }

    public void start() {
        startServer();
        messagePreviewPersister.start();
        throughputLimiter.start();

    }

    public void gracefulShutdown() throws InterruptedException {
        healthCheckService.shutdown();

        Thread.sleep(configFactory.getIntProperty(Configs.FRONTEND_GRACEFUL_SHUTDOWN_INITIAL_WAIT_MS));

        gracefulShutdown.handleShutdown();
    }

    public void shutdown() {
        undertow.stop();
        messagePreviewPersister.shutdown();
        throughputLimiter.stop();
    }


    // TODO add support for:
    // 1) SSL
    // 2) graceful shutdown
    // 3) HTTP 2
    private void startServer() {
        EventLoopGroup nioEventLoop = new NioEventLoopGroup(4);
        try {
            ServerBootstrap b = new ServerBootstrap()
                    .option(ChannelOption.SO_BACKLOG, configFactory.getIntProperty(FRONTEND_BACKLOG_SIZE))
                    .group(nioEventLoop)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(httpHandlersPipeline);

            Channel ch = b.bind(port).sync().channel();

            ch.closeFuture().addListener(future ->
                    nioEventLoop.shutdownGracefully());

        } catch (InterruptedException e) {
            throw new RuntimeException("Rethrowing exception", e);
        }

        /*
        gracefulShutdown = new HermesShutdownHandler(handlers(), hermesMetrics);
        Undertow.Builder builder = Undertow.builder()
                .addHttpListener(port, host)
                .setServerOption(REQUEST_PARSE_TIMEOUT, configFactory.getIntProperty(FRONTEND_REQUEST_PARSE_TIMEOUT))
                .setServerOption(MAX_HEADERS, configFactory.getIntProperty(FRONTEND_MAX_HEADERS))
                .setServerOption(MAX_PARAMETERS, configFactory.getIntProperty(FRONTEND_MAX_PARAMETERS))
                .setServerOption(MAX_COOKIES, configFactory.getIntProperty(FRONTEND_MAX_COOKIES))
                .setServerOption(ALWAYS_SET_KEEP_ALIVE, configFactory.getBooleanProperty(FRONTEND_ALWAYS_SET_KEEP_ALIVE))
                .setServerOption(KEEP_ALIVE, configFactory.getBooleanProperty(FRONTEND_SET_KEEP_ALIVE))
                .setSocketOption(BACKLOG, configFactory.getIntProperty(FRONTEND_BACKLOG_SIZE))
                .setSocketOption(READ_TIMEOUT, configFactory.getIntProperty(FRONTEND_READ_TIMEOUT))
                .setIoThreads(configFactory.getIntProperty(FRONTEND_IO_THREADS_COUNT))
                .setWorkerThreads(configFactory.getIntProperty(FRONTEND_WORKER_THREADS_COUNT))
                .setBufferSize(configFactory.getIntProperty(FRONTEND_BUFFER_SIZE))
                .setHandler(gracefulShutdown);

        if (configFactory.getBooleanProperty(FRONTEND_SSL_ENABLED)) {
            builder.addHttpsListener(sslPort, host, sslContextFactoryProvider.getSslContextFactory().create())
                    .setSocketOption(SSL_CLIENT_AUTH_MODE,
                            SslClientAuthMode.valueOf(configFactory.getStringProperty(FRONTEND_SSL_CLIENT_AUTH_MODE).toUpperCase()))
                    .setServerOption(ENABLE_HTTP2, configFactory.getBooleanProperty(FRONTEND_HTTP2_ENABLED));
        }
        this.undertow = builder.build();
        return undertow;
        */
    }

    private HttpHandler handlers() {
        HttpHandler healthCheckHandler = new HealthCheckHandler(healthCheckService);

        RoutingHandler routingHandler =  new RoutingHandler()
                .post("/topics/{qualifiedTopicName}", publishingHandler)
                .get("/status/ping", healthCheckHandler)
                .get("/status/health", healthCheckHandler)
                .get("/", healthCheckHandler);

        return isEnabled(FRONTEND_REQUEST_DUMPER) ? new RequestDumpingHandler(routingHandler) : routingHandler;
    }

    private boolean isEnabled(Configs property) {
        return configFactory.getBooleanProperty(property);
    }
}
