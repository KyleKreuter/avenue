package de.kyle.avenue;

import de.kyle.avenue.cluster.ClusterForwarder;
import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.handler.authentication.AuthenticationTokenHandler;
import de.kyle.avenue.handler.client.ClientConnectionHandler;
import de.kyle.avenue.handler.packet.InboundPacketHandler;
import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
import de.kyle.avenue.metrics.AvenueMetrics;
import de.kyle.avenue.net.SocketFactoryProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Single-node pub-sub server.
 * <p>
 * Lifecycle / testability seams:
 * <ul>
 *   <li>The configuration can be {@linkplain #SingleNodeServer(AvenueConfig) injected}, so a
 *       test may supply a config with a known secret/token and an ephemeral port
 *       ({@code port == 0}).</li>
 *   <li>{@link #start()} binds the {@link ServerSocket} and runs the accept loop on a
 *       dedicated thread instead of blocking the caller, so the actually bound port becomes
 *       readable via {@link #getPort()} (important when binding to port 0).</li>
 *   <li>{@link #stop()} closes the server socket, which unblocks the accept loop and shuts the
 *       executor down for a clean teardown between tests.</li>
 * </ul>
 * An optional {@link ClusterForwarder} can be injected so that the
 * {@link de.kyle.avenue.handler.packet.publish.PublishMessageInboundPacketHandler} forwards
 * local publishes to cluster peers. An external {@link TopicSubscriptionHandler} can also be
 * injected to share the subscription table with the cluster delivery path. When neither is
 * supplied the server operates in pure single-node mode (all existing tests are unaffected).
 * <p>
 * Wave 5: the server enforces an optional {@code server.max-connections} limit (E16), exposes
 * lightweight {@link AvenueMetrics} (E20) and binds a TLS {@link javax.net.ssl.SSLServerSocket}
 * instead of a plain socket when {@code server.tls.enabled=true} (E14). All of these default to
 * the previous behaviour (limit generous, no TLS) so the plain path is unchanged.
 * <p>
 * The production entry point ({@link AvenueApplication#main}) keeps its previous blocking
 * behaviour.
 */
public class SingleNodeServer {
    private static final Logger log = LoggerFactory.getLogger(SingleNodeServer.class);
    private final InboundPacketHandler inboundPacketHandler;
    private final ExecutorService executorService;
    private final TopicSubscriptionHandler topicSubscriptionHandler;
    private final AvenueConfig avenueConfig;
    private final AvenueMetrics metrics;
    private final SocketFactoryProvider socketFactoryProvider;

    /** Signals that the server socket has been bound (and {@link #getPort()} is meaningful). */
    private final CountDownLatch boundLatch = new CountDownLatch(1);

    private volatile boolean running;
    private volatile ServerSocket serverSocket;
    private Thread acceptThread;

    /**
     * Production constructor: loads the file/env configuration, starts the accept loop and
     * then blocks the calling thread until the server stops (so {@code main} stays alive).
     */
    public SingleNodeServer() {
        this(loadConfig());
        start();
        awaitTermination();
    }

    /**
     * Testable constructor (no clustering). Uses a fresh {@link TopicSubscriptionHandler}
     * and {@link ClusterForwarder#NOOP}. Fully backwards-compatible.
     *
     * @param avenueConfig the configuration to use
     */
    public SingleNodeServer(AvenueConfig avenueConfig) {
        this(avenueConfig, new TopicSubscriptionHandler(), ClusterForwarder.NOOP);
    }

    /**
     * Cluster-aware constructor with an externally managed {@link TopicSubscriptionHandler}.
     * The shared handler allows the {@link de.kyle.avenue.cluster.ClusterManager} and this
     * server to operate on the same subscription table, so cluster-delivered messages reach
     * local subscribers registered via this server's client connections.
     *
     * @param avenueConfig              the configuration to use
     * @param topicSubscriptionHandler  shared subscription table
     * @param clusterForwarder          non-blocking forwarder; {@link ClusterForwarder#NOOP} disables forwarding
     */
    public SingleNodeServer(
            AvenueConfig avenueConfig,
            TopicSubscriptionHandler topicSubscriptionHandler,
            ClusterForwarder clusterForwarder
    ) {
        this.avenueConfig = avenueConfig;
        this.metrics = new AvenueMetrics();
        AuthenticationTokenHandler authenticationTokenHandler = new AuthenticationTokenHandler(this.avenueConfig);
        this.topicSubscriptionHandler = topicSubscriptionHandler;
        this.topicSubscriptionHandler.setMetrics(this.metrics);
        this.executorService = Executors.newVirtualThreadPerTaskExecutor();
        this.inboundPacketHandler = new InboundPacketHandler(
                authenticationTokenHandler,
                topicSubscriptionHandler,
                executorService,
                clusterForwarder,
                this.metrics,
                this.avenueConfig
        );
        this.socketFactoryProvider = SocketFactoryProvider.forServer(
                avenueConfig.isServerTlsEnabled(),
                avenueConfig.getServerTlsKeystorePath(),
                avenueConfig.getServerTlsKeystorePassword()
        );
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    private static AvenueConfig loadConfig() {
        try {
            return new AvenueConfig();
        } catch (IOException e) {
            log.error("Could not load configuration file", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Binds the server socket and starts the accept loop on a dedicated thread. Returns once
     * the socket is bound (or the bind failed), so callers can immediately read {@link #getPort()}.
     */
    public void start() {
        this.running = true;
        this.metrics.startReporting(avenueConfig.getMetricsLogIntervalSeconds());
        this.acceptThread = new Thread(this::acceptLoop, "avenue-accept-loop");
        this.acceptThread.start();
        try {
            boundLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void acceptLoop() {
        try (ServerSocket server = socketFactoryProvider.createServerSocket(avenueConfig.getPort())) {
            this.serverSocket = server;
            log.info("Starting server on port {} (TLS={})",
                    server.getLocalPort(), socketFactoryProvider.isTlsEnabled());
            boundLatch.countDown();
            while (running) {
                try {
                    Socket client = server.accept();
                    if (avenueConfig.isServerTcpNoDelay()) {
                        // Latency over throughput for interactive pub/sub: flush small frames now.
                        client.setTcpNoDelay(true);
                    }
                    if (!admit(client)) {
                        continue;
                    }
                    ClientConnectionHandler clientConnectionHandler = new ClientConnectionHandler(
                            client,
                            inboundPacketHandler,
                            avenueConfig,
                            topicSubscriptionHandler,
                            metrics,
                            metrics::decrementActiveConnections
                    );
                    this.executorService.execute(clientConnectionHandler);
                } catch (IOException e) {
                    if (running) {
                        log.warn("Exception while trying to accept connection: {}", e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            log.error("Could not bind server socket: {}", e.getMessage());
        } finally {
            boundLatch.countDown();
            stop();
        }
    }

    /**
     * Enforces the configured {@code server.max-connections} limit (E16). When the limit is
     * reached the freshly accepted socket is closed immediately and the connection is counted
     * as rejected. A non-positive limit means unlimited.
     *
     * @return {@code true} if the connection was admitted (and the active gauge incremented)
     */
    private boolean admit(Socket client) {
        int limit = avenueConfig.getMaxConnections();
        long active = metrics.incrementActiveConnections();
        if (limit > 0 && active > limit) {
            metrics.decrementActiveConnections();
            metrics.incrementConnectionsRejected();
            log.warn("Max connections limit ({}) reached, rejecting connection from {}",
                    limit, client.getRemoteSocketAddress());
            closeQuietly(client);
            return false;
        }
        metrics.incrementTotalConnectionsAccepted();
        return true;
    }

    private void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // best-effort
        }
    }

    /**
     * Returns the port the server socket is actually bound to. Meaningful only after
     * {@link #start()} has returned; resolves an ephemeral ({@code 0}) port to its real value.
     *
     * @return the bound local port, or {@code -1} if the socket is not (yet) bound
     */
    public int getPort() {
        ServerSocket socket = this.serverSocket;
        return (socket != null && socket.isBound()) ? socket.getLocalPort() : -1;
    }

    /** Alias for {@link #getPort()}. */
    public int getBoundPort() {
        return getPort();
    }

    /** Exposes the metrics registry (for tests and operational tooling). */
    public AvenueMetrics getMetrics() {
        return metrics;
    }

    /** Blocks the calling thread until the accept loop has terminated. */
    private void awaitTermination() {
        try {
            if (this.acceptThread != null) {
                this.acceptThread.join();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void stop() {
        if (!this.running) {
            return;
        }
        log.info("Stopping server");
        this.running = false;
        this.metrics.stopReporting();
        ServerSocket socket = this.serverSocket;
        if (socket != null && !socket.isClosed()) {
            try {
                socket.close();
            } catch (IOException e) {
                log.warn("Error while closing server socket: {}", e.getMessage());
            }
        }
        this.executorService.shutdown();
    }
}
