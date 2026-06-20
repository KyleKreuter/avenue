package de.kyle.avenue;

import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.handler.authentication.AuthenticationTokenHandler;
import de.kyle.avenue.handler.client.ClientConnectionHandler;
import de.kyle.avenue.handler.packet.InboundPacketHandler;
import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
import de.kyle.avenue.serialization.PacketDeserializer;
import de.kyle.avenue.serialization.PacketSerializer;
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
 * The production entry point ({@link AvenueApplication#main}) keeps its previous blocking
 * behaviour: the no-arg constructor loads the file/env config, starts the server and blocks
 * the calling thread until the accept loop terminates.
 */
public class SingleNodeServer {
    private static final Logger log = LoggerFactory.getLogger(SingleNodeServer.class);
    private final PacketDeserializer packetDeserializer;
    private final PacketSerializer packetSerializer;
    private final InboundPacketHandler inboundPacketHandler;
    private final ExecutorService executorService;
    private final TopicSubscriptionHandler topicSubscriptionHandler;
    private final AvenueConfig avenueConfig;

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
     * Testable constructor: builds the server around an injected configuration but does NOT
     * start it. Call {@link #start()} to bind and begin accepting, then {@link #getPort()} to
     * learn the bound port. This never blocks the caller.
     *
     * @param avenueConfig the configuration to use (secret/token/port/packetSize/queue tuning)
     */
    public SingleNodeServer(AvenueConfig avenueConfig) {
        this.avenueConfig = avenueConfig;
        AuthenticationTokenHandler authenticationTokenHandler = new AuthenticationTokenHandler(this.avenueConfig);
        this.topicSubscriptionHandler = new TopicSubscriptionHandler();
        this.executorService = Executors.newVirtualThreadPerTaskExecutor();
        this.packetDeserializer = new PacketDeserializer(this.avenueConfig);
        this.packetSerializer = new PacketSerializer(this.avenueConfig);
        this.inboundPacketHandler = new InboundPacketHandler(
                authenticationTokenHandler,
                topicSubscriptionHandler,
                executorService
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
        this.acceptThread = new Thread(this::acceptLoop, "avenue-accept-loop");
        this.acceptThread.start();
        try {
            // Wait until the accept loop has bound the socket (or failed) before returning.
            boundLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void acceptLoop() {
        try (ServerSocket server = new ServerSocket(avenueConfig.getPort())) {
            this.serverSocket = server;
            log.info("Starting server on port {}", server.getLocalPort());
            boundLatch.countDown();
            while (running) {
                try {
                    Socket client = server.accept();
                    ClientConnectionHandler clientConnectionHandler = new ClientConnectionHandler(
                            client,
                            packetDeserializer,
                            packetSerializer,
                            inboundPacketHandler,
                            avenueConfig,
                            topicSubscriptionHandler
                    );
                    this.executorService.execute(clientConnectionHandler);
                } catch (IOException e) {
                    // A close() on the server socket surfaces here as a SocketException; that is
                    // the expected, clean way the accept loop exits on stop().
                    if (running) {
                        log.warn("Exception while trying to accept connection: {}", e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            log.error("Could not bind server socket: {}", e.getMessage());
        } finally {
            // Release any waiter even if binding failed, so start() never hangs.
            boundLatch.countDown();
            stop();
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
        // Closing the server socket unblocks ServerSocket.accept() in the accept loop.
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
