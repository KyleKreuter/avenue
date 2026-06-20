package de.kyle.avenue.net;

import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.handler.packet.InboundPacketHandler;
import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
import de.kyle.avenue.metrics.AvenueMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hand-rolled NIO selector transport for the client-facing port (the {@code server.io-mode=nio}
 * alternative to the blocking thread-per-connection {@link de.kyle.avenue.SingleNodeServer} accept
 * loop). No external dependency (no Netty).
 * <p>
 * Architecture:
 * <ul>
 *   <li><b>Acceptor</b> — one thread owning a non-blocking {@link ServerSocketChannel} on its own
 *       {@link Selector}. On {@code OP_ACCEPT} it configures the channel
 *       ({@code TCP_NODELAY} per config), enforces {@code server.max-connections} against the
 *       active-connection gauge, and hands the channel to a worker round-robin.</li>
 *   <li><b>I/O workers</b> — {@code server.nio.io-threads} {@link NioIoWorker}s (default =
 *       {@link Runtime#availableProcessors()}), each with its own selector and a disjoint connection
 *       set, so thousands of connections are served by a small fixed thread pool instead of one
 *       thread per socket.</li>
 * </ul>
 * The handler / subscription / cluster wiring ({@code inboundPacketHandler},
 * {@code topicSubscriptionHandler}, {@code metrics}) is the exact same set of collaborators the
 * blocking transport receives, so protocol, delivery, backpressure, idle-timeout, max-connections
 * and metrics behave identically — only the I/O threading model differs.
 */
public final class NioServerTransport {

    private static final Logger log = LoggerFactory.getLogger(NioServerTransport.class);

    private final AvenueConfig config;
    private final InboundPacketHandler inboundPacketHandler;
    private final TopicSubscriptionHandler topicSubscriptionHandler;
    private final AvenueMetrics metrics;

    private final NioIoWorker[] workers;
    private final AtomicInteger nextWorker = new AtomicInteger();

    private final CountDownLatch boundLatch = new CountDownLatch(1);
    private volatile boolean running;
    private volatile ServerSocketChannel serverChannel;
    private volatile Selector acceptSelector;
    private volatile int boundPort = -1;
    private Thread acceptThread;

    public NioServerTransport(
            AvenueConfig config,
            InboundPacketHandler inboundPacketHandler,
            TopicSubscriptionHandler topicSubscriptionHandler,
            AvenueMetrics metrics
    ) throws IOException {
        this.config = config;
        this.inboundPacketHandler = inboundPacketHandler;
        this.topicSubscriptionHandler = topicSubscriptionHandler;
        this.metrics = metrics;
        int threads = Math.max(1, config.getServerNioIoThreads());
        this.workers = new NioIoWorker[threads];
        for (int i = 0; i < threads; i++) {
            this.workers[i] = new NioIoWorker(i, config, inboundPacketHandler, topicSubscriptionHandler);
        }
    }

    /**
     * Binds the server channel and starts the acceptor + all I/O workers. Returns once the channel is
     * bound (or the bind failed), so callers can read {@link #getPort()} immediately — same contract
     * as the blocking accept loop.
     */
    public void start() throws IOException {
        this.running = true;
        this.acceptSelector = Selector.open();
        this.serverChannel = ServerSocketChannel.open();
        this.serverChannel.configureBlocking(false);
        this.serverChannel.bind(new InetSocketAddress(config.getPort()));
        this.serverChannel.register(acceptSelector, SelectionKey.OP_ACCEPT);
        this.boundPort = ((InetSocketAddress) serverChannel.getLocalAddress()).getPort();

        for (NioIoWorker worker : workers) {
            worker.start();
        }

        this.acceptThread = new Thread(this::acceptLoop, "avenue-nio-acceptor");
        this.acceptThread.start();
        boundLatch.countDown();
        log.info("Started NIO transport on port {} with {} I/O worker(s)", boundPort, workers.length);
    }

    private void acceptLoop() {
        try {
            while (running) {
                acceptSelector.select(1_000L);
                if (!acceptSelector.isOpen()) {
                    break;
                }
                var it = acceptSelector.selectedKeys().iterator();
                while (it.hasNext()) {
                    SelectionKey key = it.next();
                    it.remove();
                    if (key.isAcceptable()) {
                        acceptPending();
                    }
                }
            }
        } catch (IOException e) {
            if (running) {
                log.warn("NIO acceptor loop error: {}", e.getMessage());
            }
        }
    }

    /** Accepts every pending connection on the non-blocking server channel. */
    private void acceptPending() throws IOException {
        SocketChannel channel;
        while ((channel = serverChannel.accept()) != null) {
            channel.configureBlocking(false);
            if (config.isServerTcpNoDelay()) {
                channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            }
            if (!admit(channel)) {
                continue;
            }
            NioIoWorker worker = workers[Math.floorMod(nextWorker.getAndIncrement(), workers.length)];
            NioClientConnection connection = new NioClientConnection(
                    channel, worker, config, metrics, metrics::decrementActiveConnections);
            worker.register(connection);
        }
    }

    /**
     * Enforces {@code server.max-connections} exactly as {@link de.kyle.avenue.SingleNodeServer}: the
     * TCP socket is accepted but immediately closed when the limit is reached, and the rejection is
     * counted. A non-positive limit means unlimited.
     *
     * @return {@code true} if admitted (active gauge incremented), {@code false} if rejected+closed
     */
    private boolean admit(SocketChannel channel) {
        int limit = config.getMaxConnections();
        long active = metrics.incrementActiveConnections();
        if (limit > 0 && active > limit) {
            metrics.decrementActiveConnections();
            metrics.incrementConnectionsRejected();
            log.warn("Max connections limit ({}) reached, rejecting NIO connection", limit);
            closeQuietly(channel);
            return false;
        }
        metrics.incrementTotalConnectionsAccepted();
        return true;
    }

    private static void closeQuietly(SocketChannel channel) {
        try {
            channel.close();
        } catch (IOException ignored) {
            // best-effort
        }
    }

    /** The actually bound local port ({@code -1} until {@link #start()} succeeds). */
    public int getPort() {
        return boundPort;
    }

    /** Blocks until the server channel has been bound (or bind failed). */
    public void awaitBound() {
        try {
            boundLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Stops the acceptor and all workers, closing every connection. Idempotent. */
    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        try {
            if (serverChannel != null) {
                serverChannel.close();
            }
        } catch (IOException e) {
            log.warn("Error closing NIO server channel: {}", e.getMessage());
        }
        Selector accept = this.acceptSelector;
        if (accept != null) {
            accept.wakeup();
        }
        for (NioIoWorker worker : workers) {
            worker.stop();
        }
    }
}
