package de.kyle.avenue.cluster;

import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
import de.kyle.avenue.net.SocketFactoryProvider;
import de.kyle.avenue.packet.cluster.ClusterPublishPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Manages all cluster peer links for a single Avenue node and implements
 * {@link ClusterForwarder} so it can be injected directly into
 * {@link de.kyle.avenue.handler.packet.publish.PublishMessageInboundPacketHandler}.
 * <p>
 * Responsibilities:
 * <ol>
 *   <li>Runs a TCP accept loop on the configured cluster port for incoming peer
 *       connections.</li>
 *   <li>Initiates outbound connections to all statically configured peers with
 *       exponential-backoff reconnect on failure.</li>
 *   <li>Maintains the active {@link PeerLink} set, keyed by remote {@code nodeId}.</li>
 *   <li>Implements {@link ClusterForwarder#forward} to fan out a local publish to all
 *       peers; stamps the local {@code originNodeId} onto the
 *       {@link ClusterPublishPacket}.</li>
 * </ol>
 * All I/O threads are virtual threads (Java 21).
 */
public class ClusterManager implements ClusterForwarder {

    private static final Logger log = LoggerFactory.getLogger(ClusterManager.class);

    private static final long MIN_BACKOFF_MS = 1_000L;
    private static final long MAX_BACKOFF_MS = 30_000L;

    private final AvenueConfig config;
    private final TopicSubscriptionHandler topicSubscriptionHandler;
    private final MessageDeduplicator deduplicator = new MessageDeduplicator();

    /** Yields plain or TLS cluster sockets depending on {@code cluster.tls.enabled} (E14). */
    private final SocketFactoryProvider socketFactoryProvider;

    /** Active links keyed by the remote node's ID. */
    private final ConcurrentHashMap<String, PeerLink> activePeers = new ConcurrentHashMap<>();

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    private volatile boolean running = false;
    private volatile ServerSocket clusterServerSocket;

    /** Released once the cluster server socket is bound (or the bind attempt failed). */
    private final CountDownLatch boundLatch = new CountDownLatch(1);

    public ClusterManager(AvenueConfig config, TopicSubscriptionHandler topicSubscriptionHandler) {
        this.config = config;
        this.topicSubscriptionHandler = topicSubscriptionHandler;
        this.socketFactoryProvider = SocketFactoryProvider.forCluster(
                config.isClusterTlsEnabled(),
                config.getClusterTlsKeystorePath(),
                config.getClusterTlsKeystorePassword(),
                config.getClusterTlsTruststorePath(),
                config.getClusterTlsTruststorePassword()
        );
    }

    // -------------------------------------------------------------------------
    // ClusterForwarder implementation
    // -------------------------------------------------------------------------

    /**
     * Forwards a locally-received publish to all connected peers. Stamps this node's
     * {@code nodeId} as {@code originNodeId}. Non-blocking: each peer's queue absorbs
     * the work; a full queue drops the packet for that peer (at-most-once).
     */
    @Override
    public void forward(String topic, String source, String data, String messageId) {
        if (activePeers.isEmpty()) {
            return;
        }
        ClusterPublishPacket packet = new ClusterPublishPacket(
                topic,
                source,
                config.getNodeId(),
                messageId,
                data
        );
        activePeers.values().forEach(link -> link.forward(packet));
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /**
     * Starts the cluster accept loop and all outbound connector loops. Returns after the
     * cluster server socket is bound.
     */
    public void start() {
        running = true;
        executor.submit(this::acceptLoop);
        try {
            boundLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        List<String> peers = config.getClusterPeers();
        if (peers != null) {
            for (String peer : peers) {
                String trimmed = peer.trim();
                if (!trimmed.isEmpty()) {
                    executor.submit(() -> outboundConnectLoop(trimmed));
                }
            }
        }
    }

    /** Closes all peer links and the cluster server socket. */
    public void stop() {
        running = false;
        ServerSocket ss = clusterServerSocket;
        if (ss != null && !ss.isClosed()) {
            try {
                ss.close();
            } catch (IOException e) {
                log.warn("Error closing cluster server socket: {}", e.getMessage());
            }
        }
        activePeers.values().forEach(PeerLink::close);
        activePeers.clear();
        executor.shutdownNow();
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    /** Exposes the config for use by {@link ClusterNode}. */
    public AvenueConfig getConfig() {
        return config;
    }

    /**
     * Returns the actual bound port of the cluster server socket, or {@code -1} if not bound.
     */
    public int getClusterPort() {
        ServerSocket ss = clusterServerSocket;
        return (ss != null && ss.isBound()) ? ss.getLocalPort() : -1;
    }

    /** Returns the number of currently active peer links. */
    public int getActivePeerCount() {
        return activePeers.size();
    }

    /**
     * Blocks until at least one peer link is active, or the timeout elapses. Used in tests
     * for deterministic synchronization before asserting cross-node delivery.
     */
    public boolean awaitAnyPeer(long timeout, TimeUnit unit) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
        while (System.nanoTime() < deadlineNanos) {
            if (!activePeers.isEmpty()) {
                return true;
            }
            Thread.sleep(20);
        }
        return !activePeers.isEmpty();
    }

    // -------------------------------------------------------------------------
    // Inbound accept loop
    // -------------------------------------------------------------------------

    private void acceptLoop() {
        int port = config.getClusterPort();
        try (ServerSocket server = socketFactoryProvider.createServerSocket(port)) {
            clusterServerSocket = server;
            log.info("Cluster accept loop bound on port {} (TLS={})",
                    server.getLocalPort(), socketFactoryProvider.isTlsEnabled());
            boundLatch.countDown();
            while (running) {
                try {
                    Socket peer = server.accept();
                    executor.submit(() -> handleIncomingPeer(peer));
                } catch (IOException e) {
                    if (running) {
                        log.warn("Cluster accept error: {}", e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            log.error("Cannot bind cluster server socket on port {}: {}", port, e.getMessage());
        } finally {
            boundLatch.countDown();
        }
    }

    private void handleIncomingPeer(Socket peerSocket) {
        try {
            // Acceptor-side handshake: read peer's hello, then send ours.
            String remoteNodeId = ClusterHandshake.acceptorHandshake(
                    peerSocket,
                    config.getNodeId(),
                    config.getClusterSecret(),
                    config.getPacketSize()
            );

            if (activePeers.containsKey(remoteNodeId)) {
                log.info("Already connected to peer {}, dropping duplicate incoming connection", remoteNodeId);
                closeQuietly(peerSocket);
                return;
            }

            PeerLink link = buildLink(peerSocket, remoteNodeId);
            link.start();
            log.info("Accepted cluster peer: {}", remoteNodeId);

        } catch (SecurityException e) {
            log.warn("Rejected peer {}: wrong cluster secret", peerSocket.getRemoteSocketAddress());
            closeQuietly(peerSocket);
        } catch (Exception e) {
            log.warn("Error in incoming peer handshake: {}", e.getMessage());
            closeQuietly(peerSocket);
        }
    }

    // -------------------------------------------------------------------------
    // Outbound connect loop with exponential backoff
    // -------------------------------------------------------------------------

    private void outboundConnectLoop(String peerAddress) {
        long backoffMs = MIN_BACKOFF_MS;
        while (running) {
            try {
                String[] parts = peerAddress.split(":", 2);
                if (parts.length != 2) {
                    log.error("Invalid cluster peer address '{}' (expected host:port)", peerAddress);
                    return;
                }
                String host = parts[0].trim();
                int port = Integer.parseInt(parts[1].trim());

                log.debug("Connecting to cluster peer at {}:{}", host, port);
                Socket socket = socketFactoryProvider.createSocket(host, port);

                // Initiator-side handshake: send our hello first, then read peer's hello.
                String remoteNodeId = ClusterHandshake.initiatorHandshake(
                        socket,
                        config.getNodeId(),
                        config.getClusterSecret(),
                        config.getPacketSize()
                );

                if (activePeers.containsKey(remoteNodeId)) {
                    log.info("Peer {} already connected via incoming link, closing outbound duplicate", remoteNodeId);
                    closeQuietly(socket);
                    // Wait for the existing link to die before attempting another outbound.
                    waitForLinkClose(remoteNodeId);
                    backoffMs = MIN_BACKOFF_MS;
                    continue;
                }

                PeerLink link = buildLink(socket, remoteNodeId);
                link.start();
                log.info("Connected to cluster peer {} at {}:{}", remoteNodeId, host, port);
                backoffMs = MIN_BACKOFF_MS;

                // Block here until this link dies; then reconnect.
                waitForLinkClose(remoteNodeId);

            } catch (Exception e) {
                if (!running) {
                    break;
                }
                log.warn("Cannot connect to cluster peer {}: {} — retry in {} ms",
                        peerAddress, e.getMessage(), backoffMs);
                sleepQuietly(backoffMs);
                backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private PeerLink buildLink(Socket socket, String remoteNodeId) throws IOException {
        PeerLink link = new PeerLink(
                socket,
                remoteNodeId,
                config.getNodeId(),
                config.getPacketSize(),
                config.getClusterHeartbeatIntervalMs(),
                topicSubscriptionHandler,
                deduplicator,
                () -> {
                    activePeers.remove(remoteNodeId);
                    log.info("Peer {} removed from active set", remoteNodeId);
                }
        );
        activePeers.put(remoteNodeId, link);
        return link;
    }

    private void waitForLinkClose(String nodeId) throws InterruptedException {
        while (running && activePeers.containsKey(nodeId)) {
            Thread.sleep(50);
        }
    }

    private void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
        }
    }
}
