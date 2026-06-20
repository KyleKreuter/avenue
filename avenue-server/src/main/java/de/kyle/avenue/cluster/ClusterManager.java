package de.kyle.avenue.cluster;

import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
import de.kyle.avenue.metrics.ClusterMetrics;
import de.kyle.avenue.net.SocketFactoryProvider;
import de.kyle.avenue.packet.cluster.ClusterPublishPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Set;
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
 *   <li>Maintains the active {@link PeerLink} set, keyed by remote {@code nodeId}, with a
 *       deterministic tie-break when an inbound and an outbound link to the same peer race.</li>
 *   <li>Implements {@link ClusterForwarder#forward} to fan out a local publish to all peers,
 *       serializing the {@link ClusterPublishPacket} exactly once and sharing the immutable
 *       {@link OutboundItem.PreSerializedFrame} across every target link.</li>
 *   <li>Deduplicates inbound publishes per {@code (originNodeId, originEpoch)} via
 *       {@link OriginSequenceTracker}.</li>
 * </ol>
 * All I/O threads are virtual threads (Java 21).
 */
public class ClusterManager implements ClusterForwarder {

    private static final Logger log = LoggerFactory.getLogger(ClusterManager.class);

    private static final long MIN_BACKOFF_MS = 1_000L;
    private static final long MAX_BACKOFF_MS = 30_000L;

    private final AvenueConfig config;
    private final TopicSubscriptionHandler topicSubscriptionHandler;

    /** Per-node monotonic publish sequencer; one instance per node. */
    private final OriginSequencer originSequencer = new OriginSequencer();

    /** Per-origin ordered dedup trackers, keyed by {@code originNodeId:originEpoch}. */
    private final ConcurrentHashMap<String, OriginSequenceTracker> originTrackers = new ConcurrentHashMap<>();

    /** Cluster transport metrics; folded into the shared metrics snapshot when wired. */
    private final ClusterMetrics clusterMetrics = new ClusterMetrics();

    /** Yields plain or TLS cluster sockets depending on {@code cluster.tls.enabled} (E14). */
    private final ClusterSocketFactory socketFactory;

    /** Active links keyed by the remote node's ID. */
    private final ConcurrentHashMap<String, PeerLink> activePeers = new ConcurrentHashMap<>();

    /** Node IDs blocked from reconnecting (test seam for partition simulation). */
    private final Set<String> blockedPeers = ConcurrentHashMap.newKeySet();

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    private volatile boolean running = false;
    private volatile ServerSocket clusterServerSocket;

    /** Released once the cluster server socket is bound (or the bind attempt failed). */
    private final CountDownLatch boundLatch = new CountDownLatch(1);

    /**
     * Production constructor. Builds the cluster {@link SocketFactoryProvider} (plain or TLS)
     * from configuration exactly as before.
     */
    public ClusterManager(AvenueConfig config, TopicSubscriptionHandler topicSubscriptionHandler) {
        this(config, topicSubscriptionHandler, SocketFactoryProvider.forCluster(
                config.isClusterTlsEnabled(),
                config.getClusterTlsKeystorePath(),
                config.getClusterTlsKeystorePassword(),
                config.getClusterTlsTruststorePath(),
                config.getClusterTlsTruststorePassword()
        ));
    }

    /**
     * Test-oriented constructor accepting an injected {@link ClusterSocketFactory}. The
     * production constructor delegates here with a {@link SocketFactoryProvider}; tests can pass a
     * fake factory to intercept the cluster wire. Production behaviour is unchanged.
     */
    public ClusterManager(
            AvenueConfig config,
            TopicSubscriptionHandler topicSubscriptionHandler,
            ClusterSocketFactory socketFactory
    ) {
        this.config = config;
        this.topicSubscriptionHandler = topicSubscriptionHandler;
        this.socketFactory = socketFactory;
    }

    // -------------------------------------------------------------------------
    // ClusterForwarder implementation
    // -------------------------------------------------------------------------

    /**
     * Forwards a locally-received publish to all connected peers. Stamps this node's
     * {@code nodeId} as {@code originNodeId} and assigns the {@code (epoch, seq)} identity from
     * the local {@link OriginSequencer}. The packet is serialized to envelope bytes exactly once
     * and the same immutable {@link OutboundItem.PreSerializedFrame} is enqueued on every peer
     * link (O(1) instead of O(N) serialization). Non-blocking: a full peer queue drops the frame
     * for that peer (at-most-once).
     */
    @Override
    public void forward(String topic, String source, String data) {
        if (activePeers.isEmpty()) {
            // No peers: do not burn a sequence number, keeping the origin sequence dense.
            return;
        }
        ClusterPublishPacket packet = new ClusterPublishPacket(
                topic,
                source,
                config.getNodeId(),
                originSequencer.epoch(),
                originSequencer.nextSeq(),
                data
        );
        // Serialize ONCE; share the immutable byte[] across all target links.
        OutboundItem.PreSerializedFrame frame =
                new OutboundItem.PreSerializedFrame(PeerLink.serializeEnvelope(packet));
        activePeers.values().forEach(link -> {
            link.forward(frame);
            clusterMetrics.incrementMessagesForwarded();
        });
    }

    /**
     * Acceptance decision for an inbound publish, delegated from {@link PeerLink}. Looks up (or
     * creates) the {@link OriginSequenceTracker} for the origin key and applies ordered dedup.
     */
    private boolean acceptOrigin(String originKey, long seq) {
        return originTrackers
                .computeIfAbsent(originKey, key -> new OriginSequenceTracker())
                .accept(seq);
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

    /** Exposes the cluster metrics (for the shared snapshot log and tests). */
    public ClusterMetrics getClusterMetrics() {
        return clusterMetrics;
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
    // Test seams (partition / reconnect control). @VisibleForTesting
    // -------------------------------------------------------------------------

    /**
     * Closes the link to the given node, if present. The owning connect loop (if any) will
     * observe the closure and reconnect unless the peer is blocked.
     */
    // @VisibleForTesting
    void dropPeer(String nodeId) {
        PeerLink link = activePeers.get(nodeId);
        if (link != null) {
            link.close();
        }
    }

    /** Prevents (re)connecting to the given node and drops any current link. */
    // @VisibleForTesting
    void blockPeer(String nodeId) {
        blockedPeers.add(nodeId);
        dropPeer(nodeId);
    }

    /** Allows connecting to the given node again. */
    // @VisibleForTesting
    void unblockPeer(String nodeId) {
        blockedPeers.remove(nodeId);
    }

    // -------------------------------------------------------------------------
    // Inbound accept loop
    // -------------------------------------------------------------------------

    private void acceptLoop() {
        int port = config.getClusterPort();
        try (ServerSocket server = socketFactory.createServerSocket(port)) {
            clusterServerSocket = server;
            log.info("Cluster accept loop bound on port {} (TLS={})",
                    server.getLocalPort(), socketFactory.isTlsEnabled());
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
            // Cluster links are latency-sensitive: disable Nagle on accepted sockets too (the
            // ServerSocket cannot pre-set this on the child socket).
            peerSocket.setTcpNoDelay(true);

            // Acceptor-side handshake: read peer's hello, then send ours.
            String remoteNodeId = ClusterHandshake.acceptorHandshake(
                    peerSocket,
                    config.getNodeId(),
                    config.getClusterSecret(),
                    config.getPacketSize()
            );

            if (blockedPeers.contains(remoteNodeId)) {
                log.info("Peer {} is blocked, rejecting incoming connection", remoteNodeId);
                closeQuietly(peerSocket);
                return;
            }

            // Tie-break: when both sides connect simultaneously, the node with the lexicographically
            // SMALLER id keeps its OUTBOUND link. So on the inbound side, if our id is smaller than
            // the remote's we prefer to keep our own outbound link and reject this inbound one.
            if (config.getNodeId().compareTo(remoteNodeId) < 0 && activePeers.containsKey(remoteNodeId)) {
                log.info("Tie-break: keeping outbound link to {}, rejecting inbound duplicate", remoteNodeId);
                closeQuietly(peerSocket);
                return;
            }

            PeerLink link = buildLink(peerSocket, remoteNodeId);
            if (link == null) {
                // Lost the putIfAbsent race; an existing link already wins.
                log.info("Already connected to peer {}, dropping duplicate incoming connection", remoteNodeId);
                closeQuietly(peerSocket);
                return;
            }
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
                Socket socket = socketFactory.createSocket(host, port);

                // Initiator-side handshake: send our hello first, then read peer's hello.
                String remoteNodeId = ClusterHandshake.initiatorHandshake(
                        socket,
                        config.getNodeId(),
                        config.getClusterSecret(),
                        config.getPacketSize()
                );

                if (blockedPeers.contains(remoteNodeId)) {
                    log.info("Peer {} is blocked, closing outbound connection", remoteNodeId);
                    closeQuietly(socket);
                    sleepQuietly(backoffMs);
                    continue;
                }

                // Tie-break: the node with the lexicographically LARGER id yields its outbound link
                // to the peer's outbound and relies on the inbound link instead. So if our id is
                // larger and a link already exists, close this outbound duplicate.
                if (config.getNodeId().compareTo(remoteNodeId) > 0 && activePeers.containsKey(remoteNodeId)) {
                    log.info("Tie-break: yielding outbound to {}, using inbound link", remoteNodeId);
                    closeQuietly(socket);
                    waitForLinkClose(remoteNodeId);
                    backoffMs = MIN_BACKOFF_MS;
                    continue;
                }

                PeerLink link = buildLink(socket, remoteNodeId);
                if (link == null) {
                    log.info("Peer {} already connected, closing outbound duplicate", remoteNodeId);
                    closeQuietly(socket);
                    waitForLinkClose(remoteNodeId);
                    backoffMs = MIN_BACKOFF_MS;
                    continue;
                }
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

    /**
     * Builds a {@link PeerLink} and atomically registers it under {@code remoteNodeId}. Returns
     * the started-ready link on success, or {@code null} if another link is already registered
     * for that node (the caller must close its socket). The link is registered via
     * {@code putIfAbsent} BEFORE {@link PeerLink#start()} is called, and its {@code onClose}
     * removes it only by value-equality so a losing link can never evict the registered winner.
     */
    private PeerLink buildLink(Socket socket, String remoteNodeId) throws IOException {
        // Holder so the onClose callback can refer to the link instance it belongs to.
        PeerLink[] self = new PeerLink[1];
        PeerLink link = new PeerLink(
                socket,
                remoteNodeId,
                config.getNodeId(),
                config.getPacketSize(),
                config.getClusterHeartbeatIntervalMs(),
                topicSubscriptionHandler,
                this::acceptOrigin,
                clusterMetrics,
                () -> {
                    // Value-equality removal: only remove if THIS link is the registered one.
                    if (activePeers.remove(remoteNodeId, self[0])) {
                        clusterMetrics.decrementActivePeerLinks();
                        log.info("Peer {} removed from active set", remoteNodeId);
                    }
                }
        );
        self[0] = link;
        PeerLink existing = activePeers.putIfAbsent(remoteNodeId, link);
        if (existing != null) {
            // Lost the race — caller closes the socket; do NOT start this link.
            return null;
        }
        clusterMetrics.incrementActivePeerLinks();
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
