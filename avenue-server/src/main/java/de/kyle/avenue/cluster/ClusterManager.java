package de.kyle.avenue.cluster;

import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.config.ClusterTuning;
import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
import de.kyle.avenue.metrics.ClusterMetrics;
import de.kyle.avenue.net.SocketFactoryProvider;
import de.kyle.avenue.packet.cluster.ClusterPublishPacket;
import de.kyle.avenue.packet.cluster.ClusterResumePacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Manages all cluster peer links for a single Avenue node and implements
 * {@link ClusterForwarder}.
 * <p>
 * Phase C — at-least-once delivery:
 * <ul>
 *   <li>Holds one {@link ReplayBuffer} per <b>target node id</b> in {@link #replayBuffers}, NOT per
 *       link. The buffer therefore survives a link teardown, so a rebuilt link to the same peer can
 *       backfill everything the peer has not acknowledged (partition + heal). A buffer is freed only
 *       after {@code cluster.origin.expiry-ms} without any active link to that target.</li>
 *   <li>{@link #forward} assigns one strictly-monotonic {@code seq} from the single
 *       {@link OriginSequencer} per local publish (origin == self) and hands it to every active
 *       link's writer non-blockingly. The blocking {@code append} into the replay ring happens on
 *       the writer thread, so the local delivery / client-reader path never stalls.</li>
 *   <li>Per-origin ordered dedup via {@link OriginSequenceTracker} (receiving side). Backfill is
 *       idempotent because the tracker rejects any {@code seq <= contiguousSeq}.</li>
 * </ul>
 * All I/O threads are virtual threads (Java 21).
 */
public class ClusterManager implements ClusterForwarder {

    private static final Logger log = LoggerFactory.getLogger(ClusterManager.class);

    private static final long MIN_BACKOFF_MS = 1_000L;
    private static final long MAX_BACKOFF_MS = 30_000L;

    private final AvenueConfig config;
    private final ClusterTuning tuning;
    private final TopicSubscriptionHandler topicSubscriptionHandler;

    /** Per-node monotonic publish sequencer; one instance per node (single counter source). */
    private final OriginSequencer originSequencer = new OriginSequencer();

    private final HmacAuthenticator authenticator;

    /** Per-origin ordered dedup trackers, keyed by {@code originNodeId:originEpoch}. */
    private final ConcurrentHashMap<String, OriginSequenceTracker> originTrackers = new ConcurrentHashMap<>();

    /** Origin metadata (nodeId, epoch) per origin key, so resume/ack packets can be reconstructed. */
    private final ConcurrentHashMap<String, OriginMeta> originMeta = new ConcurrentHashMap<>();

    /**
     * Per-target send state (replay buffer + publish ingest queue), keyed by the remote node id.
     * Survives link teardown so a reconnect can resume and backfill, and so messages published
     * during a partition accumulate in the ingest queue until the link returns. Carries a
     * last-touch timestamp for {@code origin.expiry-ms} reclamation.
     */
    private final ConcurrentHashMap<String, TargetState> targets = new ConcurrentHashMap<>();

    /** Generous bound on the per-target publish ingest queue (independent of replay capacity). */
    private static final int INGEST_QUEUE_CAPACITY = 65_536;

    private final ClusterMetrics clusterMetrics = new ClusterMetrics();
    private final ClusterSocketFactory socketFactory;
    private final ConcurrentHashMap<String, PeerLink> activePeers = new ConcurrentHashMap<>();
    private final Set<String> blockedPeers = ConcurrentHashMap.newKeySet();

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    private volatile boolean running = false;
    private volatile ServerSocket clusterServerSocket;
    private final CountDownLatch boundLatch = new CountDownLatch(1);

    /** Immutable origin descriptor for reconstructing resume/ack packets. */
    private record OriginMeta(String originNodeId, long originEpoch) {
    }

    /**
     * Per-target durable send state that outlives any single {@link PeerLink}: the replay buffer
     * (sent-but-unacked entries, for backfill) and the publish ingest queue (handoff from
     * {@link #forward}, drained by the active link's writer; accumulates during a partition).
     */
    private static final class TargetState {
        final ReplayBuffer buffer;
        final java.util.concurrent.BlockingQueue<ReplayBuffer.Entry> ingest;
        volatile long lastTouchMs;

        TargetState(ReplayBuffer buffer, int ingestCapacity, long nowMs) {
            this.buffer = buffer;
            this.ingest = new java.util.concurrent.LinkedBlockingQueue<>(ingestCapacity);
            this.lastTouchMs = nowMs;
        }
    }

    public ClusterManager(AvenueConfig config, TopicSubscriptionHandler topicSubscriptionHandler) {
        this(config, topicSubscriptionHandler, SocketFactoryProvider.forCluster(
                config.isClusterTlsEnabled(),
                config.getClusterTlsKeystorePath(),
                config.getClusterTlsKeystorePassword(),
                config.getClusterTlsTruststorePath(),
                config.getClusterTlsTruststorePassword()
        ));
    }

    public ClusterManager(
            AvenueConfig config,
            TopicSubscriptionHandler topicSubscriptionHandler,
            ClusterSocketFactory socketFactory
    ) {
        this.config = config;
        this.tuning = config.getClusterTuning();
        this.topicSubscriptionHandler = topicSubscriptionHandler;
        this.socketFactory = socketFactory;
        this.authenticator = new HmacAuthenticator(config.getClusterSecret());
    }

    // -------------------------------------------------------------------------
    // ClusterForwarder implementation
    // -------------------------------------------------------------------------

    /**
     * Forwards a locally-received publish to all connected peers. Assigns one
     * {@code (epoch, seq)} identity from the {@link OriginSequencer} (single counter source, so the
     * seq stream towards every target is already strictly monotonic), serializes the
     * {@link ClusterPublishPacket} exactly once, and hands the same bytes to every active link's
     * writer. Strictly non-blocking: backpressure is absorbed on each link's writer thread by its
     * {@link ReplayBuffer}, never here on the publish path.
     */
    @Override
    public void forward(String topic, String source, String data) {
        if (targets.isEmpty()) {
            // No target ever discovered: do not burn a sequence number, keeping the origin sequence
            // dense for the first peer that connects.
            return;
        }
        long seq = originSequencer.nextSeq();
        ClusterPublishPacket packet = new ClusterPublishPacket(
                topic, source, config.getNodeId(),
                originSequencer.epoch(), seq, data);
        byte[] payload = PeerLink.serializeEnvelope(packet);
        ReplayBuffer.Entry entry = new ReplayBuffer.Entry(seq, payload);
        // Hand off non-blockingly to every known target's ingest queue. The active link's writer
        // appends to the replay ring (applying backpressure on ITS thread) and sends; for a
        // partitioned target the entry waits in the ingest queue until the link returns. This keeps
        // the local publish / client-reader thread strictly non-blocking.
        for (TargetState target : targets.values()) {
            if (target.ingest.offer(entry)) {
                clusterMetrics.incrementMessagesForwarded();
            } else {
                // Ingest overflow (a long partition or a persistently slow peer): drop + gap, never
                // block the publish path and never kill anything.
                clusterMetrics.incrementMessagesDropped();
                clusterMetrics.incrementClusterGapEvents();
            }
        }
    }

    // -------------------------------------------------------------------------
    // ReceiverContext (per-link, receiving side) — delegates to manager-owned state
    // -------------------------------------------------------------------------

    private boolean acceptOrigin(String originKey, long seq) {
        rememberOrigin(originKey);
        return originTrackers
                .computeIfAbsent(originKey, k -> new OriginSequenceTracker())
                .accept(seq);
    }

    private long contiguousSeqFor(String originKey) {
        OriginSequenceTracker tracker = originTrackers.get(originKey);
        return tracker == null ? 0L : tracker.contiguousSeq();
    }

    private void resetTrackerTo(String originKey, long newContiguous) {
        rememberOrigin(originKey);
        originTrackers
                .computeIfAbsent(originKey, k -> new OriginSequenceTracker())
                .resetTo(newContiguous);
    }

    /** Builds the resume/ack points for every origin this node has received from. */
    private List<ClusterResumePacket.Resume> resumePoints() {
        List<ClusterResumePacket.Resume> points = new ArrayList<>();
        for (var entry : originMeta.entrySet()) {
            OriginMeta meta = entry.getValue();
            long contiguous = contiguousSeqFor(entry.getKey());
            points.add(new ClusterResumePacket.Resume(meta.originNodeId(), meta.originEpoch(), contiguous));
        }
        return points;
    }

    private void rememberOrigin(String originKey) {
        originMeta.computeIfAbsent(originKey, k -> {
            int idx = k.lastIndexOf(':');
            String nodeId = idx >= 0 ? k.substring(0, idx) : k;
            long epoch = 0L;
            if (idx >= 0) {
                try {
                    epoch = Long.parseLong(k.substring(idx + 1));
                } catch (NumberFormatException ignored) {
                    // leave epoch 0
                }
            }
            return new OriginMeta(nodeId, epoch);
        });
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    public void start() {
        running = true;
        executor.submit(this::acceptLoop);
        try {
            boundLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        executor.submit(this::originExpirySweepLoop);

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

    public AvenueConfig getConfig() {
        return config;
    }

    public ClusterMetrics getClusterMetrics() {
        return clusterMetrics;
    }

    public int getClusterPort() {
        ServerSocket ss = clusterServerSocket;
        return (ss != null && ss.isBound()) ? ss.getLocalPort() : -1;
    }

    public int getActivePeerCount() {
        return activePeers.size();
    }

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

    void dropPeer(String nodeId) {
        PeerLink link = activePeers.get(nodeId);
        if (link != null) {
            link.close();
        }
    }

    void blockPeer(String nodeId) {
        blockedPeers.add(nodeId);
        dropPeer(nodeId);
    }

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
            peerSocket.setTcpNoDelay(true);
            ClusterHandshake.AuthResult auth = ClusterHandshake.acceptorHandshake(
                    peerSocket, config.getNodeId(), advertisedHost(), getClusterPort(),
                    originSequencer.epoch(), authenticator, config.getPacketSize(),
                    ClusterHandshake.DEFAULT_HANDSHAKE_TIMEOUT_MS, clusterMetrics);
            String remoteNodeId = auth.remoteNodeId();

            if (blockedPeers.contains(remoteNodeId)) {
                log.info("Peer {} is blocked, rejecting incoming connection", remoteNodeId);
                closeQuietly(peerSocket);
                return;
            }
            if (config.getNodeId().compareTo(remoteNodeId) < 0 && activePeers.containsKey(remoteNodeId)) {
                log.info("Tie-break: keeping outbound link to {}, rejecting inbound duplicate", remoteNodeId);
                closeQuietly(peerSocket);
                return;
            }

            PeerLink link = buildLink(peerSocket, remoteNodeId);
            if (link == null) {
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

                ClusterHandshake.AuthResult auth = ClusterHandshake.initiatorHandshake(
                        socket, config.getNodeId(), advertisedHost(), getClusterPort(),
                        originSequencer.epoch(), authenticator, config.getPacketSize(),
                        ClusterHandshake.DEFAULT_HANDSHAKE_TIMEOUT_MS, clusterMetrics);
                String remoteNodeId = auth.remoteNodeId();

                if (blockedPeers.contains(remoteNodeId)) {
                    log.info("Peer {} is blocked, closing outbound connection", remoteNodeId);
                    closeQuietly(socket);
                    sleepQuietly(backoffMs);
                    continue;
                }
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
     * Builds a {@link PeerLink} and atomically registers it under {@code remoteNodeId}. The
     * per-target {@link ReplayBuffer} is fetched (or created) here and reused across reconnects, so
     * a rebuilt link can backfill. Returns {@code null} if another link is already registered.
     */
    private PeerLink buildLink(Socket socket, String remoteNodeId) throws IOException {
        TargetState target = targetFor(remoteNodeId);

        PeerLink.ReceiverContext receiver = new PeerLink.ReceiverContext() {
            @Override
            public boolean accept(String originKey, long seq) {
                return acceptOrigin(originKey, seq);
            }

            @Override
            public long contiguousSeq(String originKey) {
                return contiguousSeqFor(originKey);
            }

            @Override
            public void resetTo(String originKey, long newContiguous) {
                resetTrackerTo(originKey, newContiguous);
            }

            @Override
            public List<ClusterResumePacket.Resume> resumePoints() {
                return ClusterManager.this.resumePoints();
            }
        };

        PeerLink[] self = new PeerLink[1];
        PeerLink link = new PeerLink(
                socket,
                remoteNodeId,
                config.getNodeId(),
                originSequencer.epoch(),
                config.getPacketSize(),
                config.getClusterHeartbeatIntervalMs(),
                tuning.ackIntervalMs(),
                tuning.strictOrdering(),
                topicSubscriptionHandler,
                receiver,
                target.buffer,
                target.ingest,
                clusterMetrics,
                Clock.SYSTEM,
                () -> {
                    if (activePeers.remove(remoteNodeId, self[0])) {
                        clusterMetrics.decrementActivePeerLinks();
                        // Mark the target for expiry: it survives until origin.expiry-ms without a link.
                        touchTarget(remoteNodeId);
                        log.info("Peer {} removed from active set", remoteNodeId);
                    }
                }
        );
        self[0] = link;
        PeerLink existing = activePeers.putIfAbsent(remoteNodeId, link);
        if (existing != null) {
            return null;
        }
        clusterMetrics.incrementActivePeerLinks();
        touchTarget(remoteNodeId);
        return link;
    }

    /** Returns the per-target send state, creating it on first use and refreshing its touch time. */
    private TargetState targetFor(String remoteNodeId) {
        TargetState target = targets.computeIfAbsent(remoteNodeId, k ->
                new TargetState(new ReplayBuffer(
                        tuning.replayCapacity(),
                        tuning.backpressurePolicy(),
                        tuning.replayOfferTimeoutMs(),
                        clusterMetrics), INGEST_QUEUE_CAPACITY, System.currentTimeMillis()));
        target.lastTouchMs = System.currentTimeMillis();
        return target;
    }

    private void touchTarget(String remoteNodeId) {
        TargetState target = targets.get(remoteNodeId);
        if (target != null) {
            target.lastTouchMs = System.currentTimeMillis();
        }
    }

    /**
     * Periodically frees replay buffers for targets that have had no active link for longer than
     * {@code cluster.origin.expiry-ms}. Releasing the buffer's depth-gauge contribution avoids a
     * gauge leak. Active targets are never reclaimed.
     */
    private void originExpirySweepLoop() {
        long expiryMs = tuning.originExpiryMs();
        // Sweep at a fraction of the expiry interval, bounded so tests with a tiny expiry still tick.
        long sweepMs = Math.max(50L, Math.min(expiryMs, 5_000L));
        try {
            while (running) {
                Thread.sleep(sweepMs);
                long now = System.currentTimeMillis();
                for (var entry : targets.entrySet()) {
                    String nodeId = entry.getKey();
                    if (activePeers.containsKey(nodeId)) {
                        continue; // never expire a target with a live link
                    }
                    TargetState target = entry.getValue();
                    if (now - target.lastTouchMs >= expiryMs) {
                        if (targets.remove(nodeId, target)) {
                            target.buffer.release();
                            log.info("Released expired replay state for target {}", nodeId);
                        }
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private String advertisedHost() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return "127.0.0.1";
        }
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
