package de.kyle.avenue.cluster;

import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.config.ClusterTuning;
import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
import de.kyle.avenue.metrics.ClusterMetrics;
import de.kyle.avenue.net.SocketFactoryProvider;
import de.kyle.avenue.packet.cluster.ClusterInterestPacket;
import de.kyle.avenue.packet.cluster.ClusterInterestSyncPacket;
import de.kyle.avenue.packet.cluster.ClusterPublishPacket;
import de.kyle.avenue.packet.cluster.ClusterResumePacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages all cluster peer links for a single Avenue node and implements
 * {@link ClusterForwarder}.
 * <p>
 * Phase C — at-least-once delivery:
 * <ul>
 *   <li>Holds one {@link ReplayBuffer} per <b>target node id</b> in {@link #targets}, NOT per link.
 *       The buffer therefore survives a link teardown, so a rebuilt link to the same peer can
 *       backfill everything the peer has not acknowledged (partition + heal). A buffer is freed only
 *       after {@code cluster.origin.expiry-ms} without any active link to that target.</li>
 *   <li>Per-origin ordered dedup via {@link OriginSequenceTracker} (receiving side). Backfill is
 *       idempotent because the tracker rejects any {@code seq <= contiguousSeq}.</li>
 * </ul>
 * <p>
 * Phase D — interest-based routing:
 * <ul>
 *   <li>{@link #forward} no longer broadcasts. It consults the {@link InterestRoutingTable} and
 *       hands the publish only to targets that have a subscriber for the topic. Each publish that is
 *       NOT sent to a connected-but-uninterested peer is counted as {@code interestRoutedSkipped}.</li>
 *   <li><b>Per-link reliability sequence.</b> Because a target now receives only a subset of this
 *       node's publishes (those of topics it cares about), the <em>origin</em> seq stream over that
 *       link is sparse. The at-least-once layer therefore keys on a dense, gap-free per-target
 *       {@code linkSeq} assigned in {@link TargetState#nextLinkSeq()} — see the class-level note on
 *       {@link PeerLink}. The publish frame still carries the origin identity for dedup. As a direct
 *       consequence the same logical publish is serialized once per interested target (each carries a
 *       different linkSeq), instead of the Phase A/C single-serialization broadcast.</li>
 *   <li><b>Interest propagation.</b> A local {@link TopicSubscriptionHandler.InterestListener} fires
 *       on the first/last local subscription of a topic. Each transition bumps a monotonic
 *       {@link #interestVersion}, updates {@link #localInterest}, and floods a best-effort
 *       {@link ClusterInterestPacket} delta to all peers. A periodic full-state
 *       {@link ClusterInterestSyncPacket} (anti-entropy) heals any lost delta. A new link also gets
 *       an immediate full-state sync on connect.</li>
 *   <li><b>Receiving-side reliability split.</b> {@link #linkTrackers} tracks contiguous
 *       {@code linkSeq} per <em>sending</em> peer (drives ACK / resume / gap), while
 *       {@link #originTrackers} keeps the Phase C origin dedup. The two are independent.</li>
 *   <li><b>DEAD/LEFT cleanup.</b> A peer's interest is reclaimed when its {@link TargetState}
 *       expires (no link for {@code origin.expiry-ms}), NOT on link teardown — see the comment in
 *       {@code buildLink}'s onClose hook for why a partitioned-but-known target must keep being
 *       routed to so its replay buffer can backfill on heal.</li>
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

    /**
     * Per-origin ordered dedup trackers, keyed by {@code originNodeId:originEpoch} (receiving-side
     * logical dedup over the ORIGIN seq).
     */
    private final ConcurrentHashMap<String, OriginSequenceTracker> originTrackers = new ConcurrentHashMap<>();

    /**
     * Per-sending-peer reliability trackers, keyed by the sender's node id, over the dense per-link
     * {@code linkSeq}. Drive cumulative ACK / resume / gap. Kept on the manager so they survive a
     * link teardown and a reconnect can resume from the last contiguous linkSeq.
     */
    private final ConcurrentHashMap<String, OriginSequenceTracker> linkTrackers = new ConcurrentHashMap<>();

    /**
     * Per-target send state (replay buffer + publish ingest queue + per-link sequencer), keyed by
     * the remote node id. Survives link teardown so a reconnect can resume and backfill.
     */
    private final ConcurrentHashMap<String, TargetState> targets = new ConcurrentHashMap<>();

    /** Generous bound on the per-target publish ingest queue (independent of replay capacity). */
    private static final int INGEST_QUEUE_CAPACITY = 65_536;

    private final ClusterMetrics clusterMetrics = new ClusterMetrics();
    private final ClusterSocketFactory socketFactory;
    private final ConcurrentHashMap<String, PeerLink> activePeers = new ConcurrentHashMap<>();
    private final Set<String> blockedPeers = ConcurrentHashMap.newKeySet();

    // -------------------------------------------------------------------------
    // Phase D — interest state
    // -------------------------------------------------------------------------

    /** Remote interest: which peers want which topics. Read on the hot publish path. */
    private final InterestRoutingTable routingTable = new InterestRoutingTable(clusterMetrics);

    /** This node's own set of topics with at least one local subscriber (propagated to peers). */
    private final Set<String> localInterest = ConcurrentHashMap.newKeySet();

    /** Monotonic interest version, separate from the publish seq. Stamps every interest update. */
    private final AtomicLong interestVersion = new AtomicLong(0);

    /**
     * Broadcast-grace bookkeeping: topic -> wall-clock deadline (ms) until which a topology change
     * makes us flood that topic to ALL peers (not just interested ones), closing the subscribe/
     * publish race. Empty and unused when {@code cluster.interest.broadcast-grace-ms == 0}.
     */
    private final ConcurrentHashMap<String, Long> broadcastGraceUntil = new ConcurrentHashMap<>();

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    private volatile boolean running = false;
    private volatile ServerSocket clusterServerSocket;
    private final CountDownLatch boundLatch = new CountDownLatch(1);

    /**
     * Per-target durable send state that outlives any single {@link PeerLink}: the replay buffer
     * (sent-but-unacked entries, for backfill), the publish ingest queue (handoff from
     * {@link #forward}, drained by the active link's writer) and the per-target {@code linkSeq}
     * sequencer that makes the reliability stream dense even under selective interest routing.
     */
    static final class TargetState {
        final ReplayBuffer buffer;
        final java.util.concurrent.BlockingQueue<ReplayBuffer.Entry> ingest;
        /**
         * Per-link reliability sequence. Assigned ONLY when a frame is actually handed to this
         * target, so the linkSeq stream this peer sees is dense/contiguous regardless of which
         * topics are filtered out — exactly what the contiguous ACK/backfill machinery needs.
         */
        final AtomicLong linkSeq = new AtomicLong(0);
        volatile long lastTouchMs;

        TargetState(ReplayBuffer buffer, int ingestCapacity, long nowMs) {
            this.buffer = buffer;
            this.ingest = new java.util.concurrent.LinkedBlockingQueue<>(ingestCapacity);
            this.lastTouchMs = nowMs;
        }

        long nextLinkSeq() {
            return linkSeq.incrementAndGet();
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
        // Phase D: subscribe to local interest transitions so we can propagate them.
        this.topicSubscriptionHandler.setInterestListener(new TopicSubscriptionHandler.InterestListener() {
            @Override
            public void onInterestAdded(String topic) {
                onLocalInterestChanged(topic, true);
            }

            @Override
            public void onInterestRemoved(String topic) {
                onLocalInterestChanged(topic, false);
            }
        });
    }

    // -------------------------------------------------------------------------
    // ClusterForwarder implementation
    // -------------------------------------------------------------------------

    /**
     * Forwards a locally-received publish only to the peers that are interested in {@code topic}
     * (Phase D), instead of broadcasting. Assigns the origin {@code (epoch, seq)} identity once for
     * dedup; then, for every interested target, stamps a target-specific dense {@code linkSeq},
     * serializes the frame for that target and hands it off non-blockingly. Connected peers that are
     * NOT interested are skipped and counted in {@code interestRoutedSkipped}.
     * <p>
     * Optional broadcast-grace: for {@code cluster.interest.broadcast-grace-ms} after a topology
     * change on {@code topic}, the topic is flooded to ALL peers to cover the ~RTT window in which a
     * fresh remote subscription's interest has not yet reached us.
     * <p>
     * Strictly non-blocking: backpressure is absorbed on each link's writer thread by its
     * {@link ReplayBuffer}, never here on the publish path. The local delivery path is unaffected.
     */
    @Override
    public void forward(String topic, String source, String data) {
        if (targets.isEmpty()) {
            // No target ever discovered: do not burn a sequence number.
            return;
        }
        // Decide the recipient set: interested peers, or ALL peers during a broadcast-grace window.
        Set<String> interested = routingTable.peersFor(topic);
        boolean flood = isInBroadcastGrace(topic);

        long originSeq = originSequencer.nextSeq();
        ClusterPublishPacket base = new ClusterPublishPacket(
                topic, source, config.getNodeId(), originSequencer.epoch(), originSeq, data);

        long skipped = 0;
        for (var targetEntry : targets.entrySet()) {
            String nodeId = targetEntry.getKey();
            TargetState target = targetEntry.getValue();
            boolean wants = flood || interested.contains(nodeId);
            if (!wants) {
                // Only count it as a saving if the peer is actually connected — an offline target
                // would not have received a broadcast anyway.
                if (activePeers.containsKey(nodeId)) {
                    skipped++;
                }
                continue;
            }
            // Assign this target's own dense reliability sequence and serialize the frame for it.
            long linkSeq = target.nextLinkSeq();
            byte[] payload = PeerLink.serializeEnvelope(base.withLinkSeq(linkSeq));
            ReplayBuffer.Entry entry = new ReplayBuffer.Entry(linkSeq, payload);
            if (target.ingest.offer(entry)) {
                clusterMetrics.incrementMessagesForwarded();
            } else {
                clusterMetrics.incrementMessagesDropped();
                clusterMetrics.incrementClusterGapEvents();
            }
        }
        clusterMetrics.addInterestRoutedSkipped(skipped);
    }

    // -------------------------------------------------------------------------
    // Phase D — local interest propagation
    // -------------------------------------------------------------------------

    /**
     * Handles a local first-subscriber (added) / last-subscriber (removed) transition for a topic:
     * updates the local interest set, bumps the interest version, opens the broadcast-grace window
     * (if enabled) and floods a best-effort interest delta to all connected peers.
     */
    private void onLocalInterestChanged(String topic, boolean added) {
        if (added) {
            localInterest.add(topic);
        } else {
            localInterest.remove(topic);
        }
        long version = interestVersion.incrementAndGet();
        openBroadcastGrace(topic);

        ClusterInterestPacket delta = new ClusterInterestPacket(
                config.getNodeId(), version, added ? "ADD" : "REMOVE", topic);
        for (PeerLink link : activePeers.values()) {
            if (link.enqueueControl(delta)) {
                clusterMetrics.incrementInterestUpdatesSent();
            }
        }
    }

    /** Builds the full local interest snapshot for an initial / periodic anti-entropy sync. */
    private ClusterInterestSyncPacket buildInterestSync() {
        // Snapshot under the current version. A concurrent change bumps the version again and sends
        // its own delta + the next periodic sync, so a momentarily stale snapshot self-heals.
        return new ClusterInterestSyncPacket(
                config.getNodeId(), interestVersion.get(), Set.copyOf(localInterest));
    }

    /** Periodically floods the full interest state to every peer (anti-entropy; heals lost deltas). */
    private void interestSyncLoop() {
        long intervalMs = Math.max(50L, tuning.interestSyncIntervalMs());
        try {
            while (running) {
                Thread.sleep(intervalMs);
                if (activePeers.isEmpty()) {
                    continue;
                }
                ClusterInterestSyncPacket sync = buildInterestSync();
                for (PeerLink link : activePeers.values()) {
                    if (link.enqueueControl(sync)) {
                        clusterMetrics.incrementInterestUpdatesSent();
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void openBroadcastGrace(String topic) {
        long graceMs = tuning.interestBroadcastGraceMs();
        if (graceMs > 0) {
            broadcastGraceUntil.put(topic, System.currentTimeMillis() + graceMs);
        }
    }

    private boolean isInBroadcastGrace(String topic) {
        if (tuning.interestBroadcastGraceMs() <= 0) {
            return false;
        }
        Long until = broadcastGraceUntil.get(topic);
        if (until == null) {
            return false;
        }
        if (System.currentTimeMillis() <= until) {
            return true;
        }
        broadcastGraceUntil.remove(topic, until); // expired — prune lazily
        return false;
    }

    // -------------------------------------------------------------------------
    // Receiving-side reliability + dedup (delegated to from PeerLink.ReceiverContext)
    // -------------------------------------------------------------------------

    /** RELIABILITY: accept a linkSeq on the wire from a sending peer (contiguous frontier). */
    private boolean acceptLink(String senderNodeId, long linkSeq) {
        return linkTrackers
                .computeIfAbsent(senderNodeId, k -> new OriginSequenceTracker())
                .accept(linkSeq);
    }

    private long contiguousLinkSeq(String senderNodeId) {
        OriginSequenceTracker t = linkTrackers.get(senderNodeId);
        return t == null ? 0L : t.contiguousSeq();
    }

    private void resetLinkTo(String senderNodeId, long newContiguous) {
        linkTrackers
                .computeIfAbsent(senderNodeId, k -> new OriginSequenceTracker())
                .resetTo(newContiguous);
    }

    /** DEDUP: accept an origin seq for logical deduplication (Phase C semantics). */
    private boolean acceptOrigin(String originKey, long originSeq) {
        return originTrackers
                .computeIfAbsent(originKey, k -> new OriginSequenceTracker())
                .accept(originSeq);
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
        executor.submit(this::interestSyncLoop);

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

    /** @VisibleForTesting — exposes the interest routing table for convergence assertions. */
    InterestRoutingTable getRoutingTable() {
        return routingTable;
    }

    /**
     * @VisibleForTesting — whether this node's routing table currently records {@code remoteNodeId}
     * as interested in {@code topic}. Lets tests poll for interest convergence deterministically
     * (no fixed sleeps) before asserting on cross-node routing.
     */
    public boolean knowsInterest(String remoteNodeId, String topic) {
        return routingTable.peersFor(topic).contains(remoteNodeId);
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
     * per-target {@link ReplayBuffer} and {@code linkSeq} sequencer are fetched (or created) here and
     * reused across reconnects, so a rebuilt link can resume the same dense linkSeq stream and
     * backfill. Returns {@code null} if another link is already registered.
     */
    private PeerLink buildLink(Socket socket, String remoteNodeId) throws IOException {
        TargetState target = targetFor(remoteNodeId);

        PeerLink.ReceiverContext receiver = new PeerLink.ReceiverContext() {
            @Override
            public boolean acceptLink(String senderNodeId, long linkSeq) {
                return ClusterManager.this.acceptLink(senderNodeId, linkSeq);
            }

            @Override
            public long contiguousLinkSeq(String senderNodeId) {
                return ClusterManager.this.contiguousLinkSeq(senderNodeId);
            }

            @Override
            public void resetLinkTo(String senderNodeId, long newContiguous) {
                ClusterManager.this.resetLinkTo(senderNodeId, newContiguous);
            }

            @Override
            public long linkResumePoint(String senderNodeId) {
                return ClusterManager.this.contiguousLinkSeq(senderNodeId);
            }

            @Override
            public boolean acceptOrigin(String originKey, long originSeq) {
                return ClusterManager.this.acceptOrigin(originKey, originSeq);
            }

            @Override
            public void onInterestDelta(String nodeId, long version, String op, String topic) {
                InterestRoutingTable.Op o = "ADD".equalsIgnoreCase(op)
                        ? InterestRoutingTable.Op.ADD : InterestRoutingTable.Op.REMOVE;
                routingTable.applyDelta(nodeId, version, o, topic);
            }

            @Override
            public void onInterestSync(String nodeId, long version, Set<String> topics) {
                routingTable.replaceFullState(nodeId, version, topics);
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
                this::buildInterestSync,
                () -> {
                    if (activePeers.remove(remoteNodeId, self[0])) {
                        clusterMetrics.decrementActivePeerLinks();
                        // Mark the target for expiry: it survives until origin.expiry-ms without a link.
                        touchTarget(remoteNodeId);
                        // Phase D / Phase C interplay: do NOT drop the peer's interest here. The
                        // TargetState (replay buffer + ingest queue + linkSeq) survives the teardown
                        // until origin.expiry so a reconnect can backfill — and that backfill is only
                        // populated if publishes for the peer's topics keep being routed into its
                        // ingest queue DURING the partition. Hard-removing interest on disconnect
                        // would silently drop those partition-time publishes and break heal/backfill.
                        // Instead the interest is reclaimed together with the TargetState in the
                        // expiry sweep (a genuine LEFT), and a real reconnect overwrites it with a
                        // fresh full-state sync anyway.
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
     * {@code cluster.origin.expiry-ms}. Active targets are never reclaimed.
     */
    private void originExpirySweepLoop() {
        long expiryMs = tuning.originExpiryMs();
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
                            // Phase D DEAD/LEFT cleanup: the target is gone for good now, so reclaim
                            // its interest and its receiving-side reliability tracker too.
                            routingTable.removeNode(nodeId);
                            linkTrackers.remove(nodeId);
                            log.info("Released expired replay + interest state for target {}", nodeId);
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
