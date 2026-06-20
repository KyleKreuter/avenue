package de.kyle.avenue.config;

import de.kyle.avenue.cluster.ReplayBuffer;

/**
 * Bundle of cluster transport tuning knobs introduced with Phase C's at-least-once delivery and
 * extended through Phase D (interest routing) and Phase E (SWIM membership).
 * <p>
 * These are gathered into a single record (instead of being added as yet more
 * {@link AvenueConfig} constructor parameters) so the existing 7-, 13- and 26-argument
 * {@code AvenueConfig} constructors — and every test that calls them — stay source-compatible: they
 * simply delegate to {@link #defaults()}. The file/{@code .env} constructor builds a populated
 * instance from the {@code cluster.replay.*}, {@code cluster.ack-interval-ms},
 * {@code cluster.strict-ordering}, {@code cluster.origin.expiry-ms}, {@code cluster.interest.*} and
 * {@code cluster.swim.*} properties.
 *
 * @param replayCapacity        max buffered un-acked messages per target ({@code cluster.replay.capacity})
 * @param backpressurePolicy    policy when the replay ring is full ({@code cluster.replay.backpressure.policy})
 * @param replayOfferTimeoutMs  how long a writer waits for ring space under BLOCK
 *                              ({@code cluster.replay.offer-timeout-ms})
 * @param ackIntervalMs         cumulative ACK send interval in ms ({@code cluster.ack-interval-ms})
 * @param strictOrdering        hold out-of-order seqs until contiguous before delivery
 *                              ({@code cluster.strict-ordering})
 * @param originExpiryMs        how long a replay buffer survives without a reconnect before it is
 *                              freed ({@code cluster.origin.expiry-ms})
 * @param interestSyncIntervalMs period of the anti-entropy full interest-state sync to every peer
 *                              ({@code cluster.interest.sync-interval-ms}); Phase D
 * @param interestBroadcastGraceMs grace window after an interest topology change during which a node
 *                              floods the affected topic to ALL peers (closes the subscribe/publish
 *                              race), {@code 0} = off ({@code cluster.interest.broadcast-grace-ms})
 * @param swimProbeIntervalMs   SWIM probe period in ms ({@code cluster.swim.probe-interval-ms}); Phase E
 * @param swimProbeTimeoutMs    SWIM (direct and indirect) probe timeout in ms
 *                              ({@code cluster.swim.probe-timeout-ms})
 * @param swimIndirectProbeCount number of helpers used for an indirect probe
 *                              ({@code cluster.swim.indirect-probe-count})
 * @param swimSuspicionTimeoutMs how long a member stays SUSPECT before being declared DEAD
 *                              ({@code cluster.swim.suspicion-timeout-ms})
 * @param swimGossipFanout      max number of gossip updates piggybacked per SWIM packet
 *                              ({@code cluster.swim.gossip-fanout})
 * @param swimDeadMemberTimeoutMs how long a DEAD/LEFT member is retained before reaping
 *                              ({@code cluster.swim.dead-member-timeout-ms})
 * @param advertisedHost        host this node advertises to peers ({@code cluster.advertised-host};
 *                              empty = auto-detect the local IP)
 * @param clusterPacketMaxSize  max frame size for cluster links, larger than the client packet size
 *                              to fit JoinAck member lists ({@code cluster.packet.max-size})
 * @param batchMaxFrames        max frames the cluster writer coalesces into one flush
 *                              ({@code cluster.batch.max-frames}); {@code 1} reproduces the legacy
 *                              per-frame flush behaviour. Write-batching is opportunistic: at low
 *                              load a batch is a single frame, so there is no added latency.
 * @param batchMaxDelayMicros   optional linger window (microseconds) the cluster writer may park to
 *                              accumulate more frames before flushing ({@code cluster.batch.max-delay-micros});
 *                              {@code 0} (default) means purely opportunistic — no artificial wait.
 */
public record ClusterTuning(
        int replayCapacity,
        ReplayBuffer.BackpressurePolicy backpressurePolicy,
        long replayOfferTimeoutMs,
        long ackIntervalMs,
        boolean strictOrdering,
        long originExpiryMs,
        long interestSyncIntervalMs,
        long interestBroadcastGraceMs,
        long swimProbeIntervalMs,
        long swimProbeTimeoutMs,
        int swimIndirectProbeCount,
        long swimSuspicionTimeoutMs,
        int swimGossipFanout,
        long swimDeadMemberTimeoutMs,
        String advertisedHost,
        int clusterPacketMaxSize,
        int batchMaxFrames,
        long batchMaxDelayMicros
) {

    public static final int DEFAULT_REPLAY_CAPACITY = 65_536;
    public static final long DEFAULT_REPLAY_OFFER_TIMEOUT_MS = 1_000L;
    public static final long DEFAULT_ACK_INTERVAL_MS = 200L;
    public static final boolean DEFAULT_STRICT_ORDERING = false;
    public static final long DEFAULT_ORIGIN_EXPIRY_MS = 300_000L;
    public static final long DEFAULT_INTEREST_SYNC_INTERVAL_MS = 10_000L;
    public static final long DEFAULT_INTEREST_BROADCAST_GRACE_MS = 0L;

    // Phase E — SWIM membership defaults.
    public static final long DEFAULT_SWIM_PROBE_INTERVAL_MS = 1_000L;
    public static final long DEFAULT_SWIM_PROBE_TIMEOUT_MS = 500L;
    public static final int DEFAULT_SWIM_INDIRECT_PROBE_COUNT = 3;
    public static final long DEFAULT_SWIM_SUSPICION_TIMEOUT_MS = 5_000L;
    public static final int DEFAULT_SWIM_GOSSIP_FANOUT = 4;
    public static final long DEFAULT_SWIM_DEAD_MEMBER_TIMEOUT_MS = 30_000L;
    public static final String DEFAULT_ADVERTISED_HOST = "";
    public static final int DEFAULT_CLUSTER_PACKET_MAX_SIZE = 1_048_576;

    // Write-batching (write-coalescing) defaults. Batching is opportunistic and default-on:
    // max-frames=64 caps a batch, max-delay-micros=0 means no linger (no added latency).
    public static final int DEFAULT_BATCH_MAX_FRAMES = 64;
    public static final long DEFAULT_BATCH_MAX_DELAY_MICROS = 0L;

    /**
     * Compact canonical constructor: applies the SWIM defaults if a caller (a legacy short
     * constructor or {@code defaults()}) leaves the Phase E knobs at sentinel values, and normalizes
     * a {@code null} advertised host to the empty string. Concretely, non-positive SWIM intervals /
     * counts are coerced to their defaults so a partially-specified tuning is always usable.
     */
    public ClusterTuning {
        if (swimProbeIntervalMs <= 0) swimProbeIntervalMs = DEFAULT_SWIM_PROBE_INTERVAL_MS;
        if (swimProbeTimeoutMs <= 0) swimProbeTimeoutMs = DEFAULT_SWIM_PROBE_TIMEOUT_MS;
        if (swimIndirectProbeCount < 0) swimIndirectProbeCount = DEFAULT_SWIM_INDIRECT_PROBE_COUNT;
        if (swimSuspicionTimeoutMs <= 0) swimSuspicionTimeoutMs = DEFAULT_SWIM_SUSPICION_TIMEOUT_MS;
        if (swimGossipFanout <= 0) swimGossipFanout = DEFAULT_SWIM_GOSSIP_FANOUT;
        if (swimDeadMemberTimeoutMs <= 0) swimDeadMemberTimeoutMs = DEFAULT_SWIM_DEAD_MEMBER_TIMEOUT_MS;
        if (advertisedHost == null) advertisedHost = DEFAULT_ADVERTISED_HOST;
        if (clusterPacketMaxSize <= 0) clusterPacketMaxSize = DEFAULT_CLUSTER_PACKET_MAX_SIZE;
        // Coerce a non-positive batch cap to the default so a partially-specified tuning (e.g. a
        // legacy short constructor that leaves the field at its sentinel 0) is always usable. A
        // caller wanting the legacy per-frame flush passes batchMaxFrames=1 explicitly.
        if (batchMaxFrames <= 0) batchMaxFrames = DEFAULT_BATCH_MAX_FRAMES;
        if (batchMaxDelayMicros < 0) batchMaxDelayMicros = DEFAULT_BATCH_MAX_DELAY_MICROS;
    }

    /**
     * Backwards-compatible 6-argument constructor (pre-Phase-D). Keeps every existing caller — in
     * particular the Phase C {@code AtLeastOnceTest} tunings — source-compatible by applying the
     * Phase D interest defaults and the Phase E SWIM defaults.
     */
    public ClusterTuning(
            int replayCapacity,
            ReplayBuffer.BackpressurePolicy backpressurePolicy,
            long replayOfferTimeoutMs,
            long ackIntervalMs,
            boolean strictOrdering,
            long originExpiryMs
    ) {
        this(replayCapacity, backpressurePolicy, replayOfferTimeoutMs, ackIntervalMs,
                strictOrdering, originExpiryMs,
                DEFAULT_INTEREST_SYNC_INTERVAL_MS, DEFAULT_INTEREST_BROADCAST_GRACE_MS);
    }

    /**
     * Backwards-compatible 8-argument constructor (pre-Phase-E). Applies the Phase E SWIM defaults so
     * any caller that already pinned the Phase C/D knobs keeps working.
     */
    public ClusterTuning(
            int replayCapacity,
            ReplayBuffer.BackpressurePolicy backpressurePolicy,
            long replayOfferTimeoutMs,
            long ackIntervalMs,
            boolean strictOrdering,
            long originExpiryMs,
            long interestSyncIntervalMs,
            long interestBroadcastGraceMs
    ) {
        this(replayCapacity, backpressurePolicy, replayOfferTimeoutMs, ackIntervalMs,
                strictOrdering, originExpiryMs, interestSyncIntervalMs, interestBroadcastGraceMs,
                DEFAULT_SWIM_PROBE_INTERVAL_MS, DEFAULT_SWIM_PROBE_TIMEOUT_MS,
                DEFAULT_SWIM_INDIRECT_PROBE_COUNT, DEFAULT_SWIM_SUSPICION_TIMEOUT_MS,
                DEFAULT_SWIM_GOSSIP_FANOUT, DEFAULT_SWIM_DEAD_MEMBER_TIMEOUT_MS,
                DEFAULT_ADVERTISED_HOST, DEFAULT_CLUSTER_PACKET_MAX_SIZE,
                DEFAULT_BATCH_MAX_FRAMES, DEFAULT_BATCH_MAX_DELAY_MICROS);
    }

    /**
     * Backwards-compatible 16-argument constructor (pre-write-batching). Applies the write-batching
     * defaults so every caller that already pinned the Phase C/D/E knobs through to the cluster
     * packet size — notably the cluster integration/chaos tests — stays source-compatible.
     */
    public ClusterTuning(
            int replayCapacity,
            ReplayBuffer.BackpressurePolicy backpressurePolicy,
            long replayOfferTimeoutMs,
            long ackIntervalMs,
            boolean strictOrdering,
            long originExpiryMs,
            long interestSyncIntervalMs,
            long interestBroadcastGraceMs,
            long swimProbeIntervalMs,
            long swimProbeTimeoutMs,
            int swimIndirectProbeCount,
            long swimSuspicionTimeoutMs,
            int swimGossipFanout,
            long swimDeadMemberTimeoutMs,
            String advertisedHost,
            int clusterPacketMaxSize
    ) {
        this(replayCapacity, backpressurePolicy, replayOfferTimeoutMs, ackIntervalMs,
                strictOrdering, originExpiryMs, interestSyncIntervalMs, interestBroadcastGraceMs,
                swimProbeIntervalMs, swimProbeTimeoutMs, swimIndirectProbeCount,
                swimSuspicionTimeoutMs, swimGossipFanout, swimDeadMemberTimeoutMs,
                advertisedHost, clusterPacketMaxSize,
                DEFAULT_BATCH_MAX_FRAMES, DEFAULT_BATCH_MAX_DELAY_MICROS);
    }

    /** Production-safe defaults used by every non-file constructor of {@link AvenueConfig}. */
    public static ClusterTuning defaults() {
        return new ClusterTuning(
                DEFAULT_REPLAY_CAPACITY,
                ReplayBuffer.BackpressurePolicy.BLOCK,
                DEFAULT_REPLAY_OFFER_TIMEOUT_MS,
                DEFAULT_ACK_INTERVAL_MS,
                DEFAULT_STRICT_ORDERING,
                DEFAULT_ORIGIN_EXPIRY_MS,
                DEFAULT_INTEREST_SYNC_INTERVAL_MS,
                DEFAULT_INTEREST_BROADCAST_GRACE_MS);
    }
}
