package de.kyle.avenue.config;

import de.kyle.avenue.cluster.ReplayBuffer;

/**
 * Bundle of cluster transport tuning knobs introduced with Phase C's at-least-once delivery.
 * <p>
 * These are gathered into a single record (instead of being added as yet more
 * {@link AvenueConfig} constructor parameters) so the existing 7-, 13- and 26-argument
 * {@code AvenueConfig} constructors — and every test that calls them — stay source-compatible: they
 * simply delegate to {@link #defaults()}. The file/{@code .env} constructor builds a populated
 * instance from the {@code cluster.replay.*}, {@code cluster.ack-interval-ms},
 * {@code cluster.strict-ordering} and {@code cluster.origin.expiry-ms} properties.
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
 */
public record ClusterTuning(
        int replayCapacity,
        ReplayBuffer.BackpressurePolicy backpressurePolicy,
        long replayOfferTimeoutMs,
        long ackIntervalMs,
        boolean strictOrdering,
        long originExpiryMs
) {

    public static final int DEFAULT_REPLAY_CAPACITY = 65_536;
    public static final long DEFAULT_REPLAY_OFFER_TIMEOUT_MS = 1_000L;
    public static final long DEFAULT_ACK_INTERVAL_MS = 200L;
    public static final boolean DEFAULT_STRICT_ORDERING = false;
    public static final long DEFAULT_ORIGIN_EXPIRY_MS = 300_000L;

    /** Production-safe defaults used by every non-file constructor of {@link AvenueConfig}. */
    public static ClusterTuning defaults() {
        return new ClusterTuning(
                DEFAULT_REPLAY_CAPACITY,
                ReplayBuffer.BackpressurePolicy.BLOCK,
                DEFAULT_REPLAY_OFFER_TIMEOUT_MS,
                DEFAULT_ACK_INTERVAL_MS,
                DEFAULT_STRICT_ORDERING,
                DEFAULT_ORIGIN_EXPIRY_MS);
    }
}
