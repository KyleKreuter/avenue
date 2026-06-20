package de.kyle.avenue.metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Lightweight, dependency-free metrics registry for the cluster transport, mirroring the style of
 * {@link AvenueMetrics}: plain {@link AtomicLong} counters/gauges, no external monitoring
 * dependency, a public getter per value.
 *
 * <p>Counters are monotonic; {@link #activePeerLinks} is a gauge adjusted in place as peer links
 * come and go. Values are read by the shared {@link AvenueMetrics} snapshot log (see
 * {@link AvenueMetrics#setClusterMetrics}) and by tests.
 */
public final class ClusterMetrics {

    // Counters (monotonic).
    private final AtomicLong messagesForwarded = new AtomicLong();
    private final AtomicLong messagesReceived = new AtomicLong();
    private final AtomicLong messagesDeduped = new AtomicLong();
    private final AtomicLong messagesDropped = new AtomicLong();
    private final AtomicLong handshakeAuthFailures = new AtomicLong();

    // Phase C — at-least-once delivery counters.
    private final AtomicLong clusterBackfillMessages = new AtomicLong();
    private final AtomicLong clusterGapEvents = new AtomicLong();
    private final AtomicLong clusterSlowPeerStalls = new AtomicLong();
    private final AtomicLong acksSent = new AtomicLong();
    private final AtomicLong acksReceived = new AtomicLong();

    // Gauge (current value).
    private final AtomicLong activePeerLinks = new AtomicLong();
    /** Current total depth of all per-target replay buffers (sum of un-acked entries). */
    private final AtomicLong replayBufferDepth = new AtomicLong();

    // ------------------------------------------------------------------
    // Counter mutations
    // ------------------------------------------------------------------

    /** Records that one cluster publish frame was enqueued towards a peer link. */
    public void incrementMessagesForwarded() {
        messagesForwarded.incrementAndGet();
    }

    /** Records that one cluster publish frame was received from a peer and accepted for delivery. */
    public void incrementMessagesReceived() {
        messagesReceived.incrementAndGet();
    }

    /** Records that one received cluster publish frame was dropped as a duplicate. */
    public void incrementMessagesDeduped() {
        messagesDeduped.incrementAndGet();
    }

    /** Records that one outbound cluster frame was dropped due to a full peer queue. */
    public void incrementMessagesDropped() {
        messagesDropped.incrementAndGet();
    }

    /**
     * Records that one cluster peer handshake was rejected because the HMAC proof did not verify
     * (a peer that does not hold the shared {@code cluster.secret}).
     */
    public void incrementHandshakeAuthFailures() {
        handshakeAuthFailures.incrementAndGet();
    }

    // ------------------------------------------------------------------
    // Gauge mutations
    // ------------------------------------------------------------------

    /** Records that one buffered message was re-sent to a peer during reconnect backfill. */
    public void incrementClusterBackfillMessages() {
        clusterBackfillMessages.incrementAndGet();
    }

    /** Adds {@code n} to the backfill-messages counter (batch increment during a replay). */
    public void addClusterBackfillMessages(long n) {
        if (n > 0) {
            clusterBackfillMessages.addAndGet(n);
        }
    }

    /**
     * Records that a gap occurred: replay state for a requested range was unavailable, or an entry
     * was dropped under backpressure, forcing the receiver to resync forward past lost messages.
     */
    public void incrementClusterGapEvents() {
        clusterGapEvents.incrementAndGet();
    }

    /** Records that a writer stalled because the replay buffer was full (slow peer backpressure). */
    public void incrementClusterSlowPeerStalls() {
        clusterSlowPeerStalls.incrementAndGet();
    }

    /** Records that one cumulative ACK packet was sent towards a peer. */
    public void incrementAcksSent() {
        acksSent.incrementAndGet();
    }

    /** Records that one cumulative ACK packet was received from a peer. */
    public void incrementAcksReceived() {
        acksReceived.incrementAndGet();
    }

    /**
     * Adjusts the aggregate replay-buffer-depth gauge by {@code delta} (positive when entries are
     * buffered, negative when they are acked/evicted). Each {@link de.kyle.avenue.cluster.ReplayBuffer}
     * reports its own size change so the gauge reflects the cluster-wide un-acked backlog.
     */
    public void addReplayBufferDepth(long delta) {
        replayBufferDepth.addAndGet(delta);
    }

    /** Increments the active-peer-link gauge (a link was added). */
    public void incrementActivePeerLinks() {
        activePeerLinks.incrementAndGet();
    }

    /** Decrements the active-peer-link gauge (a link was removed), never below zero. */
    public void decrementActivePeerLinks() {
        activePeerLinks.updateAndGet(current -> current > 0 ? current - 1 : 0);
    }

    // ------------------------------------------------------------------
    // Getters (for tests and the periodic reporter)
    // ------------------------------------------------------------------

    public long getMessagesForwarded() {
        return messagesForwarded.get();
    }

    public long getMessagesReceived() {
        return messagesReceived.get();
    }

    public long getMessagesDeduped() {
        return messagesDeduped.get();
    }

    public long getMessagesDropped() {
        return messagesDropped.get();
    }

    public long getActivePeerLinks() {
        return activePeerLinks.get();
    }

    public long getHandshakeAuthFailures() {
        return handshakeAuthFailures.get();
    }

    public long getClusterBackfillMessages() {
        return clusterBackfillMessages.get();
    }

    public long getClusterGapEvents() {
        return clusterGapEvents.get();
    }

    public long getClusterSlowPeerStalls() {
        return clusterSlowPeerStalls.get();
    }

    public long getAcksSent() {
        return acksSent.get();
    }

    public long getAcksReceived() {
        return acksReceived.get();
    }

    public long getReplayBufferDepth() {
        return replayBufferDepth.get();
    }
}
