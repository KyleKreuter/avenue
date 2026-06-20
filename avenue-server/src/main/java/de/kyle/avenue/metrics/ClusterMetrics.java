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

    // Gauge (current value).
    private final AtomicLong activePeerLinks = new AtomicLong();

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
}
