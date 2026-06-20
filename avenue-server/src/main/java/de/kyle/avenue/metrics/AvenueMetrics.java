package de.kyle.avenue.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Lightweight, dependency-free metrics registry for the Avenue server (E20).
 *
 * <p>All counters and gauges are {@link AtomicLong}s so they can be updated from any of the
 * many virtual threads handling connections, deliveries and cluster I/O without locking.
 * Counters only ever increase; gauges (e.g. {@link #activeConnections}, {@link #subscriptionCount},
 * {@link #maxOutboundQueueDepth}) are set/adjusted in place.
 *
 * <p>A single daemon scheduler periodically logs a snapshot so operators get visibility without
 * any external monitoring stack. Every value also has a public getter so tests can assert on
 * them directly. Intentionally no Micrometer / Prometheus dependency is introduced.
 */
public final class AvenueMetrics {

    private static final Logger log = LoggerFactory.getLogger(AvenueMetrics.class);

    // Counters (monotonic).
    private final AtomicLong messagesPublished = new AtomicLong();
    private final AtomicLong messagesDelivered = new AtomicLong();
    private final AtomicLong droppedMessages = new AtomicLong();
    private final AtomicLong slowConsumerDisconnects = new AtomicLong();
    private final AtomicLong totalConnectionsAccepted = new AtomicLong();
    private final AtomicLong connectionsRejected = new AtomicLong();

    // Gauges (current value).
    private final AtomicLong activeConnections = new AtomicLong();
    private final AtomicLong subscriptionCount = new AtomicLong();
    private final AtomicLong maxOutboundQueueDepth = new AtomicLong();

    private volatile ScheduledExecutorService reporter;

    /** Optional cluster metrics; folded into the periodic snapshot log when present. */
    private volatile ClusterMetrics clusterMetrics;

    // ------------------------------------------------------------------
    // Counter mutations
    // ------------------------------------------------------------------

    /** Records that a local client publish was accepted and fanned out. */
    public void incrementMessagesPublished() {
        messagesPublished.incrementAndGet();
    }

    /** Records that a single outbound packet was enqueued for delivery to a subscriber. */
    public void incrementMessagesDelivered() {
        messagesDelivered.incrementAndGet();
    }

    /** Records that a message was dropped due to a full outbound queue (DROP_MESSAGE policy). */
    public void incrementDroppedMessages() {
        droppedMessages.incrementAndGet();
    }

    /** Records that a slow consumer was disconnected (DISCONNECT_SLOW_CONSUMER policy). */
    public void incrementSlowConsumerDisconnects() {
        slowConsumerDisconnects.incrementAndGet();
    }

    /** Records and returns the new count of accepted connections. */
    public long incrementTotalConnectionsAccepted() {
        return totalConnectionsAccepted.incrementAndGet();
    }

    /** Records that an inbound connection was rejected (e.g. max-connections limit reached). */
    public void incrementConnectionsRejected() {
        connectionsRejected.incrementAndGet();
    }

    // ------------------------------------------------------------------
    // Gauge mutations
    // ------------------------------------------------------------------

    /** Increments the active-connection gauge and returns the new value. */
    public long incrementActiveConnections() {
        return activeConnections.incrementAndGet();
    }

    /** Decrements the active-connection gauge (never below zero). */
    public void decrementActiveConnections() {
        activeConnections.updateAndGet(current -> current > 0 ? current - 1 : 0);
    }

    /** Sets the current total subscription count (gauge). */
    public void setSubscriptionCount(long value) {
        subscriptionCount.set(value);
    }

    /**
     * Records an observed per-connection outbound queue depth, keeping the running maximum.
     * Gives operators a cheap aggregate view of buffering pressure without per-connection state.
     */
    public void recordOutboundQueueDepth(int depth) {
        maxOutboundQueueDepth.accumulateAndGet(depth, Math::max);
    }

    // ------------------------------------------------------------------
    // Getters (for tests and the periodic reporter)
    // ------------------------------------------------------------------

    public long getMessagesPublished() {
        return messagesPublished.get();
    }

    public long getMessagesDelivered() {
        return messagesDelivered.get();
    }

    public long getDroppedMessages() {
        return droppedMessages.get();
    }

    public long getSlowConsumerDisconnects() {
        return slowConsumerDisconnects.get();
    }

    public long getTotalConnectionsAccepted() {
        return totalConnectionsAccepted.get();
    }

    public long getConnectionsRejected() {
        return connectionsRejected.get();
    }

    public long getActiveConnections() {
        return activeConnections.get();
    }

    public long getSubscriptionCount() {
        return subscriptionCount.get();
    }

    public long getMaxOutboundQueueDepth() {
        return maxOutboundQueueDepth.get();
    }

    /**
     * Attaches a {@link ClusterMetrics} instance so the periodic snapshot log includes cluster
     * counters. Optional and null-safe: when clustering is disabled this is never called and the
     * snapshot simply omits the cluster line.
     */
    public void setClusterMetrics(ClusterMetrics clusterMetrics) {
        this.clusterMetrics = clusterMetrics;
    }

    /** Returns the attached cluster metrics, or {@code null} if clustering is disabled. */
    public ClusterMetrics getClusterMetrics() {
        return clusterMetrics;
    }

    // ------------------------------------------------------------------
    // Periodic reporting
    // ------------------------------------------------------------------

    /**
     * Starts a single daemon thread that logs a metrics snapshot at a fixed interval. A
     * non-positive interval disables periodic logging entirely (the getters still work).
     *
     * @param intervalSeconds the reporting interval; {@code <= 0} disables periodic logging
     */
    public synchronized void startReporting(long intervalSeconds) {
        if (intervalSeconds <= 0 || reporter != null) {
            return;
        }
        ThreadFactory factory = runnable -> {
            Thread thread = new Thread(runnable, "avenue-metrics-reporter");
            thread.setDaemon(true);
            return thread;
        };
        reporter = Executors.newSingleThreadScheduledExecutor(factory);
        reporter.scheduleAtFixedRate(this::logSnapshot, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    /** Stops the periodic reporter if it is running. Safe to call multiple times. */
    public synchronized void stopReporting() {
        if (reporter != null) {
            reporter.shutdownNow();
            reporter = null;
        }
    }

    private void logSnapshot() {
        log.info("Avenue metrics | activeConnections={} totalAccepted={} rejected={} "
                        + "published={} delivered={} dropped={} slowConsumerDisconnects={} "
                        + "subscriptions={} maxOutboundQueueDepth={}",
                getActiveConnections(), getTotalConnectionsAccepted(), getConnectionsRejected(),
                getMessagesPublished(), getMessagesDelivered(), getDroppedMessages(),
                getSlowConsumerDisconnects(), getSubscriptionCount(), getMaxOutboundQueueDepth());

        ClusterMetrics cluster = this.clusterMetrics;
        if (cluster != null) {
            log.info("Avenue cluster metrics | activePeerLinks={} forwarded={} received={} "
                            + "deduped={} dropped={} | backfill={} gaps={} slowPeerStalls={} "
                            + "acksSent={} acksReceived={} replayDepth={}",
                    cluster.getActivePeerLinks(), cluster.getMessagesForwarded(),
                    cluster.getMessagesReceived(), cluster.getMessagesDeduped(),
                    cluster.getMessagesDropped(),
                    cluster.getClusterBackfillMessages(), cluster.getClusterGapEvents(),
                    cluster.getClusterSlowPeerStalls(), cluster.getAcksSent(),
                    cluster.getAcksReceived(), cluster.getReplayBufferDepth());
        }
    }
}
