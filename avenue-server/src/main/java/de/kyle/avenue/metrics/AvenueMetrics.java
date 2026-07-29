package de.kyle.avenue.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

/**
 * Lightweight, dependency-free metrics registry for the Avenue server (E20).
 *
 * <p>All counters and gauges are lock-free so they can be updated from any of the many virtual
 * threads handling connections, deliveries and cluster I/O. Counters only ever increase; gauges
 * (e.g. {@link #activeConnections}, {@link #subscriptionCount}, {@link #maxOutboundQueueDepth})
 * are set/adjusted in place.
 *
 * <h2>Why the hot-path counters are striped</h2>
 * {@link #incrementMessagesDelivered()} and {@link #recordOutboundQueueDepth(int)} are called
 * <em>once per subscriber per message</em> — on an N-way fan-out that is 2N updates per publish,
 * from as many threads as the fan-out touches connections. Backing those two with a single
 * {@link AtomicLong} put every delivering thread onto one contended cache line: a plain CAS for the
 * counter, and a full CAS <em>retry loop</em> ({@code accumulateAndGet}) for the running maximum.
 * Both are now striped — a {@link LongAdder} and a {@link LongAccumulator} — so concurrent updaters
 * hit different cells and only the (rare) read aggregates them.
 *
 * <p>The trade-off is deliberate and harmless here: a striped read is not an atomic snapshot across
 * cells, so a getter called <em>during</em> concurrent updates may miss an in-flight increment. All
 * readers are the periodic reporter, the admin endpoint and tests that read after the traffic has
 * settled, none of which need linearizability. The remaining {@link AtomicLong}s are the
 * per-connection gauges: they change once per connect/disconnect (not per message), so they are
 * uncontended, and {@link #incrementActiveConnections()} needs its exact return value to enforce
 * the max-connections limit — something a {@link LongAdder} cannot provide.
 *
 * <p>A single daemon scheduler periodically logs a snapshot so operators get visibility without
 * any external monitoring stack. Every value also has a public getter so tests can assert on
 * them directly. Intentionally no Micrometer / Prometheus dependency is introduced.
 */
public final class AvenueMetrics {

    private static final Logger log = LoggerFactory.getLogger(AvenueMetrics.class);

    // Counters (monotonic). Striped: updated from many threads, read rarely.
    private final LongAdder messagesPublished = new LongAdder();
    private final LongAdder messagesDelivered = new LongAdder();
    private final LongAdder droppedMessages = new LongAdder();
    private final LongAdder slowConsumerDisconnects = new LongAdder();
    private final LongAdder totalConnectionsAccepted = new LongAdder();
    private final LongAdder connectionsRejected = new LongAdder();

    // Gauges (current value). AtomicLong on purpose: per-connection rate, and the active-connection
    // gauge must return its exact post-increment value for the max-connections check.
    private final AtomicLong activeConnections = new AtomicLong();
    private final AtomicLong subscriptionCount = new AtomicLong();

    /**
     * Running maximum observed outbound queue depth. Striped like the counters because it is
     * recorded on the per-subscriber delivery path; {@link LongAccumulator} gives the same
     * "keep the maximum" semantics as the previous {@code accumulateAndGet(depth, Math::max)}
     * without funnelling every delivering thread through one CAS retry loop.
     */
    private final LongAccumulator maxOutboundQueueDepth = new LongAccumulator(Math::max, 0L);

    private volatile ScheduledExecutorService reporter;

    /** Optional cluster metrics; folded into the periodic snapshot log when present. */
    private volatile ClusterMetrics clusterMetrics;

    // ------------------------------------------------------------------
    // Counter mutations
    // ------------------------------------------------------------------

    /** Records that a local client publish was accepted and fanned out. */
    public void incrementMessagesPublished() {
        messagesPublished.increment();
    }

    /** Records that a single outbound packet was enqueued for delivery to a subscriber. */
    public void incrementMessagesDelivered() {
        messagesDelivered.increment();
    }

    /** Records that a message was dropped due to a full outbound queue (DROP_MESSAGE policy). */
    public void incrementDroppedMessages() {
        droppedMessages.increment();
    }

    /** Records that a slow consumer was disconnected (DISCONNECT_SLOW_CONSUMER policy). */
    public void incrementSlowConsumerDisconnects() {
        slowConsumerDisconnects.increment();
    }

    /** Records an accepted connection. */
    public void incrementTotalConnectionsAccepted() {
        totalConnectionsAccepted.increment();
    }

    /** Records that an inbound connection was rejected (e.g. max-connections limit reached). */
    public void incrementConnectionsRejected() {
        connectionsRejected.increment();
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
        maxOutboundQueueDepth.accumulate(depth);
    }

    // ------------------------------------------------------------------
    // Getters (for tests and the periodic reporter)
    // ------------------------------------------------------------------

    public long getMessagesPublished() {
        return messagesPublished.sum();
    }

    public long getMessagesDelivered() {
        return messagesDelivered.sum();
    }

    public long getDroppedMessages() {
        return droppedMessages.sum();
    }

    public long getSlowConsumerDisconnects() {
        return slowConsumerDisconnects.sum();
    }

    public long getTotalConnectionsAccepted() {
        return totalConnectionsAccepted.sum();
    }

    public long getConnectionsRejected() {
        return connectionsRejected.sum();
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
                            + "acksSent={} acksReceived={} replayDepth={} | interestSent={} "
                            + "interestReceived={} routedSkipped={} routingTopics={}",
                    cluster.getActivePeerLinks(), cluster.getMessagesForwarded(),
                    cluster.getMessagesReceived(), cluster.getMessagesDeduped(),
                    cluster.getMessagesDropped(),
                    cluster.getClusterBackfillMessages(), cluster.getClusterGapEvents(),
                    cluster.getClusterSlowPeerStalls(), cluster.getAcksSent(),
                    cluster.getAcksReceived(), cluster.getReplayBufferDepth(),
                    cluster.getInterestUpdatesSent(), cluster.getInterestUpdatesReceived(),
                    cluster.getInterestRoutedSkipped(), cluster.getRoutingTableTopicCount());
        }
    }
}
