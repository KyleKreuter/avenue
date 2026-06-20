package de.kyle.avenue.cluster;

import de.kyle.avenue.metrics.ClusterMetrics;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cluster-wide interest routing table (Phase D): which <em>remote</em> nodes are interested in
 * which topics, so {@link ClusterManager#forward} only fans a publish out to nodes that actually
 * have a subscriber for the topic instead of broadcasting to every peer.
 * <p>
 * Three maps are kept in lock-step:
 * <ul>
 *   <li>{@link #topicToNodes} — {@code topic -> set of remote node ids} interested in it. This is
 *       the forward index read on the hot publish path via {@link #peersFor(String)}.</li>
 *   <li>{@link #nodeToTopics} — {@code remoteNodeId -> set of topics} it is interested in. This
 *       inverse index makes {@link #removeNode(String)} and {@link #replaceFullState} O(topics of
 *       that node) instead of O(all topics).</li>
 *   <li>{@link #lastVersion} — {@code remoteNodeId -> highest interest version seen}, used to drop
 *       stale/out-of-order interest updates idempotently (a delta or full-sync older than what we
 *       already applied is ignored).</li>
 * </ul>
 * <h2>Matching semantics</h2>
 * Matching is <b>exact</b> on the normalized topic key. There is intentionally <b>NO wildcard /
 * prefix matching</b>: {@link #peersFor(String)} returns exactly the nodes that registered interest
 * for that precise topic string. Wildcard interest, if ever needed, would be a separate feature.
 * <p>
 * Thread-safety: backed entirely by {@link ConcurrentHashMap}s and concurrent key-sets. The hot
 * read path ({@link #peersFor}) is lock-free. Mutations ({@link #applyDelta},
 * {@link #replaceFullState}, {@link #removeNode}) synchronize per remote node id so the version
 * check and the index updates for one node are atomic relative to each other, while different nodes
 * never contend.
 */
public final class InterestRoutingTable {

    /** Forward index: topic -> remote node ids interested in it. Read on the hot publish path. */
    private final ConcurrentHashMap<String, Set<String>> topicToNodes = new ConcurrentHashMap<>();

    /** Inverse index: remote node id -> topics it is interested in (for fast remove/replace). */
    private final ConcurrentHashMap<String, Set<String>> nodeToTopics = new ConcurrentHashMap<>();

    /** Highest interest version applied per remote node id (idempotency / out-of-order guard). */
    private final ConcurrentHashMap<String, Long> lastVersion = new ConcurrentHashMap<>();

    /** Optional metrics sink; the topic-count gauge is refreshed after every mutation. */
    private final ClusterMetrics metrics;

    /** Per-node mutation monitors so version-check + index update for one node is atomic. */
    private final ConcurrentHashMap<String, Object> nodeLocks = new ConcurrentHashMap<>();

    /** Interest delta operation. */
    public enum Op { ADD, REMOVE }

    public InterestRoutingTable() {
        this(null);
    }

    public InterestRoutingTable(ClusterMetrics metrics) {
        this.metrics = metrics;
    }

    private Object lockFor(String nodeId) {
        return nodeLocks.computeIfAbsent(nodeId, k -> new Object());
    }

    /**
     * Returns the set of remote node ids currently interested in {@code topic} (exact match on the
     * already-normalized key; no wildcards). The returned set is the live backing set — callers
     * must only iterate it, never mutate it. Returns an empty set when nobody is interested.
     */
    public Set<String> peersFor(String topic) {
        Set<String> nodes = topicToNodes.get(topic);
        return nodes == null ? Set.of() : nodes;
    }

    /**
     * Applies a single ADD/REMOVE interest delta from {@code nodeId}, stamped with {@code version}.
     * Deltas with {@code version <= } the last version applied for that node are ignored, which makes
     * replays and out-of-order arrivals harmless. Note: a full-state sync and a delta share the same
     * monotonic version space per node, so a newer full sync always wins over an older delta.
     *
     * @param nodeId  the remote node whose interest changed
     * @param version the sender's monotonic interest version for this update
     * @param op      ADD (node now interested) or REMOVE (node no longer interested)
     * @param topic   the normalized topic key
     */
    public void applyDelta(String nodeId, long version, Op op, String topic) {
        synchronized (lockFor(nodeId)) {
            Long seen = lastVersion.get(nodeId);
            if (seen != null && version <= seen) {
                return; // stale or duplicate — ignore for idempotency
            }
            lastVersion.put(nodeId, version);
            if (op == Op.ADD) {
                addLocked(nodeId, topic);
            } else {
                removeLocked(nodeId, topic);
            }
        }
        refreshGauge();
    }

    /**
     * Replaces the <em>entire</em> interest set of {@code nodeId} with {@code topics} (anti-entropy
     * full-state sync). Applied only when {@code version} is newer than the last version seen for the
     * node, so an out-of-order sync never regresses fresher delta state. Topics the node dropped are
     * removed from the forward index; newly-added ones are inserted.
     *
     * @param nodeId  the remote node whose full interest set is being replaced
     * @param version the sender's monotonic interest version at snapshot time
     * @param topics  the complete set of normalized topics the node is now interested in
     */
    public void replaceFullState(String nodeId, long version, Set<String> topics) {
        synchronized (lockFor(nodeId)) {
            Long seen = lastVersion.get(nodeId);
            if (seen != null && version <= seen) {
                return; // an equal or newer state was already applied
            }
            lastVersion.put(nodeId, version);

            Set<String> previous = nodeToTopics.get(nodeId);
            if (previous != null) {
                // Remove the node from every topic it no longer cares about.
                for (String old : previous) {
                    if (!topics.contains(old)) {
                        removeFromForwardIndex(nodeId, old);
                    }
                }
            }
            // Install the new inverse-index set and add the node to every (new) topic.
            Set<String> fresh = ConcurrentHashMap.newKeySet();
            fresh.addAll(topics);
            if (fresh.isEmpty()) {
                nodeToTopics.remove(nodeId);
            } else {
                nodeToTopics.put(nodeId, fresh);
            }
            for (String t : topics) {
                topicToNodes.computeIfAbsent(t, k -> ConcurrentHashMap.newKeySet()).add(nodeId);
            }
        }
        refreshGauge();
    }

    /**
     * Removes all interest of {@code nodeId} (used when a peer link dies / the node leaves), and
     * forgets its version watermark so a future reconnect of the same node id starts clean.
     */
    public void removeNode(String nodeId) {
        synchronized (lockFor(nodeId)) {
            Set<String> topics = nodeToTopics.remove(nodeId);
            if (topics != null) {
                for (String t : topics) {
                    removeFromForwardIndex(nodeId, t);
                }
            }
            lastVersion.remove(nodeId);
        }
        nodeLocks.remove(nodeId);
        refreshGauge();
    }

    /** Number of topics that currently have at least one interested remote node (gauge source). */
    public int topicCount() {
        return topicToNodes.size();
    }

    // -------------------------------------------------------------------------
    // Internals (caller holds the per-node monitor)
    // -------------------------------------------------------------------------

    private void addLocked(String nodeId, String topic) {
        nodeToTopics.computeIfAbsent(nodeId, k -> ConcurrentHashMap.newKeySet()).add(topic);
        topicToNodes.computeIfAbsent(topic, k -> ConcurrentHashMap.newKeySet()).add(nodeId);
    }

    private void removeLocked(String nodeId, String topic) {
        Set<String> topics = nodeToTopics.get(nodeId);
        if (topics != null) {
            topics.remove(topic);
            if (topics.isEmpty()) {
                nodeToTopics.remove(nodeId, topics);
            }
        }
        removeFromForwardIndex(nodeId, topic);
    }

    /** Drops {@code nodeId} from the forward index for {@code topic}, pruning an empty topic entry. */
    private void removeFromForwardIndex(String nodeId, String topic) {
        topicToNodes.computeIfPresent(topic, (k, nodes) -> {
            nodes.remove(nodeId);
            return nodes.isEmpty() ? null : nodes;
        });
    }

    private void refreshGauge() {
        if (metrics != null) {
            metrics.setRoutingTableTopicCount(topicCount());
        }
    }
}
