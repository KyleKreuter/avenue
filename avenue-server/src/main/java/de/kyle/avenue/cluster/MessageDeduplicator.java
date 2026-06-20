package de.kyle.avenue.cluster;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Bounded LRU set of seen {@code messageId} strings used to deduplicate
 * {@link de.kyle.avenue.packet.cluster.ClusterPublishPacket} deliveries.
 * <p>
 * In a correctly configured full-mesh each message arrives exactly once via the single-hop
 * rule. This deduplicator acts as a safety net against misconfiguration (e.g., a peer listed
 * twice, or a loop caused by a non-full-mesh setup). It is bounded to {@code maxSize} entries;
 * the oldest entries are evicted when the limit is reached.
 * <p>
 * Thread-safe via {@code synchronized} on all access — the seen-set is written and read on
 * the virtual-thread reader of each peer link, and the capacity is small enough that lock
 * contention is negligible.
 */
public class MessageDeduplicator {

    private static final int DEFAULT_MAX_SIZE = 10_000;

    private final LinkedHashMap<String, Boolean> seen;

    public MessageDeduplicator() {
        this(DEFAULT_MAX_SIZE);
    }

    public MessageDeduplicator(int maxSize) {
        // Access-order LRU map: eldest entry is removed when the capacity is exceeded.
        this.seen = new LinkedHashMap<>(maxSize, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > maxSize;
            }
        };
    }

    /**
     * Records a {@code messageId} and reports whether it was already seen.
     *
     * @param messageId the UUID string of the cluster message
     * @return {@code true} if this ID was seen before (duplicate), {@code false} if it is new
     */
    public synchronized boolean isDuplicate(String messageId) {
        if (seen.containsKey(messageId)) {
            return true;
        }
        seen.put(messageId, Boolean.TRUE);
        return false;
    }
}
