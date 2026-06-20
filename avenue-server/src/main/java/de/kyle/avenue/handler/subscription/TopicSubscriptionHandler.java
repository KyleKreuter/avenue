package de.kyle.avenue.handler.subscription;

import de.kyle.avenue.handler.client.ClientConnection;
import de.kyle.avenue.handler.client.ClientConnectionHandler;
import de.kyle.avenue.metrics.AvenueMetrics;
import de.kyle.avenue.proto.ClientEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks which clients are subscribed to which topics and fans packets out to them.
 * <p>
 * Concurrency model: the outer map is a {@link ConcurrentHashMap}; each topic's subscriber
 * set is a {@link ConcurrentHashMap#newKeySet() concurrent set}. Delivery iterates the set
 * while other threads may concurrently subscribe/unsubscribe without throwing
 * {@code ConcurrentModificationException} and without lost updates. Topic keys are
 * normalized in exactly one place ({@link #normalize(String)}) so subscribe, deliver and
 * unsubscribe always agree on the key.
 */
public class TopicSubscriptionHandler {
    private static final Logger log = LoggerFactory.getLogger(TopicSubscriptionHandler.class);

    /**
     * Phase D interest hook. The cluster layer registers a listener so it learns the exact moments a
     * topic gains its <em>first</em> local subscriber ({@link #onInterestAdded}) or loses its
     * <em>last</em> one ({@link #onInterestRemoved}) on this node, which is precisely when the node's
     * interest in that topic toggles and must be propagated to peers.
     * <p>
     * The default {@link #NOOP} keeps single-node operation completely unaffected: with no cluster
     * wired in, the transitions are detected but nothing happens.
     */
    public interface InterestListener {
        /** Called after a topic gained its first local subscriber (node is now interested). */
        void onInterestAdded(String topic);

        /** Called after a topic lost its last local subscriber (node is no longer interested). */
        void onInterestRemoved(String topic);

        InterestListener NOOP = new InterestListener() {
            @Override
            public void onInterestAdded(String topic) {
            }

            @Override
            public void onInterestRemoved(String topic) {
            }
        };
    }

    /** Defaults to NOOP so the single-node path is untouched until the cluster wires a listener in. */
    private volatile InterestListener interestListener = InterestListener.NOOP;

    /** Wires the cluster interest listener. Idempotent; {@code null} resets to {@link InterestListener#NOOP}. */
    public void setInterestListener(InterestListener listener) {
        this.interestListener = listener != null ? listener : InterestListener.NOOP;
    }

    private final Map<String, Set<ClientConnection>> topicSubscriptions = new ConcurrentHashMap<>();

    /** Running total of active (client, topic) subscriptions, mirrored into the metrics gauge. */
    private final AtomicLong subscriptionTotal = new AtomicLong();

    /**
     * Optional metrics registry. Defaults to a standalone instance so the handler works without
     * external wiring (and existing tests are unaffected); the owning server replaces it with
     * its shared registry via {@link #setMetrics(AvenueMetrics)}.
     */
    private volatile AvenueMetrics metrics = new AvenueMetrics();

    /** Injects the shared metrics registry so the subscription-count gauge is reported centrally. */
    public void setMetrics(AvenueMetrics metrics) {
        if (metrics != null) {
            this.metrics = metrics;
            this.metrics.setSubscriptionCount(subscriptionTotal.get());
        }
    }

    /**
     * Single source of truth for topic-key normalization. Must be used by every method that
     * reads or writes {@link #topicSubscriptions} so that keys never diverge.
     * <p>
     * Exposed publicly so that callers (e.g. the subscribe acknowledgment) can echo back the
     * exact normalized key the subscription was registered under, keeping client and server
     * in agreement on the topic name.
     */
    public String normalize(String topic) {
        return topic.toLowerCase(Locale.ROOT).strip();
    }

    /**
     * Fans a {@link ClientEnvelope} out to every subscriber of {@code topic}.
     * <p>
     * Encode-once fan-out: the envelope is serialized to its bare protobuf payload bytes
     * <em>exactly once</em> here, and the same immutable {@code byte[]} is handed to every
     * subscriber via {@link ClientConnectionHandler#enqueuePreSerialized(byte[])}. This turns the
     * per-publish serialization cost from O(N subscribers) into O(1).
     *
     * @param topic    the topic the message was published on (normalized internally)
     * @param envelope the outbound envelope to serialize once and deliver to all subscribers
     * @param maxSize  the configured maximum payload size enforced during serialization
     */
    public void deliverPacketToSubscribers(String topic, ClientEnvelope envelope, int maxSize) {
        deliverToSubscribers(normalize(topic), envelope, maxSize);
    }

    /**
     * Variant for callers that have already normalized the topic key (the local publish hot path),
     * so the {@code toLowerCase + strip} allocation is not repeated. Behaviour is otherwise identical
     * to {@link #deliverPacketToSubscribers(String, ClientEnvelope, int)}.
     *
     * @param normalizedTopic the already-{@link #normalize(String) normalized} topic key
     * @param envelope        the outbound envelope to serialize once and deliver to all subscribers
     * @param maxSize         the configured maximum payload size enforced during serialization
     */
    /**
     * Current number of subscribers for an already-{@link #normalize(String) normalized} topic key.
     * Used by the publish hot path to choose inline vs. executor-based fan-out without re-normalizing
     * or re-looking-up inside delivery. {@code 0} when the topic has no subscribers.
     */
    public int subscriberCount(String normalizedTopic) {
        Set<ClientConnection> subscribers = topicSubscriptions.get(normalizedTopic);
        return subscribers == null ? 0 : subscribers.size();
    }

    public void deliverToSubscribers(String normalizedTopic, ClientEnvelope envelope, int maxSize) {
        Set<ClientConnection> subscribers = topicSubscriptions.get(normalizedTopic);
        if (subscribers == null || subscribers.isEmpty()) {
            log.warn("Packet was not delivered to other clients because no subscriptions are registered");
            return;
        }
        // Encode-once: serialize the envelope a single time and share the immutable bytes with every
        // subscriber instead of re-serializing per writer.
        byte[] payload = ClientConnectionHandler.encodeForFanOut(envelope, maxSize);
        fanOut(subscribers, payload);
    }

    /**
     * Fan-out variant for the publish hot path that takes the <em>already-serialized</em> bare
     * payload bytes of the {@code PublishOutbound} {@link ClientEnvelope} directly, so the caller can
     * produce them via {@link de.kyle.avenue.serialization.OutboundEncoder} without ever allocating a
     * {@code ClientEnvelope}/{@code PublishOutbound} builder or message object.
     * <p>
     * Behaviour is otherwise identical to {@link #deliverToSubscribers(String, ClientEnvelope, int)}:
     * the same immutable {@code byte[]} is shared with every subscriber (encode-once fan-out). The
     * size guard is enforced by the caller (the encoder produces exactly-sized bytes), so it is not
     * re-checked here.
     *
     * @param normalizedTopic the already-{@link #normalize(String) normalized} topic key
     * @param payload         the bare protobuf payload bytes of the outbound envelope (no length
     *                        prefix); must never be mutated after this call as it is shared
     */
    public void deliverPreSerializedToSubscribers(String normalizedTopic, byte[] payload) {
        Set<ClientConnection> subscribers = topicSubscriptions.get(normalizedTopic);
        if (subscribers == null || subscribers.isEmpty()) {
            log.warn("Packet was not delivered to other clients because no subscriptions are registered");
            return;
        }
        fanOut(subscribers, payload);
    }

    /**
     * Shared fan-out tail: hand the same immutable bytes to every subscriber. The concurrent set
     * tolerates concurrent subscribe/unsubscribe during iteration; delivery is non-blocking as each
     * handler only enqueues onto its own outbound queue.
     */
    private static void fanOut(Set<ClientConnection> subscribers, byte[] payload) {
        for (ClientConnection clientConnection : subscribers) {
            clientConnection.enqueuePreSerialized(payload);
        }
    }

    public void subscribeToTopic(String topic, ClientConnection clientConnection) {
        String normalizedTopic = normalize(topic);
        // Detect the "first subscription for this topic" transition ATOMICALLY: the flag is set only
        // when the computeIfAbsent mapping function actually runs, i.e. the subscriber set did not
        // exist yet. This cannot false-positive on a concurrent subscribe to the same topic, since
        // computeIfAbsent runs the function at most once per absent key.
        boolean[] firstForTopic = {false};
        boolean added = topicSubscriptions
                .computeIfAbsent(normalizedTopic, key -> {
                    firstForTopic[0] = true;
                    return ConcurrentHashMap.newKeySet();
                })
                .add(clientConnection);
        if (added) {
            metrics.setSubscriptionCount(subscriptionTotal.incrementAndGet());
        }
        // Fire the interest-added event AFTER the map mutation (outside the computeIfAbsent lambda),
        // so the listener never runs while holding the map's internal bin lock.
        if (firstForTopic[0]) {
            interestListener.onInterestAdded(normalizedTopic);
        }
    }

    public void unsubscribeFromAllTopics(ClientConnection clientConnection) {
        // Remove the client from every topic and drop now-empty topic sets to avoid leaking
        // memory for topics that no longer have any subscribers.
        long removed = 0;
        // Collect the topics whose LAST subscriber we just removed, then fire interest-removed for
        // each AFTER the loop (outside any computeIfPresent lambda). Deferring the events keeps the
        // listener off the map's internal bin lock and avoids re-entrancy surprises.
        java.util.List<String> nowEmptyTopics = new java.util.ArrayList<>();
        for (Map.Entry<String, Set<ClientConnection>> entry : topicSubscriptions.entrySet()) {
            String topic = entry.getKey();
            if (entry.getValue().remove(clientConnection)) {
                removed++;
                // computeIfPresent re-checks emptiness atomically against concurrent inserts and
                // returns null exactly when the topic set became empty and was therefore removed.
                // A null return == "this client removed the last subscription for this topic", which
                // is precisely the interest-removed transition; a non-null return means another
                // subscriber raced in, so interest is retained and no event must fire.
                Set<ClientConnection> remaining =
                        topicSubscriptions.computeIfPresent(topic, (key, current) -> current.isEmpty() ? null : current);
                if (remaining == null) {
                    nowEmptyTopics.add(topic);
                }
            }
        }
        if (removed > 0) {
            metrics.setSubscriptionCount(subscriptionTotal.addAndGet(-removed));
        }
        for (String topic : nowEmptyTopics) {
            interestListener.onInterestRemoved(topic);
        }
    }
}
