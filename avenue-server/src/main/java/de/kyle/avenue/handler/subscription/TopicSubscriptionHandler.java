package de.kyle.avenue.handler.subscription;

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

    private final Map<String, Set<ClientConnectionHandler>> topicSubscriptions = new ConcurrentHashMap<>();

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

    public void deliverPacketToSubscribers(String topic, ClientEnvelope envelope) {
        Set<ClientConnectionHandler> subscribers = topicSubscriptions.get(normalize(topic));
        if (subscribers == null || subscribers.isEmpty()) {
            log.warn("Packet was not delivered to other clients because no subscriptions are registered");
            return;
        }
        // The concurrent set tolerates concurrent subscribe/unsubscribe during iteration.
        // Delivery is non-blocking: each handler only enqueues onto its own outbound queue.
        for (ClientConnectionHandler clientConnectionHandler : subscribers) {
            clientConnectionHandler.enqueue(envelope);
        }
    }

    public void subscribeToTopic(String topic, ClientConnectionHandler clientConnectionHandler) {
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
                .add(clientConnectionHandler);
        if (added) {
            metrics.setSubscriptionCount(subscriptionTotal.incrementAndGet());
        }
        // Fire the interest-added event AFTER the map mutation (outside the computeIfAbsent lambda),
        // so the listener never runs while holding the map's internal bin lock.
        if (firstForTopic[0]) {
            interestListener.onInterestAdded(normalizedTopic);
        }
    }

    public void unsubscribeFromAllTopics(ClientConnectionHandler clientConnectionHandler) {
        // Remove the client from every topic and drop now-empty topic sets to avoid leaking
        // memory for topics that no longer have any subscribers.
        long removed = 0;
        // Collect the topics whose LAST subscriber we just removed, then fire interest-removed for
        // each AFTER the loop (outside any computeIfPresent lambda). Deferring the events keeps the
        // listener off the map's internal bin lock and avoids re-entrancy surprises.
        java.util.List<String> nowEmptyTopics = new java.util.ArrayList<>();
        for (Map.Entry<String, Set<ClientConnectionHandler>> entry : topicSubscriptions.entrySet()) {
            String topic = entry.getKey();
            if (entry.getValue().remove(clientConnectionHandler)) {
                removed++;
                // computeIfPresent re-checks emptiness atomically against concurrent inserts and
                // returns null exactly when the topic set became empty and was therefore removed.
                // A null return == "this client removed the last subscription for this topic", which
                // is precisely the interest-removed transition; a non-null return means another
                // subscriber raced in, so interest is retained and no event must fire.
                Set<ClientConnectionHandler> remaining =
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
