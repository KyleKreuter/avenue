package de.kyle.avenue.handler.subscription;

import de.kyle.avenue.handler.client.ClientConnectionHandler;
import de.kyle.avenue.metrics.AvenueMetrics;
import de.kyle.avenue.packet.OutboundPacket;
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

    public void deliverPacketToSubscribers(String topic, OutboundPacket packet) {
        Set<ClientConnectionHandler> subscribers = topicSubscriptions.get(normalize(topic));
        if (subscribers == null || subscribers.isEmpty()) {
            log.warn("Packet was not delivered to other clients because no subscriptions are registered");
            return;
        }
        // The concurrent set tolerates concurrent subscribe/unsubscribe during iteration.
        // Delivery is non-blocking: each handler only enqueues onto its own outbound queue.
        for (ClientConnectionHandler clientConnectionHandler : subscribers) {
            clientConnectionHandler.enqueue(packet);
        }
    }

    public void subscribeToTopic(String topic, ClientConnectionHandler clientConnectionHandler) {
        // Atomic create-or-get of the subscriber set, then add. No check-then-put race.
        boolean added = topicSubscriptions
                .computeIfAbsent(normalize(topic), key -> ConcurrentHashMap.newKeySet())
                .add(clientConnectionHandler);
        if (added) {
            metrics.setSubscriptionCount(subscriptionTotal.incrementAndGet());
        }
    }

    public void unsubscribeFromAllTopics(ClientConnectionHandler clientConnectionHandler) {
        // Remove the client from every topic and drop now-empty topic sets to avoid leaking
        // memory for topics that no longer have any subscribers.
        long removed = 0;
        for (Map.Entry<String, Set<ClientConnectionHandler>> entry : topicSubscriptions.entrySet()) {
            String topic = entry.getKey();
            if (entry.getValue().remove(clientConnectionHandler)) {
                removed++;
                // computeIfPresent re-checks emptiness atomically against concurrent inserts.
                topicSubscriptions.computeIfPresent(topic, (key, current) -> current.isEmpty() ? null : current);
            }
        }
        if (removed > 0) {
            metrics.setSubscriptionCount(subscriptionTotal.addAndGet(-removed));
        }
    }
}
