package de.kyle.avenue.handler.subscription;

import de.kyle.avenue.handler.client.ClientConnectionHandler;
import de.kyle.avenue.packet.OutboundPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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

    /**
     * Single source of truth for topic-key normalization. Must be used by every method that
     * reads or writes {@link #topicSubscriptions} so that keys never diverge.
     */
    private String normalize(String topic) {
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
        topicSubscriptions
                .computeIfAbsent(normalize(topic), key -> ConcurrentHashMap.newKeySet())
                .add(clientConnectionHandler);
    }

    public void unsubscribeFromAllTopics(ClientConnectionHandler clientConnectionHandler) {
        // Remove the client from every topic and drop now-empty topic sets to avoid leaking
        // memory for topics that no longer have any subscribers.
        topicSubscriptions.forEach((topic, subscribers) -> {
            if (subscribers.remove(clientConnectionHandler)) {
                // computeIfPresent re-checks emptiness atomically against concurrent inserts.
                topicSubscriptions.computeIfPresent(topic, (key, current) -> current.isEmpty() ? null : current);
            }
        });
    }
}
