package de.kyle.avenue.handler.subscription;

import de.kyle.avenue.handler.client.ClientConnection;
import de.kyle.avenue.handler.client.ClientConnectionHandler;
import de.kyle.avenue.metrics.AvenueMetrics;
import de.kyle.avenue.proto.ClientEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks which clients are subscribed to which topics and fans packets out to them.
 * <p>
 * Concurrency model: the outer map is a {@link ConcurrentHashMap}; each topic's subscribers are
 * held in a {@link Subscribers} holder as a <b>copy-on-write array</b>. Delivery reads the array
 * reference once and iterates that immutable snapshot, so other threads may concurrently
 * subscribe/unsubscribe without throwing {@code ConcurrentModificationException} and without lost
 * updates. Topic keys are normalized in exactly one place ({@link #normalize(String)}) so
 * subscribe, deliver and unsubscribe always agree on the key.
 *
 * <h2>Why copy-on-write instead of a concurrent set</h2>
 * The subscriber collection is read once per subscriber per message and written only on
 * subscribe/unsubscribe — an extremely read-skewed access pattern. The previous
 * {@link ConcurrentHashMap#newKeySet() concurrent key-set} paid for that read with a fresh
 * {@code KeyIterator} plus a {@code Traverser} walk over hash bins, chasing a pointer per
 * subscriber across scattered {@code Node} objects. JFR on the fan-out path showed
 * {@code fanOut} + {@code ConcurrentHashMap$Traverser.advance} at ~11 % of server CPU
 * <em>even at fan-out 1</em>, with {@code ConcurrentHashMap$KeyIterator} in the allocation profile.
 * <p>
 * A copy-on-write {@code ClientConnection[]} makes delivery a bounds-checked walk over one
 * contiguous, prefetch-friendly array: no iterator object, no bin traversal, one dependent load per
 * subscriber. The cost moves to the mutation side (an array copy per subscribe/unsubscribe), which
 * is exactly where it is affordable.
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

    /** Shared empty snapshot, so a topic without subscribers never allocates. */
    private static final ClientConnection[] EMPTY = new ClientConnection[0];

    /**
     * Per-topic subscriber holder: a copy-on-write array plus the retirement flag that keeps the
     * holder and the map entry in lock-step.
     * <p>
     * Mutations synchronize on the holder itself. {@link #array} is {@code volatile} so a reader
     * that grabs the reference always sees a fully-published, immutable snapshot — readers never
     * lock.
     */
    private static final class Subscribers {
        /** Immutable snapshot; replaced wholesale under {@code this} on every mutation. */
        private volatile ClientConnection[] array = EMPTY;

        /**
         * Set (under {@code this}) when the holder ran empty and was removed from the map. A
         * subscriber that raced in and still holds a reference to this dead holder must discard it
         * and retry, otherwise its subscription would be written into an unreachable holder and
         * silently lost.
         */
        private boolean retired;
    }

    private final Map<String, Subscribers> topicSubscriptions = new ConcurrentHashMap<>();

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
        return subscribersOf(normalizedTopic).length;
    }

    public void deliverToSubscribers(String normalizedTopic, ClientEnvelope envelope, int maxSize) {
        ClientConnection[] subscribers = subscribersOf(normalizedTopic);
        if (subscribers.length == 0) {
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
        ClientConnection[] subscribers = subscribersOf(normalizedTopic);
        if (subscribers.length == 0) {
            log.warn("Packet was not delivered to other clients because no subscriptions are registered");
            return;
        }
        fanOut(subscribers, payload);
    }

    /**
     * Current subscriber snapshot for an already-{@link #normalize(String) normalized} topic key.
     * The returned array is immutable and must never be written to; it is the live snapshot shared
     * with every concurrent reader.
     */
    private ClientConnection[] subscribersOf(String normalizedTopic) {
        Subscribers holder = topicSubscriptions.get(normalizedTopic);
        return holder == null ? EMPTY : holder.array;
    }

    /**
     * Shared fan-out tail: hand the same immutable bytes to every subscriber. Iterating a
     * copy-on-write snapshot tolerates concurrent subscribe/unsubscribe (a subscriber removed mid
     * fan-out may still receive this message, exactly as with the previous concurrent set — its
     * transport drops the frame once closed). Delivery is non-blocking as each handler only enqueues
     * onto its own outbound queue.
     */
    private static void fanOut(ClientConnection[] subscribers, byte[] payload) {
        for (ClientConnection clientConnection : subscribers) {
            clientConnection.enqueuePreSerialized(payload);
        }
    }

    /**
     * Test seam, invoked in {@link #subscribeToTopic} between looking a holder up and locking it —
     * precisely the window in which a concurrent unsubscribe can retire that holder out from under
     * the subscriber. Empty in production (a single no-op call on the cold subscribe path, never on
     * delivery); {@code TopicSubscriptionHandlerTest} overrides it to hold a thread inside that
     * window so the retire/subscribe race is exercised deterministically rather than being left to
     * a nanosecond-wide timing coincidence that a stress loop does not reliably hit.
     */
    void awaitHolderRaceWindow() {
    }

    /**
     * Identity search over a subscriber snapshot. Subscriber membership is deliberately keyed on the
     * live connection instance ({@link ClientConnection} overrides neither {@code equals} nor
     * {@code hashCode}), so this mirrors the identity semantics of the previous set exactly.
     */
    private static int indexOf(ClientConnection[] subscribers, ClientConnection target) {
        for (int i = 0; i < subscribers.length; i++) {
            if (subscribers[i] == target) {
                return i;
            }
        }
        return -1;
    }

    public void subscribeToTopic(String topic, ClientConnection clientConnection) {
        String normalizedTopic = normalize(topic);
        boolean added;
        boolean firstForTopic;
        while (true) {
            Subscribers holder = topicSubscriptions.computeIfAbsent(normalizedTopic, key -> new Subscribers());
            awaitHolderRaceWindow();
            synchronized (holder) {
                if (holder.retired) {
                    // A concurrent unsubscribe emptied this holder and unmapped it. Because the
                    // retirement and the unmapping happen under this same monitor, the next
                    // computeIfAbsent is guaranteed to install a fresh holder — this retries once.
                    continue;
                }
                ClientConnection[] current = holder.array;
                if (indexOf(current, clientConnection) >= 0) {
                    // Already subscribed: idempotent, exactly like the previous Set.add() == false.
                    added = false;
                    firstForTopic = false;
                } else {
                    ClientConnection[] next = Arrays.copyOf(current, current.length + 1);
                    next[current.length] = clientConnection;
                    holder.array = next;
                    added = true;
                    // An empty holder can only be a freshly created one (an emptied holder is
                    // retired and unmapped in the same critical section), so this is exactly the
                    // "topic gained its first subscriber" transition.
                    firstForTopic = current.length == 0;
                }
                break;
            }
        }
        if (added) {
            metrics.setSubscriptionCount(subscriptionTotal.incrementAndGet());
        }
        // Fire the interest-added event AFTER the mutation and outside the holder monitor, so the
        // listener never runs while holding a lock the delivery path could contend on.
        if (firstForTopic) {
            interestListener.onInterestAdded(normalizedTopic);
        }
    }

    public void unsubscribeFromAllTopics(ClientConnection clientConnection) {
        // Remove the client from every topic and drop now-empty topic holders to avoid leaking
        // memory for topics that no longer have any subscribers.
        long removed = 0;
        // Collect the topics whose LAST subscriber we just removed, then fire interest-removed for
        // each AFTER the loop. Deferring the events keeps the listener off the holder monitors and
        // avoids re-entrancy surprises.
        List<String> nowEmptyTopics = null;
        for (Map.Entry<String, Subscribers> entry : topicSubscriptions.entrySet()) {
            Subscribers holder = entry.getValue();
            boolean didRemove;
            boolean becameEmpty = false;
            synchronized (holder) {
                ClientConnection[] current = holder.array;
                int index = indexOf(current, clientConnection);
                didRemove = index >= 0;
                if (didRemove) {
                    if (current.length == 1) {
                        // Last subscriber gone: retire the holder AND unmap it in the same critical
                        // section. Doing both under the monitor is what makes the subscribe-side
                        // retry terminate — a racing subscriber can only observe `retired` after the
                        // entry is already gone from the map, so its next computeIfAbsent creates a
                        // fresh holder rather than spinning on this one.
                        holder.array = EMPTY;
                        holder.retired = true;
                        topicSubscriptions.remove(entry.getKey(), holder);
                        becameEmpty = true;
                    } else {
                        ClientConnection[] next = new ClientConnection[current.length - 1];
                        System.arraycopy(current, 0, next, 0, index);
                        System.arraycopy(current, index + 1, next, index, current.length - 1 - index);
                        holder.array = next;
                    }
                }
            }
            if (didRemove) {
                removed++;
            }
            if (becameEmpty) {
                if (nowEmptyTopics == null) {
                    nowEmptyTopics = new ArrayList<>();
                }
                nowEmptyTopics.add(entry.getKey());
            }
        }
        if (removed > 0) {
            metrics.setSubscriptionCount(subscriptionTotal.addAndGet(-removed));
        }
        if (nowEmptyTopics != null) {
            for (String topic : nowEmptyTopics) {
                interestListener.onInterestRemoved(topic);
            }
        }
    }
}
