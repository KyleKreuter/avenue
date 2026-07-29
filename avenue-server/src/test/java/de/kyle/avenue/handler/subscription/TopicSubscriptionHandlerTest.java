package de.kyle.avenue.handler.subscription;

import de.kyle.avenue.handler.client.ClientConnection;
import de.kyle.avenue.proto.ClientEnvelope;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link TopicSubscriptionHandler}, focused on the copy-on-write subscriber array
 * that replaced the concurrent key-set.
 * <p>
 * The single-threaded cases pin the observable contract (idempotent subscribe, fan-out reaches every
 * subscriber, interest transitions fire exactly on the first/last subscriber). The concurrent case
 * targets the one race the copy-on-write design introduces: a holder that runs empty is retired and
 * unmapped, so a subscriber racing against that teardown must retry against a fresh holder instead
 * of writing into the dead one — a lost subscription there would silently stop delivering to a live
 * client. Pure in-memory, no I/O.
 */
class TopicSubscriptionHandlerTest {

    /** Minimal {@link ClientConnection} that only counts what it was handed. */
    private static final class RecordingConnection implements ClientConnection {
        private final AtomicInteger preSerialized = new AtomicInteger();

        @Override
        public void enqueue(ClientEnvelope envelope) {
        }

        @Override
        public void send(ClientEnvelope envelope) {
        }

        @Override
        public void enqueuePreSerialized(byte[] frame) {
            preSerialized.incrementAndGet();
        }
    }

    /** Records the interest transitions the cluster layer would react to. */
    private static final class RecordingInterestListener implements TopicSubscriptionHandler.InterestListener {
        private final List<String> added = Collections.synchronizedList(new ArrayList<>());
        private final List<String> removed = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void onInterestAdded(String topic) {
            added.add(topic);
        }

        @Override
        public void onInterestRemoved(String topic) {
            removed.add(topic);
        }
    }

    @Test
    void subscribe_is_idempotent_for_the_same_connection() {
        TopicSubscriptionHandler handler = new TopicSubscriptionHandler();
        RecordingConnection connection = new RecordingConnection();

        handler.subscribeToTopic("Orders", connection);
        handler.subscribeToTopic("orders", connection);
        handler.subscribeToTopic("  ORDERS  ", connection);

        Assertions.assertEquals(1, handler.subscriberCount("orders"),
                "re-subscribing the same connection must not duplicate it");
    }

    @Test
    void fan_out_reaches_every_subscriber_exactly_once() {
        TopicSubscriptionHandler handler = new TopicSubscriptionHandler();
        List<RecordingConnection> connections = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            RecordingConnection connection = new RecordingConnection();
            connections.add(connection);
            handler.subscribeToTopic("orders", connection);
        }

        Assertions.assertEquals(16, handler.subscriberCount("orders"));
        handler.deliverPreSerializedToSubscribers("orders", new byte[] {1, 2, 3});

        for (RecordingConnection connection : connections) {
            Assertions.assertEquals(1, connection.preSerialized.get(),
                    "every subscriber must receive the fan-out exactly once");
        }
    }

    @Test
    void interest_transitions_fire_on_first_and_last_subscriber_only() {
        TopicSubscriptionHandler handler = new TopicSubscriptionHandler();
        RecordingInterestListener listener = new RecordingInterestListener();
        handler.setInterestListener(listener);

        RecordingConnection first = new RecordingConnection();
        RecordingConnection second = new RecordingConnection();

        handler.subscribeToTopic("orders", first);
        handler.subscribeToTopic("orders", second);
        Assertions.assertEquals(List.of("orders"), listener.added,
                "interest must be announced only when the topic gains its FIRST subscriber");

        handler.unsubscribeFromAllTopics(first);
        Assertions.assertTrue(listener.removed.isEmpty(),
                "interest must be retained while another subscriber remains");

        handler.unsubscribeFromAllTopics(second);
        Assertions.assertEquals(List.of("orders"), listener.removed,
                "interest must be withdrawn when the topic loses its LAST subscriber");

        // Re-subscribing after the holder was retired must announce interest again.
        handler.subscribeToTopic("orders", first);
        Assertions.assertEquals(List.of("orders", "orders"), listener.added,
                "a topic resurrected after retirement must re-announce interest");
    }

    @Test
    void unsubscribe_leaves_the_remaining_subscribers_intact() {
        TopicSubscriptionHandler handler = new TopicSubscriptionHandler();
        RecordingConnection keep1 = new RecordingConnection();
        RecordingConnection drop = new RecordingConnection();
        RecordingConnection keep2 = new RecordingConnection();

        handler.subscribeToTopic("orders", keep1);
        handler.subscribeToTopic("orders", drop);
        handler.subscribeToTopic("orders", keep2);

        // Removing from the MIDDLE of the array exercises both arraycopy halves.
        handler.unsubscribeFromAllTopics(drop);
        Assertions.assertEquals(2, handler.subscriberCount("orders"));

        handler.deliverPreSerializedToSubscribers("orders", new byte[] {1});
        Assertions.assertEquals(1, keep1.preSerialized.get());
        Assertions.assertEquals(1, keep2.preSerialized.get());
        Assertions.assertEquals(0, drop.preSerialized.get(),
                "an unsubscribed connection must not receive further messages");
    }

    @Test
    void subscription_gauge_tracks_add_and_remove() {
        TopicSubscriptionHandler handler = new TopicSubscriptionHandler();
        RecordingConnection connection = new RecordingConnection();

        handler.subscribeToTopic("a", connection);
        handler.subscribeToTopic("b", connection);
        handler.subscribeToTopic("a", connection); // idempotent, must not double-count

        handler.unsubscribeFromAllTopics(connection);
        Assertions.assertEquals(0, handler.subscriberCount("a"));
        Assertions.assertEquals(0, handler.subscriberCount("b"));
    }

    /**
     * Hammers the retire/resubscribe race: many threads repeatedly take a topic down to zero
     * subscribers (retiring and unmapping the holder) while others subscribe to that same topic.
     * If a subscriber were written into an already-retired holder, it would be unreachable from the
     * map and the final delivery would miss it.
     */
    @Test
    void concurrent_subscribe_and_unsubscribe_never_loses_a_subscription() throws Exception {
        TopicSubscriptionHandler handler = new TopicSubscriptionHandler();
        int threads = 8;
        int rounds = 500;

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<RecordingConnection> connections = new ArrayList<>();
        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();

        try {
            for (int t = 0; t < threads; t++) {
                RecordingConnection connection = new RecordingConnection();
                connections.add(connection);
                futures.add(pool.submit(() -> {
                    start.await();
                    for (int round = 0; round < rounds; round++) {
                        // Every thread churns the SAME topic, so holders are constantly retired and
                        // recreated underneath each other.
                        handler.subscribeToTopic("hot-topic", connection);
                        handler.unsubscribeFromAllTopics(connection);
                    }
                    return null;
                }));
            }
            start.countDown();
            for (java.util.concurrent.Future<?> future : futures) {
                future.get(60, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        // The churn ended with every connection unsubscribed, so the topic must be fully drained.
        Assertions.assertEquals(0, handler.subscriberCount("hot-topic"),
                "after the churn every subscription must be gone");

        // Now subscribe all of them once more and verify each is genuinely reachable via the map.
        for (RecordingConnection connection : connections) {
            handler.subscribeToTopic("hot-topic", connection);
        }
        Assertions.assertEquals(threads, handler.subscriberCount("hot-topic"),
                "every subscription registered after the churn must be visible");

        handler.deliverPreSerializedToSubscribers("hot-topic", new byte[] {7});
        for (RecordingConnection connection : connections) {
            Assertions.assertEquals(1, connection.preSerialized.get(),
                    "a subscription must never be stranded in a retired holder");
        }
    }

    /**
     * Deterministically drives the retire/subscribe race the stress loop above cannot reliably hit,
     * by parking a subscriber inside the window between holder lookup and holder lock while another
     * thread takes the topic down to zero (retiring and unmapping that very holder).
     * <p>
     * Without the retry on {@code retired} the parked subscriber writes itself into the dead holder:
     * the topic then reports zero subscribers and the client silently never receives anything again.
     */
    @Test
    void subscriber_racing_a_holder_retirement_is_not_lost() throws Exception {
        CountDownLatch insideWindow = new CountDownLatch(1);
        CountDownLatch retirementDone = new CountDownLatch(1);
        AtomicInteger windowHits = new AtomicInteger();
        // Armed only after the pre-state is in place, so the setup subscribe does not park itself.
        AtomicBoolean armed = new AtomicBoolean();

        TopicSubscriptionHandler handler = new TopicSubscriptionHandler() {
            @Override
            void awaitHolderRaceWindow() {
                // Only the FIRST armed pass parks: the retry after observing `retired` must run through.
                if (!armed.get() || windowHits.incrementAndGet() != 1) {
                    return;
                }
                insideWindow.countDown();
                try {
                    Assertions.assertTrue(retirementDone.await(10, TimeUnit.SECONDS),
                            "retirement must complete while the subscriber is parked");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };

        RecordingConnection leaving = new RecordingConnection();
        RecordingConnection joining = new RecordingConnection();

        // Pre-state: the topic exists with exactly one subscriber, so removing it retires the holder.
        handler.subscribeToTopic("orders", leaving);
        armed.set(true);

        Thread subscriber = new Thread(() -> handler.subscribeToTopic("orders", joining), "racing-subscriber");
        subscriber.start();

        // Wait until the subscriber holds the (about to die) holder but has not locked it yet.
        Assertions.assertTrue(insideWindow.await(10, TimeUnit.SECONDS),
                "subscriber must reach the race window");
        handler.unsubscribeFromAllTopics(leaving);
        retirementDone.countDown();

        subscriber.join(TimeUnit.SECONDS.toMillis(10));
        Assertions.assertFalse(subscriber.isAlive(), "the racing subscriber must not hang");

        Assertions.assertEquals(1, handler.subscriberCount("orders"),
                "the subscription that raced the retirement must be visible on the live holder");
        handler.deliverPreSerializedToSubscribers("orders", new byte[] {1});
        Assertions.assertEquals(1, joining.preSerialized.get(),
                "the racing subscriber must still receive messages");
        Assertions.assertEquals(0, leaving.preSerialized.get());
    }
}
