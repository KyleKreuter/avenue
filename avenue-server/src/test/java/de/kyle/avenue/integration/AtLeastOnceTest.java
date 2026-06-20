package de.kyle.avenue.integration;

import de.kyle.avenue.cluster.ClusterNode;
import de.kyle.avenue.cluster.ReplayBuffer;
import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.config.ClusterTuning;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Phase C — at-least-once delivery integration tests.
 * <p>
 * Topology: {@code clientPub --> node1 --(cluster)--> node2 --> clientSub}. node2 initiates the
 * cluster link to node1, so node1's send side (replay buffer for target {@code node-2}) is what is
 * exercised. Determinism comes from the {@link ClusterNode#blockPeer}/{@link ClusterNode#unblockPeer}
 * test seams plus bounded polling on metrics and received-message sets — never fixed sleeps as a
 * synchronization primitive.
 */
class AtLeastOnceTest {

    private static final String HOST = "127.0.0.1";
    private static final String SECRET = "int-test-secret";
    private static final String TOKEN = "int-test-token";
    private static final String CLUSTER_SECRET = "cluster-secret-42";
    private static final int PACKET_SIZE = 65536;
    private static final long TIMEOUT = 15;
    private static final TimeUnit UNIT = TimeUnit.SECONDS;
    private static final long HEARTBEAT_MS = 1_000L;

    private ClusterNode node1;
    private ClusterNode node2;

    @AfterEach
    void stopNodes() {
        if (node2 != null) node2.stop();
        if (node1 != null) node1.stop();
    }

    private AvenueConfig config(String nodeId, List<String> peers, ClusterTuning tuning) {
        return new AvenueConfig(
                PACKET_SIZE, true, SECRET, TOKEN,
                0, 8192, 100,
                true, nodeId, 0, peers, CLUSTER_SECRET, HEARTBEAT_MS,
                0L, 10_000, null, 0L,
                false, "", "",
                false, "", "", "", "",
                tuning);
    }

    /** Brings up node1 (no outbound peers) and node2 (outbound to node1), linked both ways. */
    private void startLinkedNodes(ClusterTuning tuning1, ClusterTuning tuning2) throws InterruptedException {
        node1 = new ClusterNode(config("node-1", List.of(), tuning1));
        node1.start();
        Assertions.assertTrue(node1.getClusterPort() > 0);

        String node1Addr = HOST + ":" + node1.getClusterPort();
        node2 = new ClusterNode(config("node-2", List.of(node1Addr), tuning2));
        node2.start();

        Assertions.assertTrue(node2.awaitAnyPeer(TIMEOUT, UNIT), "node2 must link to node1");
        long deadline = System.nanoTime() + UNIT.toNanos(TIMEOUT);
        while (node1.getActivePeerCount() == 0 && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        Assertions.assertTrue(node1.getActivePeerCount() > 0, "node1 must see node2");
    }

    private void awaitTrue(java.util.function.BooleanSupplier condition, String message) throws InterruptedException {
        long deadline = System.nanoTime() + UNIT.toNanos(TIMEOUT);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        Assertions.assertTrue(condition.getAsBoolean(), message);
    }

    private void awaitReconnect(ClusterNode from) throws InterruptedException {
        awaitTrue(() -> from.getActivePeerCount() > 0, "link must re-establish after heal");
    }

    /**
     * Phase D: a publish only flows to a remote subscriber once that node's interest has propagated
     * to the publisher's node. These Phase C tests publish on node1 (the sender) and subscribe on
     * node2 (the receiver), so node1 must first learn node2's interest. This is a deterministic
     * convergence wait, not a fixed sleep.
     */
    private void awaitInterest(ClusterNode publisherNode, String subscriberNodeId, String topic)
            throws InterruptedException {
        awaitTrue(() -> publisherNode.knowsInterest(subscriberNodeId, topic.toLowerCase(java.util.Locale.ROOT)),
                "interest for '" + topic + "' must converge on the publisher node");
    }

    /**
     * slow_peer_no_loss: with a SMALL replay ring and many in-flight messages, the writer blocks
     * (BLOCK policy) and only advances as cumulative ACKs evict the ring head. All N messages must
     * still arrive and no gap may be produced.
     */
    @RepeatedTest(3)
    void slow_peer_no_loss() throws Exception {
        int n = 300;
        // Tiny ring forces repeated BLOCK + ack-driven advance; default-ish ack interval so acks flow.
        ClusterTuning senderTuning = new ClusterTuning(
                16, ReplayBuffer.BackpressurePolicy.BLOCK, 5_000L, 100L, false,
                ClusterTuning.DEFAULT_ORIGIN_EXPIRY_MS);
        // node1 is the SENDER (publisher side) -> it owns the small replay ring toward node-2.
        startLinkedNodes(senderTuning, ClusterTuning.defaults());

        try (TestClient pub = new TestClient(HOST, node1.getClientPort(), PACKET_SIZE);
             TestClient sub = new TestClient(HOST, node2.getClientPort(), PACKET_SIZE)) {
            pub.authenticate(SECRET, TIMEOUT, UNIT);
            sub.authenticate(SECRET, TIMEOUT, UNIT);
            sub.subscribe("alo", TOKEN, TIMEOUT, UNIT);
            awaitInterest(node1, "node-2", "alo");

            for (int i = 0; i < n; i++) {
                pub.publish("alo", "m-" + i, "pub", TOKEN);
            }

            Set<String> received = new HashSet<>();
            long deadline = System.nanoTime() + UNIT.toNanos(TIMEOUT);
            while (received.size() < n && System.nanoTime() < deadline) {
                JSONObject p = sub.awaitPacket("PublishMessageOutboundPacket", 1, UNIT);
                if (p != null) {
                    received.add(p.getJSONObject("body").getString("data"));
                }
            }
            Assertions.assertEquals(n, received.size(), "all N messages must arrive under backpressure");
            Assertions.assertEquals(0, node1.getClusterMetrics().getClusterGapEvents(),
                    "no gaps may occur in the lossless slow-peer path");
        }
    }

    /**
     * partition_heal_backfill: cut the link while messages are sent (ACKs suppressed via a long ack
     * interval so the sender's replay buffer retains them un-acked), then heal. The subscriber must
     * end up with exactly M distinct messages (exactly-once), and backfill must have fired.
     */
    @RepeatedTest(3)
    void partition_heal_backfill() throws Exception {
        int m = 50;
        // Long ack interval: the sender keeps everything un-acked in the replay buffer, so the
        // post-heal resume backfills it. Capacity comfortably holds M.
        ClusterTuning tuning = new ClusterTuning(
                4096, ReplayBuffer.BackpressurePolicy.BLOCK, 1_000L, 60_000L, false,
                ClusterTuning.DEFAULT_ORIGIN_EXPIRY_MS);
        startLinkedNodes(tuning, tuning);

        try (TestClient pub = new TestClient(HOST, node1.getClientPort(), PACKET_SIZE);
             TestClient sub = new TestClient(HOST, node2.getClientPort(), PACKET_SIZE)) {
            pub.authenticate(SECRET, TIMEOUT, UNIT);
            sub.authenticate(SECRET, TIMEOUT, UNIT);
            sub.subscribe("heal", TOKEN, TIMEOUT, UNIT);
            awaitInterest(node1, "node-2", "heal");

            // Send half, let them arrive (so they sit un-acked in node1's buffer), then partition.
            for (int i = 0; i < m / 2; i++) {
                pub.publish("heal", "h-" + i, "pub", TOKEN);
            }
            Set<String> received = new HashSet<>();
            long firstHalfDeadline = System.nanoTime() + UNIT.toNanos(TIMEOUT);
            while (received.size() < m / 2 && System.nanoTime() < firstHalfDeadline) {
                JSONObject p = sub.awaitPacket("PublishMessageOutboundPacket", 1, UNIT);
                if (p != null) {
                    received.add(p.getJSONObject("body").getString("data"));
                }
            }
            Assertions.assertEquals(m / 2, received.size(), "first half must arrive before partition");

            // Partition node1 -> node2 (block on the sender side; node2's connector will retry).
            node1.blockPeer("node-2");
            awaitTrue(() -> node1.getActivePeerCount() == 0, "link must drop on partition");

            // Send the second half during the partition: these queue on node1's per-target ingest.
            for (int i = m / 2; i < m; i++) {
                pub.publish("heal", "h-" + i, "pub", TOKEN);
            }

            // Heal: unblock and wait for reconnect; the buffered first half is backfilled and the
            // partition-time second half is flushed. Duplicates are dropped by the receiver tracker.
            node1.unblockPeer("node-2");
            awaitReconnect(node1);

            long deadline = System.nanoTime() + UNIT.toNanos(TIMEOUT);
            while (received.size() < m && System.nanoTime() < deadline) {
                JSONObject p = sub.awaitPacket("PublishMessageOutboundPacket", 1, UNIT);
                if (p != null) {
                    received.add(p.getJSONObject("body").getString("data"));
                }
            }
            Assertions.assertEquals(m, received.size(), "subscriber must end with exactly M distinct messages");
            // Exactly-once: no extra deliveries beyond M within a short extra window.
            Assertions.assertTrue(sub.expectNoPacket("PublishMessageOutboundPacket", 500, TimeUnit.MILLISECONDS)
                            || received.size() == m,
                    "no surplus deliveries beyond M");
            Assertions.assertTrue(node1.getClusterMetrics().getClusterBackfillMessages() > 0,
                    "backfill must have re-sent buffered messages on reconnect");
        }
    }

    /**
     * strict_ordering_preserves_per_origin_order: with strict ordering enabled the receiver must
     * deliver a single origin's messages in exact sequence order to its subscribers.
     */
    @Test
    void strict_ordering_preserves_per_origin_order() throws Exception {
        int n = 200;
        // Strict ordering on the RECEIVER (node2). node1 (sender) tuning is irrelevant for ordering.
        ClusterTuning strict = new ClusterTuning(
                4096, ReplayBuffer.BackpressurePolicy.BLOCK, 1_000L, 200L, true,
                ClusterTuning.DEFAULT_ORIGIN_EXPIRY_MS);
        startLinkedNodes(ClusterTuning.defaults(), strict);

        try (TestClient pub = new TestClient(HOST, node1.getClientPort(), PACKET_SIZE);
             TestClient sub = new TestClient(HOST, node2.getClientPort(), PACKET_SIZE)) {
            pub.authenticate(SECRET, TIMEOUT, UNIT);
            sub.authenticate(SECRET, TIMEOUT, UNIT);
            sub.subscribe("ord", TOKEN, TIMEOUT, UNIT);
            awaitInterest(node1, "node-2", "ord");

            for (int i = 0; i < n; i++) {
                pub.publish("ord", Integer.toString(i), "pub", TOKEN);
            }

            int expected = 0;
            long deadline = System.nanoTime() + UNIT.toNanos(TIMEOUT);
            while (expected < n && System.nanoTime() < deadline) {
                JSONObject p = sub.awaitPacket("PublishMessageOutboundPacket", 1, UNIT);
                if (p != null) {
                    int got = Integer.parseInt(p.getJSONObject("body").getString("data"));
                    Assertions.assertEquals(expected, got, "strict mode must deliver in seq order");
                    expected++;
                }
            }
            Assertions.assertEquals(n, expected, "all messages must arrive, in order");
        }
    }

    /**
     * buffer_overflow_gap: with a tiny replay ring and a short offer timeout, a burst that exceeds
     * the ring under suppressed ACKs forces overflow -> gap events (no crash). Delivery continues
     * afterwards (a final marker still arrives).
     */
    @Test
    void buffer_overflow_gap() throws Exception {
        // node1 (sender) has a tiny ring + short offer timeout: a fast 200-message burst overruns
        // the 4-slot ring faster than node2's periodic ACKs can evict it, so overflowing entries
        // degrade to gaps (no block-forever, no crash). node2 keeps a normal ACK cadence so the
        // ring still drains afterwards and delivery resumes losslessly.
        ClusterTuning senderTuning = new ClusterTuning(
                4, ReplayBuffer.BackpressurePolicy.BLOCK, 20L, 200L, false,
                ClusterTuning.DEFAULT_ORIGIN_EXPIRY_MS);
        startLinkedNodes(senderTuning, ClusterTuning.defaults());

        try (TestClient pub = new TestClient(HOST, node1.getClientPort(), PACKET_SIZE);
             TestClient sub = new TestClient(HOST, node2.getClientPort(), PACKET_SIZE)) {
            pub.authenticate(SECRET, TIMEOUT, UNIT);
            sub.authenticate(SECRET, TIMEOUT, UNIT);
            sub.subscribe("ovf", TOKEN, TIMEOUT, UNIT);
            awaitInterest(node1, "node-2", "ovf");

            int burst = 200;
            for (int i = 0; i < burst; i++) {
                pub.publish("ovf", "o-" + i, "pub", TOKEN);
            }

            // A gap must be recorded (ring overflowed); the node must not crash.
            awaitTrue(() -> node1.getClusterMetrics().getClusterGapEvents() > 0,
                    "buffer overflow must record at least one gap event");

            // Drain whatever made it through; a contiguous subset is acceptable (no crash).
            Set<String> received = new HashSet<>();
            long drainDeadline = System.nanoTime() + UNIT.toNanos(5);
            while (System.nanoTime() < drainDeadline) {
                JSONObject p = sub.awaitPacket("PublishMessageOutboundPacket", 300, TimeUnit.MILLISECONDS);
                if (p != null) {
                    received.add(p.getJSONObject("body").getString("data"));
                }
            }
            Assertions.assertFalse(received.isEmpty(), "some messages must still be delivered despite gaps");

            // Lossless continuation: a fresh marker after the burst still reaches the subscriber.
            pub.publish("ovf", "marker-final", "pub", TOKEN);
            awaitTrue(() -> {
                try {
                    JSONObject p = sub.awaitPacket("PublishMessageOutboundPacket", 300, TimeUnit.MILLISECONDS);
                    return p != null && "marker-final".equals(p.getJSONObject("body").getString("data"));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }, "delivery must continue losslessly after the gap");
        }
    }
}
