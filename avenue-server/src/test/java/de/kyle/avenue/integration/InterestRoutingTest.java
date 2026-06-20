package de.kyle.avenue.integration;

import de.kyle.avenue.cluster.ClusterNode;
import de.kyle.avenue.config.AvenueConfig;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Phase D — interest-based routing integration tests.
 * <p>
 * Topology (two nodes, full-duplex single link):
 * <pre>
 *   clientA --(TCP)--> node1 --(cluster)--> node2 <--(TCP)-- clientB
 * </pre>
 * node2 initiates the cluster link to node1. Determinism is achieved without fixed sleeps by:
 * <ul>
 *   <li>{@link ClusterNode#awaitAnyPeer} for link establishment,</li>
 *   <li>{@link TestClient#subscribe} blocking on the subscribe ack,</li>
 *   <li>polling {@link ClusterNode#knowsInterest} until interest has converged on the publisher
 *       node before publishing, and</li>
 *   <li>bounded metric / received-set polling for the assertions.</li>
 * </ul>
 */
class InterestRoutingTest {

    private static final String HOST = "127.0.0.1";
    private static final String SECRET = "int-test-secret";
    private static final String TOKEN = "int-test-token";
    private static final String CLUSTER_SECRET = "cluster-secret-42";
    private static final int PACKET_SIZE = 65536;
    private static final long TIMEOUT = 10;
    private static final TimeUnit UNIT = TimeUnit.SECONDS;
    private static final long HEARTBEAT_MS = 1_000L;

    private ClusterNode node1;
    private ClusterNode node2;

    @AfterEach
    void stopNodes() {
        if (node2 != null) node2.stop();
        if (node1 != null) node1.stop();
    }

    private AvenueConfig config(String nodeId, List<String> peers) {
        return new AvenueConfig(
                PACKET_SIZE, true, SECRET, TOKEN,
                0, 1024, 100,
                true, nodeId, 0, peers, CLUSTER_SECRET, HEARTBEAT_MS);
    }

    /** Brings up node1 (no outbound peers) and node2 (outbound to node1), linked both ways. */
    private void startLinkedNodes() throws InterruptedException {
        node1 = new ClusterNode(config("node-1", List.of()));
        node1.start();
        Assertions.assertTrue(node1.getClusterPort() > 0);

        String node1Addr = HOST + ":" + node1.getClusterPort();
        node2 = new ClusterNode(config("node-2", List.of(node1Addr)));
        node2.start();

        Assertions.assertTrue(node2.awaitAnyPeer(TIMEOUT, UNIT), "node2 must link to node1");
        awaitTrue(() -> node1.getActivePeerCount() > 0, "node1 must see node2");
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

    // -------------------------------------------------------------------------
    // 1) interested_node_receives
    // -------------------------------------------------------------------------

    @RepeatedTest(3)
    void interested_node_receives() throws Exception {
        startLinkedNodes();
        try (TestClient sub = new TestClient(HOST, node2.getClientPort(), PACKET_SIZE);
             TestClient pub = new TestClient(HOST, node1.getClientPort(), PACKET_SIZE)) {
            sub.authenticate(SECRET, TIMEOUT, UNIT);
            pub.authenticate(SECRET, TIMEOUT, UNIT);

            // Subscriber on node2 -> node2 propagates interest for "T" to node1.
            sub.subscribe("T", TOKEN, TIMEOUT, UNIT);

            // Wait until node1 (the publisher's node) has learned node2's interest in "T". This
            // makes the publish deterministically routable.
            awaitTrue(() -> node1.knowsInterest("node-2", "t"),
                    "node1 must learn node2's interest in topic T before publishing");

            pub.publish("T", "hello-interested", "pub", TOKEN);

            JSONObject received = sub.awaitPacket("PublishMessageOutboundPacket", TIMEOUT, UNIT);
            Assertions.assertNotNull(received, "interested node2 must receive the publish");
            Assertions.assertEquals("hello-interested",
                    received.getJSONObject("body").getString("data"));
        }
    }

    // -------------------------------------------------------------------------
    // 2) uninterested_node_gets_nothing — the core negative test
    // -------------------------------------------------------------------------

    @RepeatedTest(3)
    void uninterested_node_gets_nothing() throws Exception {
        startLinkedNodes();
        try (TestClient pub = new TestClient(HOST, node1.getClientPort(), PACKET_SIZE)) {
            pub.authenticate(SECRET, TIMEOUT, UNIT);

            // node2 has NO subscriber for "T". After the link is up and the initial interest sync has
            // flowed, node1 must know node2 is NOT interested in "T". Wait for that negative state to
            // be stable: node1 has applied node2's (empty) interest sync.
            awaitTrue(() -> node1.getClusterMetrics().getInterestUpdatesReceived() > 0,
                    "node1 must have received node2's initial interest sync");
            Assertions.assertFalse(node1.knowsInterest("node-2", "t"),
                    "node2 has no subscriber, so node1 must not record interest in T");

            long forwardedBefore = node1.getClusterMetrics().getMessagesForwarded();
            long receivedBefore = node2.getClusterMetrics().getMessagesReceived();

            // Publish several messages for the uninterested topic.
            for (int i = 0; i < 5; i++) {
                pub.publish("T", "should-not-arrive-" + i, "pub", TOKEN);
            }

            // The publisher node must record the skip (interest routing saved the fan-out)...
            awaitTrue(() -> node1.getClusterMetrics().getInterestRoutedSkipped() > 0,
                    "node1 must record interest-routed skips for the uninterested peer");

            // ...and must NOT have forwarded anything to node2 (no new forwarded frames)...
            Assertions.assertEquals(forwardedBefore, node1.getClusterMetrics().getMessagesForwarded(),
                    "no publish frame may be forwarded to the uninterested peer");

            // ...and node2 must NOT have received any ClusterPublishPacket. Give the wire a real
            // chance: poll for a short window and assert the receive counter never moves.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (System.nanoTime() < deadline) {
                Assertions.assertEquals(receivedBefore, node2.getClusterMetrics().getMessagesReceived(),
                        "uninterested node2 must never receive a cluster publish");
                Thread.sleep(50);
            }
        }
    }

    // -------------------------------------------------------------------------
    // 3) interest_propagation — new subscription updates the other node, then traffic flows
    // -------------------------------------------------------------------------

    @Test
    void interest_propagation() throws Exception {
        startLinkedNodes();
        try (TestClient sub = new TestClient(HOST, node2.getClientPort(), PACKET_SIZE);
             TestClient pub = new TestClient(HOST, node1.getClientPort(), PACKET_SIZE)) {
            sub.authenticate(SECRET, TIMEOUT, UNIT);
            pub.authenticate(SECRET, TIMEOUT, UNIT);

            // Initially node1 does not know about interest in "late".
            Assertions.assertFalse(node1.knowsInterest("node-2", "late"));

            // A NEW subscription must propagate to node1.
            sub.subscribe("late", TOKEN, TIMEOUT, UNIT);
            awaitTrue(() -> node1.getClusterMetrics().getInterestUpdatesReceived() > 0,
                    "node1 must receive at least one interest update");
            awaitTrue(() -> node1.knowsInterest("node-2", "late"),
                    "node1's routing table must reflect the new subscription");

            // Then traffic flows to the now-interested node.
            pub.publish("late", "after-subscribe", "pub", TOKEN);
            JSONObject received = sub.awaitPacket("PublishMessageOutboundPacket", TIMEOUT, UNIT);
            Assertions.assertNotNull(received, "traffic must flow after interest propagation");
            Assertions.assertEquals("after-subscribe",
                    received.getJSONObject("body").getString("data"));
        }
    }

    // -------------------------------------------------------------------------
    // 4) unsubscribe removes interest (last subscriber gone)
    // -------------------------------------------------------------------------

    @Test
    void unsubscribe_removes_interest_on_publisher_node() throws Exception {
        startLinkedNodes();
        TestClient sub = new TestClient(HOST, node2.getClientPort(), PACKET_SIZE);
        try {
            sub.authenticate(SECRET, TIMEOUT, UNIT);
            sub.subscribe("bye", TOKEN, TIMEOUT, UNIT);
            awaitTrue(() -> node1.knowsInterest("node-2", "bye"),
                    "node1 must first learn the interest");

            // Closing the only subscriber removes the last subscription for "bye" on node2, which
            // must propagate an interest-removed delta to node1.
            sub.close();
            awaitTrue(() -> !node1.knowsInterest("node-2", "bye"),
                    "node1 must drop the interest once node2's last subscriber leaves");
        } finally {
            sub.close();
        }
    }
}
