package de.kyle.avenue.integration;

import de.kyle.avenue.cluster.ClusterNode;
import de.kyle.avenue.config.AvenueConfig;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Cluster integration test (Wave 4).
 * <p>
 * Spins up TWO {@link ClusterNode} instances in-process on ephemeral client AND cluster
 * ports, configures them as mutual peers, and exercises cross-node publish/subscribe.
 * <p>
 * Determinism is achieved via:
 * <ol>
 *   <li>{@link ClusterNode#awaitAnyPeer} — blocks until the peer TCP link is established
 *       before any client action is taken.</li>
 *   <li>{@link TestClient#subscribe} — blocks on the subscribe acknowledgment so the
 *       subscription is guaranteed server-side before the publish is sent.</li>
 *   <li>All assertions use bounded waits (no fixed-duration {@code Thread.sleep}).</li>
 * </ol>
 * The two-node topology is:
 * <pre>
 *   clientA --(TCP)--> node1 --(cluster)--> node2 <--(TCP)-- clientB
 * </pre>
 */
class ClusterIntegrationTest {

    private static final String HOST = "127.0.0.1";
    private static final String SECRET = "int-test-secret";
    private static final String TOKEN = "int-test-token";
    private static final String CLUSTER_SECRET = "cluster-secret-42";
    private static final int PACKET_SIZE = 65536;
    private static final long TIMEOUT = 10;
    private static final TimeUnit UNIT = TimeUnit.SECONDS;

    // Fast heartbeat so the watchdog doesn't trigger during the test.
    private static final long HEARTBEAT_MS = 1_000L;

    private ClusterNode node1;
    private ClusterNode node2;

    @BeforeEach
    void startNodes() throws InterruptedException {
        // Phase 1: start node1 without any peers so we can learn its cluster port.
        // Then start node2 with node1 as peer, and re-configure node1 with node2 as peer.
        // Since static peer lists are fixed at construction, we start node1 first with NO
        // outbound peers (node2 will connect inbound to node1) and node2 with node1 as peer.
        // Full-mesh in 2-node case: only ONE outbound direction is needed; the other side
        // accepts the incoming connection.

        // Start node1 first with cluster enabled but empty peer list.
        // node2 will be configured to connect outbound to node1's cluster port.
        AvenueConfig config1 = new AvenueConfig(
                PACKET_SIZE,
                true,
                SECRET, TOKEN,
                0,            // ephemeral client port
                1024, 100,
                true,                        // clusterEnabled
                "node-1",                    // nodeId
                0,                           // ephemeral cluster port
                List.of(),                   // no outbound peers for node1
                CLUSTER_SECRET,
                HEARTBEAT_MS
        );
        node1 = new ClusterNode(config1);
        node1.start();

        Assertions.assertTrue(node1.getClientPort() > 0, "Node1 must have a client port");
        Assertions.assertTrue(node1.getClusterPort() > 0, "Node1 must have a cluster port");

        // node2 connects outbound to node1's cluster port.
        String node1ClusterAddress = HOST + ":" + node1.getClusterPort();
        AvenueConfig config2 = new AvenueConfig(
                PACKET_SIZE,
                true,
                SECRET, TOKEN,
                0,            // ephemeral client port
                1024, 100,
                true,                        // clusterEnabled
                "node-2",                    // nodeId
                0,                           // ephemeral cluster port
                List.of(node1ClusterAddress),// outbound peer: node1
                CLUSTER_SECRET,
                HEARTBEAT_MS
        );
        node2 = new ClusterNode(config2);
        node2.start();

        Assertions.assertTrue(node2.getClientPort() > 0, "Node2 must have a client port");

        // Wait deterministically until node2 has established the peer link to node1.
        boolean linked2 = node2.awaitAnyPeer(TIMEOUT, UNIT);
        Assertions.assertTrue(linked2, "Node2 must establish a peer link to Node1 within timeout");

        // Node1 may need a moment to register the incoming peer on its side.
        long deadline = System.nanoTime() + UNIT.toNanos(TIMEOUT);
        while (node1.getActivePeerCount() == 0 && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        Assertions.assertTrue(node1.getActivePeerCount() > 0,
                "Node1 must see Node2 as an active peer after incoming connection");
    }

    @AfterEach
    void stopNodes() {
        if (node2 != null) node2.stop();
        if (node1 != null) node1.stop();
    }

    // -------------------------------------------------------------------------
    // Main cross-node pub/sub test (run 3x for determinism check)
    // -------------------------------------------------------------------------

    @RepeatedTest(3)
    void subscriber_on_node1_receives_message_published_on_node2() throws Exception {
        // clientA connects to node1 and subscribes.
        // clientB connects to node2 and publishes.
        try (TestClient clientA = new TestClient(HOST, node1.getClientPort(), PACKET_SIZE);
             TestClient clientB = new TestClient(HOST, node2.getClientPort(), PACKET_SIZE)) {

            clientA.authenticate(SECRET, TIMEOUT, UNIT);
            clientB.authenticate(SECRET, TIMEOUT, UNIT);

            // Subscribe on node1. The ack guarantees the subscription is registered BEFORE
            // we publish on node2, eliminating any subscribe/publish race.
            clientA.subscribe("global", TOKEN, TIMEOUT, UNIT);

            // Publish on node2. Node2 will forward to node1 via the cluster link. Node1
            // will deliver to clientA.
            clientB.publish("global", "hello-from-node2", "publisher-b", TOKEN);

            JSONObject received = clientA.awaitPacket("PublishMessageOutboundPacket", TIMEOUT, UNIT);
            Assertions.assertNotNull(received, "Subscriber on node1 must receive the message published on node2");
            Assertions.assertEquals("global",
                    received.getJSONObject("header").getString("topic"),
                    "Topic must be 'global'");
            Assertions.assertEquals("publisher-b",
                    received.getJSONObject("header").getString("source"),
                    "Source must be preserved across the cluster link");
            Assertions.assertEquals("hello-from-node2",
                    received.getJSONObject("body").getString("data"),
                    "Data must be preserved across the cluster link");
        }
    }

    @Test
    void subscriber_on_node2_receives_message_published_on_node1() throws Exception {
        // Reverse direction: publish on node1, subscriber on node2. Node1 does NOT have an
        // outbound peer connector (node2 initiated the connection), but the cluster link is
        // bidirectional — node1 can still forward to node2 via the accepted incoming link.
        try (TestClient clientA = new TestClient(HOST, node1.getClientPort(), PACKET_SIZE);
             TestClient clientB = new TestClient(HOST, node2.getClientPort(), PACKET_SIZE)) {

            clientA.authenticate(SECRET, TIMEOUT, UNIT);
            clientB.authenticate(SECRET, TIMEOUT, UNIT);

            // Subscribe on node2.
            clientB.subscribe("reverse", TOKEN, TIMEOUT, UNIT);

            // Publish on node1.
            clientA.publish("reverse", "hello-from-node1", "publisher-a", TOKEN);

            JSONObject received = clientB.awaitPacket("PublishMessageOutboundPacket", TIMEOUT, UNIT);
            Assertions.assertNotNull(received, "Subscriber on node2 must receive the message published on node1");
            Assertions.assertEquals("hello-from-node1",
                    received.getJSONObject("body").getString("data"));
        }
    }

    @Test
    void cross_node_message_is_delivered_exactly_once_to_subscriber() throws Exception {
        // Sanity: deduplication must prevent double delivery even if somehow a message
        // arrives twice (simulated here by asserting we receive exactly one packet then
        // no second one arrives within a short extra window).
        try (TestClient clientA = new TestClient(HOST, node1.getClientPort(), PACKET_SIZE);
             TestClient clientB = new TestClient(HOST, node2.getClientPort(), PACKET_SIZE)) {

            clientA.authenticate(SECRET, TIMEOUT, UNIT);
            clientB.authenticate(SECRET, TIMEOUT, UNIT);

            clientA.subscribe("dedup", TOKEN, TIMEOUT, UNIT);
            clientB.publish("dedup", "once", "publisher-b", TOKEN);

            // First delivery must arrive.
            JSONObject first = clientA.awaitPacket("PublishMessageOutboundPacket", TIMEOUT, UNIT);
            Assertions.assertNotNull(first, "Must receive the message at least once");

            // Second delivery must NOT arrive within a short window (no duplicate).
            boolean noDuplicate = clientA.expectNoPacket("PublishMessageOutboundPacket", 1, TimeUnit.SECONDS);
            Assertions.assertTrue(noDuplicate, "Message must be delivered exactly once — no duplicate");
        }
    }

    @Test
    void local_publish_without_cross_node_subscriber_does_not_duplicate() throws Exception {
        // A subscriber on node2 subscribes on node2. Publisher also on node2.
        // The message is delivered locally. It is also forwarded to node1 (which has no
        // subscriber), but must NOT come back to node2 (single-hop rule).
        try (TestClient subOnNode2 = new TestClient(HOST, node2.getClientPort(), PACKET_SIZE);
             TestClient pubOnNode2 = new TestClient(HOST, node2.getClientPort(), PACKET_SIZE)) {

            subOnNode2.authenticate(SECRET, TIMEOUT, UNIT);
            pubOnNode2.authenticate(SECRET, TIMEOUT, UNIT);

            subOnNode2.subscribe("local-only", TOKEN, TIMEOUT, UNIT);
            pubOnNode2.publish("local-only", "local-data", "pub-local", TOKEN);

            // The subscriber receives the message once (locally).
            JSONObject first = subOnNode2.awaitPacket("PublishMessageOutboundPacket", TIMEOUT, UNIT);
            Assertions.assertNotNull(first, "Local subscriber must receive the message");

            // No duplicate from the cluster path returning the message.
            boolean noDuplicate = subOnNode2.expectNoPacket("PublishMessageOutboundPacket", 1, TimeUnit.SECONDS);
            Assertions.assertTrue(noDuplicate, "No duplicate via cluster loop-back");
        }
    }

    @Test
    void existing_single_node_scenario_unaffected_by_cluster() throws Exception {
        // Both subscriber and publisher on the SAME node (node1). Cluster must not interfere.
        try (TestClient subscriber = new TestClient(HOST, node1.getClientPort(), PACKET_SIZE);
             TestClient publisher = new TestClient(HOST, node1.getClientPort(), PACKET_SIZE)) {

            subscriber.authenticate(SECRET, TIMEOUT, UNIT);
            publisher.authenticate(SECRET, TIMEOUT, UNIT);

            subscriber.subscribe("local-test", TOKEN, TIMEOUT, UNIT);
            publisher.publish("local-test", "local-value", "pub-1", TOKEN);

            JSONObject received = subscriber.awaitPacket("PublishMessageOutboundPacket", TIMEOUT, UNIT);
            Assertions.assertNotNull(received, "Local pub-sub must still work in cluster mode");
            Assertions.assertEquals("local-value", received.getJSONObject("body").getString("data"));
        }
    }
}
