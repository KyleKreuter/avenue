package de.kyle.avenue.integration;

import de.kyle.avenue.cluster.ClusterNode;
import de.kyle.avenue.config.AvenueConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Integration test for the HMAC challenge-response cluster node authentication (Phase B).
 * <p>
 * Two in-process {@link ClusterNode}s are wired as mutual peers on ephemeral client and cluster
 * ports. The test verifies both the happy path and the rejection path of the handshake:
 * <ol>
 *   <li><b>Matching secret</b> — both nodes share the same {@code cluster.secret}; the peer link
 *       comes up ({@link ClusterNode#awaitAnyPeer} returns {@code true} on both sides).</li>
 *   <li><b>Mismatched secret</b> — the connecting node holds a different {@code cluster.secret};
 *       no peer link is ever established and the acceptor records a handshake auth failure.</li>
 * </ol>
 * Determinism is achieved purely via bounded polling ({@code awaitAnyPeer} / a deadline loop on the
 * failure counter); no fixed {@code Thread.sleep} is used as a synchronization primitive.
 */
class ClusterAuthTest {

    private static final String HOST = "127.0.0.1";
    private static final String SECRET = "int-test-secret";
    private static final String TOKEN = "int-test-token";
    private static final String CLUSTER_SECRET = "cluster-secret-correct";
    private static final String WRONG_CLUSTER_SECRET = "cluster-secret-WRONG";
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

    private AvenueConfig clusterConfig(String nodeId, List<String> peers, String clusterSecret) {
        return new AvenueConfig(
                PACKET_SIZE,
                true,
                SECRET, TOKEN,
                0,            // ephemeral client port
                1024, 100,
                true,         // clusterEnabled
                nodeId,
                0,            // ephemeral cluster port
                peers,
                clusterSecret,
                HEARTBEAT_MS
        );
    }

    @Test
    void matching_secret_establishes_peer_link() throws Exception {
        node1 = new ClusterNode(clusterConfig("node-1", List.of(), CLUSTER_SECRET));
        node1.start();
        Assertions.assertTrue(node1.getClusterPort() > 0, "Node1 must bind a cluster port");

        String node1Address = HOST + ":" + node1.getClusterPort();
        node2 = new ClusterNode(clusterConfig("node-2", List.of(node1Address), CLUSTER_SECRET));
        node2.start();

        // The initiator (node2) must establish the authenticated link to node1.
        Assertions.assertTrue(node2.awaitAnyPeer(TIMEOUT, UNIT),
                "Node2 must establish a peer link with the correct shared secret");

        // node1 must register the inbound peer as well.
        long deadline = System.nanoTime() + UNIT.toNanos(TIMEOUT);
        while (node1.getActivePeerCount() == 0 && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        Assertions.assertTrue(node1.getActivePeerCount() > 0,
                "Node1 must accept the authenticated inbound peer");

        // No handshake should have been rejected on either side.
        Assertions.assertEquals(0,
                node1.getClusterMetrics().getHandshakeAuthFailures(),
                "Node1 must not record an auth failure on a matching secret");
    }

    @Test
    void wrong_secret_prevents_peer_link_and_counts_auth_failure() throws Exception {
        node1 = new ClusterNode(clusterConfig("node-1", List.of(), CLUSTER_SECRET));
        node1.start();
        Assertions.assertTrue(node1.getClusterPort() > 0, "Node1 must bind a cluster port");

        // node2 connects outbound with the WRONG secret -> handshake must fail.
        String node1Address = HOST + ":" + node1.getClusterPort();
        node2 = new ClusterNode(clusterConfig("node-2", List.of(node1Address), WRONG_CLUSTER_SECRET));
        node2.start();

        // Wait until a handshake auth failure has been recorded. With the wrong secret the
        // INITIATOR (node2) verifies the acceptor's proof first and rejects it, so node2's
        // counter is the one that increments deterministically; the acceptor (node1) simply
        // hits its handshake read-timeout. We assert on the cluster-wide sum so the test does
        // not depend on which side detects the mismatch first.
        long deadline = System.nanoTime() + UNIT.toNanos(TIMEOUT);
        long failures = 0;
        while (System.nanoTime() < deadline) {
            failures = node1.getClusterMetrics().getHandshakeAuthFailures()
                    + node2.getClusterMetrics().getHandshakeAuthFailures();
            if (failures > 0) {
                break;
            }
            Thread.sleep(20);
        }

        Assertions.assertTrue(failures > 0,
                "A handshake auth failure must be recorded for the wrong secret");

        // The link must never come up on either side.
        Assertions.assertEquals(0, node1.getActivePeerCount(),
                "Node1 must have no active peer link after a failed handshake");
        Assertions.assertFalse(node2.awaitAnyPeer(500, TimeUnit.MILLISECONDS),
                "Node2 must not establish a peer link with the wrong secret");
    }
}
