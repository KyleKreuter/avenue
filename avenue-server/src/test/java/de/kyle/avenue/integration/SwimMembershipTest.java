package de.kyle.avenue.integration;

import de.kyle.avenue.cluster.ClusterNode;
import de.kyle.avenue.cluster.membership.Member;
import de.kyle.avenue.cluster.membership.MemberState;
import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.config.ClusterTuning;
import de.kyle.avenue.cluster.ReplayBuffer;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * Phase E — SWIM gossip membership integration tests.
 * <p>
 * Each test spins up N in-process {@link ClusterNode}s on ephemeral ports. node2/node3 use node1 as
 * the static seed for the initial SWIM join; from there the membership-driven LinkSupervisor connects
 * the nodes into a full mesh and SWIM gossip keeps every view converged. All synchronization is via
 * bounded polling helpers ({@link #awaitMembership}, {@link #awaitTrue}) — never fixed sleeps.
 * <p>
 * Fast SWIM tuning is used so failure detection / convergence happens in well under the test timeout.
 */
class SwimMembershipTest {

    private static final String HOST = "127.0.0.1";
    private static final String SECRET = "int-test-secret";
    private static final String TOKEN = "int-test-token";
    private static final String CLUSTER_SECRET = "cluster-secret-42";
    private static final int PACKET_SIZE = 65536;
    private static final long TIMEOUT = 20;
    private static final TimeUnit UNIT = TimeUnit.SECONDS;
    private static final long HEARTBEAT_MS = 500L;

    private final List<ClusterNode> nodes = new ArrayList<>();

    @AfterEach
    void stopNodes() {
        // Stop in reverse start order; ignore individual failures so one bad node cannot leak others.
        for (int i = nodes.size() - 1; i >= 0; i--) {
            try {
                nodes.get(i).stop();
            } catch (Exception ignored) {
            }
        }
        nodes.clear();
    }

    private static ClusterTuning fastSwimTuning() {
        return new ClusterTuning(
                ClusterTuning.DEFAULT_REPLAY_CAPACITY,
                ReplayBuffer.BackpressurePolicy.BLOCK,
                ClusterTuning.DEFAULT_REPLAY_OFFER_TIMEOUT_MS,
                ClusterTuning.DEFAULT_ACK_INTERVAL_MS,
                false,
                ClusterTuning.DEFAULT_ORIGIN_EXPIRY_MS,
                ClusterTuning.DEFAULT_INTEREST_SYNC_INTERVAL_MS,
                ClusterTuning.DEFAULT_INTEREST_BROADCAST_GRACE_MS,
                200L,   // swimProbeIntervalMs
                100L,   // swimProbeTimeoutMs
                2,      // swimIndirectProbeCount
                500L,   // swimSuspicionTimeoutMs
                2,      // swimGossipFanout
                2_000L, // swimDeadMemberTimeoutMs
                "",     // advertisedHost
                1_048_576 // clusterPacketMaxSize
        );
    }

    private AvenueConfig config(String nodeId, List<String> peers) {
        return new AvenueConfig(
                PACKET_SIZE, true, SECRET, TOKEN,
                0, 8192, 100,
                true, nodeId, 0, peers, CLUSTER_SECRET, HEARTBEAT_MS,
                0L, 10_000, null, 0L,
                false, "", "",
                false, "", "", "", "",
                fastSwimTuning());
    }

    private ClusterNode startNode(String nodeId, List<String> seeds) {
        ClusterNode node = new ClusterNode(config(nodeId, seeds));
        node.start();
        Assertions.assertTrue(node.getClusterPort() > 0, nodeId + " must bind a cluster port");
        nodes.add(node);
        return node;
    }

    private static String addr(ClusterNode node) {
        return HOST + ":" + node.getClusterPort();
    }

    private void awaitTrue(BooleanSupplier condition, String message) throws InterruptedException {
        awaitTrue(condition, message, TIMEOUT, UNIT);
    }

    private void awaitTrue(BooleanSupplier condition, String message, long timeout, TimeUnit unit)
            throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        Assertions.assertTrue(condition.getAsBoolean(), message);
    }

    private void awaitMembership(ClusterNode node, int expectedAlive, String label)
            throws InterruptedException {
        awaitTrue(() -> node.getMemberCount() >= expectedAlive,
                label + " must see " + expectedAlive + " alive members (saw " + node.getMemberCount() + ")");
    }

    private MemberState stateOf(ClusterNode observer, String subjectNodeId) {
        Member m = observer.getMemberRegistry().get(subjectNodeId);
        return m == null ? null : m.getState();
    }

    // -------------------------------------------------------------------------
    // 1) three_nodes_converge
    // -------------------------------------------------------------------------

    @Test
    void three_nodes_converge() throws Exception {
        ClusterNode node1 = startNode("node-1", List.of());
        ClusterNode node2 = startNode("node-2", List.of(addr(node1)));
        ClusterNode node3 = startNode("node-3", List.of(addr(node1)));

        awaitMembership(node1, 2, "node1");
        awaitMembership(node2, 2, "node2");
        awaitMembership(node3, 2, "node3");
    }

    // -------------------------------------------------------------------------
    // 2) dynamic_join — a newcomer is discovered and can publish cross-node
    // -------------------------------------------------------------------------

    @Test
    void dynamic_join() throws Exception {
        ClusterNode node1 = startNode("node-1", List.of());
        ClusterNode node2 = startNode("node-2", List.of(addr(node1)));

        awaitMembership(node1, 1, "node1");
        awaitMembership(node2, 1, "node2");

        // A third node joins via node1 only.
        ClusterNode node3 = startNode("node-3", List.of(addr(node1)));

        awaitTrue(() -> stateOf(node1, "node-3") == MemberState.ALIVE, "node1 must see node3 ALIVE");
        awaitTrue(() -> stateOf(node2, "node-3") == MemberState.ALIVE, "node2 must see node3 ALIVE");

        // Cross-node publish from the newcomer to a subscriber on node1.
        try (TestClient sub = new TestClient(HOST, node1.getClientPort(), PACKET_SIZE);
             TestClient pub = new TestClient(HOST, node3.getClientPort(), PACKET_SIZE)) {
            sub.authenticate(SECRET, TIMEOUT, UNIT);
            pub.authenticate(SECRET, TIMEOUT, UNIT);
            sub.subscribe("newcomer", TOKEN, TIMEOUT, UNIT);
            awaitTrue(() -> node3.knowsInterest("node-1", "newcomer"),
                    "node3 must learn node1's interest before publishing");

            pub.publish("newcomer", "from-node3", "pub", TOKEN);
            JSONObject received = sub.awaitPacket("PublishMessageOutboundPacket", TIMEOUT, UNIT);
            Assertions.assertNotNull(received, "subscriber on node1 must receive node3's publish");
            Assertions.assertEquals("from-node3", received.getJSONObject("body").getString("data"));
        }
    }

    // -------------------------------------------------------------------------
    // 3) graceful_leave — converges fast via the LeavePacket
    // -------------------------------------------------------------------------

    @Test
    void graceful_leave() throws Exception {
        ClusterNode node1 = startNode("node-1", List.of());
        ClusterNode node2 = startNode("node-2", List.of(addr(node1)));
        ClusterNode node3 = startNode("node-3", List.of(addr(node1)));

        awaitMembership(node1, 2, "node1");
        awaitMembership(node2, 2, "node2");
        awaitMembership(node3, 2, "node3");

        // node3 leaves gracefully -> a LeavePacket should make node1 & node2 converge to LEFT/DEAD
        // well within twice the suspicion timeout (and typically near-instantly).
        node3.stop();
        nodes.remove(node3);

        long fastWindowMs = fastSwimTuning().swimSuspicionTimeoutMs() * 2;
        awaitTrue(() -> {
                    MemberState s = stateOf(node1, "node-3");
                    return s == MemberState.LEFT || s == MemberState.DEAD;
                }, "node1 must see node3 LEFT/DEAD quickly after a graceful leave",
                fastWindowMs, TimeUnit.MILLISECONDS);
        awaitTrue(() -> {
                    MemberState s = stateOf(node2, "node-3");
                    return s == MemberState.LEFT || s == MemberState.DEAD;
                }, "node2 must see node3 LEFT/DEAD quickly after a graceful leave",
                fastWindowMs, TimeUnit.MILLISECONDS);

        // Remaining two see one alive peer each.
        awaitMembership(node1, 1, "node1 after leave");
        awaitMembership(node2, 1, "node2 after leave");
    }

    // -------------------------------------------------------------------------
    // 4) crash_detection — block a node out of view; SUSPECT then DEAD
    // -------------------------------------------------------------------------

    @Test
    void crash_detection() throws Exception {
        ClusterNode node1 = startNode("node-1", List.of());
        ClusterNode node2 = startNode("node-2", List.of(addr(node1)));
        ClusterNode node3 = startNode("node-3", List.of(addr(node1)));

        awaitMembership(node1, 2, "node1");
        awaitMembership(node2, 2, "node2");
        awaitMembership(node3, 2, "node3");

        // Simulate a node3 crash from the perspective of node1 AND node2: block + drop the links so
        // node3 is unreachable for direct and indirect probes alike.
        node1.blockPeer("node-3");
        node2.blockPeer("node-3");
        node3.blockPeer("node-1");
        node3.blockPeer("node-2");

        // node1 and node2 must converge on node3 being DEAD, and end up with a single alive peer.
        awaitTrue(() -> stateOf(node1, "node-3") == MemberState.DEAD, "node1 must declare node3 DEAD");
        awaitTrue(() -> stateOf(node2, "node-3") == MemberState.DEAD, "node2 must declare node3 DEAD");
        awaitTrue(() -> node1.getMemberCount() == 1, "node1 converges to 1 alive");
        awaitTrue(() -> node2.getMemberCount() == 1, "node2 converges to 1 alive");
    }

    // -------------------------------------------------------------------------
    // 5) restart_same_nodeid — no ghost, no flapping
    // -------------------------------------------------------------------------

    @RepeatedTest(3)
    void restart_same_nodeid() throws Exception {
        ClusterNode node1 = startNode("node-1", List.of());
        ClusterNode node2 = startNode("node-2", List.of(addr(node1)));
        ClusterNode node3 = startNode("node-3", List.of(addr(node1)));

        awaitMembership(node1, 2, "node1");
        awaitMembership(node3, 2, "node3");

        // node3 leaves and a fresh process with the SAME node id rejoins via node1.
        node3.stop();
        nodes.remove(node3);
        awaitTrue(() -> {
            MemberState s = stateOf(node1, "node-3");
            return s == MemberState.LEFT || s == MemberState.DEAD;
        }, "node1 must observe the old node3 leaving");

        ClusterNode node3b = startNode("node-3", List.of(addr(node1)));

        // The restarted node3 must become ALIVE again on node1 (its bumped incarnation beats any stale
        // record) and node3b must see the full cluster — i.e. no ghost, convergence restored.
        awaitTrue(() -> stateOf(node1, "node-3") == MemberState.ALIVE,
                "restarted node3 must be ALIVE again on node1 (no sticky-dead ghost)");
        awaitMembership(node3b, 2, "restarted node3");
        awaitMembership(node1, 2, "node1 after restart");
    }

    // -------------------------------------------------------------------------
    // 6) partition_refute — a suspected-but-alive node refutes
    // -------------------------------------------------------------------------

    @Test
    void partition_refute() throws Exception {
        ClusterNode node1 = startNode("node-1", List.of());
        ClusterNode node2 = startNode("node-2", List.of(addr(node1)));
        ClusterNode node3 = startNode("node-3", List.of(addr(node1)));

        awaitMembership(node1, 2, "node1");
        awaitMembership(node2, 2, "node2");
        awaitMembership(node3, 2, "node3");

        // Cut ONLY the node1<->node3 link. node3 stays alive and its link to node2 is intact, so node2
        // can vouch for node3 and node3 can refute via its incarnation. node1 will briefly suspect
        // node3 (indirect probe via node2 may rescue it, or it flips SUSPECT then back to ALIVE).
        node1.blockPeer("node-3");
        node3.blockPeer("node-1");

        // Eventually node1 must see node3 ALIVE again (refute / indirect vouch), NOT stuck DEAD.
        awaitTrue(() -> stateOf(node1, "node-3") == MemberState.ALIVE,
                "node1 must converge back to node3 ALIVE (refuted, still reachable via node2)");
        Assertions.assertNotEquals(MemberState.DEAD, stateOf(node1, "node-3"),
                "a still-alive, reachable-via-node2 node must not end up DEAD");
    }
}
