package de.kyle.avenue.integration.cluster;

import de.kyle.avenue.cluster.ClusterNode;
import de.kyle.avenue.cluster.ReplayBuffer;
import de.kyle.avenue.cluster.membership.MemberState;
import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.config.ClusterTuning;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * Reusable multi-node cluster test harness (Phase F).
 * <p>
 * Spins up {@code N} in-process {@link ClusterNode}s on ephemeral ports, wires their SWIM seeds and
 * exposes <b>polling-based</b> convergence helpers ({@link #awaitConvergence}, {@link #awaitMembership})
 * — never fixed sleeps — plus fault-injection helpers ({@link #partition}, {@link #heal},
 * {@link #slowPeer}, {@link #crash}, {@link #restart}) built on the existing
 * {@code dropPeer}/{@code blockPeer}/{@code unblockPeer} test seams on {@link ClusterNode}.
 * <p>
 * It is intentionally NOT forced onto the existing cluster tests (those keep their own bespoke
 * setup); it is provided for new tests and as the single place where node bring-up, fast SWIM tuning
 * and fault helpers live. {@link #close()} stops every node in reverse start order and is safe to
 * call from an {@code @AfterEach}.
 */
public final class ClusterTestHarness implements AutoCloseable {

    public static final String HOST = "127.0.0.1";
    public static final String SECRET = "harness-secret";
    public static final String TOKEN = "harness-token";
    public static final String CLUSTER_SECRET = "harness-cluster-secret";
    public static final int PACKET_SIZE = 65536;
    private static final long HEARTBEAT_MS = 500L;

    private final List<ClusterNode> nodes = new ArrayList<>();
    private final List<String> nodeIds = new ArrayList<>();

    /** Fast SWIM tuning so failure detection / convergence completes well within test timeouts. */
    public static ClusterTuning fastSwimTuning() {
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
                1_048_576);
    }

    private static AvenueConfig config(String nodeId, List<String> seeds) {
        return new AvenueConfig(
                PACKET_SIZE, true, SECRET, TOKEN,
                0, 8192, 100,
                true, nodeId, 0, seeds, CLUSTER_SECRET, HEARTBEAT_MS,
                0L, 10_000, null, 0L,
                false, "", "",
                false, "", "", "", "",
                fastSwimTuning());
    }

    /**
     * Starts {@code count} nodes named {@code node-1..node-N}. The first node has no seed; every other
     * node uses the first node as its single SWIM seed, from which the LinkSupervisor forms a full mesh.
     *
     * @return this harness, for chaining
     */
    public ClusterTestHarness startCluster(int count) {
        ClusterNode first = startNode("node-1", List.of());
        for (int i = 2; i <= count; i++) {
            startNode("node-" + i, List.of(addr(first)));
        }
        return this;
    }

    /** Starts a single named node with the given seed addresses and tracks it for teardown. */
    public ClusterNode startNode(String nodeId, List<String> seeds) {
        ClusterNode node = new ClusterNode(config(nodeId, seeds));
        node.start();
        if (node.getClusterPort() <= 0) {
            throw new IllegalStateException(nodeId + " did not bind a cluster port");
        }
        nodes.add(node);
        nodeIds.add(nodeId);
        return node;
    }

    public ClusterNode node(int index) {
        return nodes.get(index);
    }

    public List<ClusterNode> nodes() {
        return List.copyOf(nodes);
    }

    public static String addr(ClusterNode node) {
        return HOST + ":" + node.getClusterPort();
    }

    // -------------------------------------------------------------------------
    // Convergence (polling only — no fixed sleeps)
    // -------------------------------------------------------------------------

    /** Polls until {@code condition} holds or the deadline elapses; returns whether it held. */
    public static boolean awaitTrue(BooleanSupplier condition, long timeout, TimeUnit unit)
            throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(20);
        }
        return condition.getAsBoolean();
    }

    /** Blocks until {@code node} sees at least {@code expectedAlive} ALIVE remote members. */
    public boolean awaitMembership(ClusterNode node, int expectedAlive, long timeout, TimeUnit unit)
            throws InterruptedException {
        return awaitTrue(() -> node.getMemberCount() >= expectedAlive, timeout, unit);
    }

    /**
     * Blocks until EVERY node sees {@code (clusterSize - 1)} ALIVE peers, i.e. the cluster has fully
     * converged into a mesh. Returns whether convergence was reached within the timeout.
     */
    public boolean awaitConvergence(long timeout, TimeUnit unit) throws InterruptedException {
        int expected = nodes.size() - 1;
        return awaitTrue(() -> nodes.stream().allMatch(n -> n.getMemberCount() >= expected), timeout, unit);
    }

    /** Polls until {@code observer} records {@code subject} in {@code state} (or any of {@code states}). */
    public boolean awaitState(ClusterNode observer, String subjectNodeId, long timeout, TimeUnit unit,
                              MemberState... states) throws InterruptedException {
        return awaitTrue(() -> {
            var m = observer.getMemberRegistry().get(subjectNodeId);
            if (m == null) {
                return false;
            }
            for (MemberState s : states) {
                if (m.getState() == s) {
                    return true;
                }
            }
            return false;
        }, timeout, unit);
    }

    // -------------------------------------------------------------------------
    // Fault injection (built on the dropPeer/blockPeer/unblockPeer seams)
    // -------------------------------------------------------------------------

    /**
     * Symmetrically partitions nodes {@code a} and {@code b}: each blocks the other so neither direct
     * nor reconnect traffic flows between them, simulating a network split that survives reconnect
     * attempts.
     */
    public void partition(ClusterNode a, String aId, ClusterNode b, String bId) {
        a.blockPeer(bId);
        b.blockPeer(aId);
    }

    /** Heals a previous {@link #partition}: both sides unblock and the LinkSupervisor reconnects. */
    public void heal(ClusterNode a, String aId, ClusterNode b, String bId) {
        a.unblockPeer(bId);
        b.unblockPeer(aId);
    }

    /**
     * Simulates a transient "slow peer" by dropping the current link without blocking: the owning
     * connect loop immediately rebuilds it and backfills from the surviving replay buffer. Useful for
     * exercising reconnect + backfill without a permanent partition.
     */
    public void slowPeer(ClusterNode observer, String peerId) {
        observer.dropPeer(peerId);
    }

    /**
     * Simulates a crash of {@code victim} from the cluster's perspective by blocking it on every other
     * node (and blocking every other node on {@code victim}), so it becomes unreachable for direct and
     * indirect probes alike and converges to DEAD.
     */
    public void crash(ClusterNode victim, String victimId) {
        for (int i = 0; i < nodes.size(); i++) {
            ClusterNode other = nodes.get(i);
            if (other == victim) {
                continue;
            }
            String otherId = nodeIds.get(i);
            other.blockPeer(victimId);
            victim.blockPeer(otherId);
        }
    }

    /**
     * Restarts {@code victim} with the SAME node id: stops the old node and starts a fresh one seeded
     * by {@code seed}. Returns the new {@link ClusterNode}. The old instance is removed from teardown
     * tracking and the new one added.
     */
    public ClusterNode restart(ClusterNode victim, String victimId, ClusterNode seed) {
        int idx = nodes.indexOf(victim);
        if (idx >= 0) {
            try {
                victim.stop();
            } catch (Exception ignored) {
            }
            nodes.remove(idx);
            nodeIds.remove(idx);
        }
        return startNode(victimId, List.of(addr(seed)));
    }

    // -------------------------------------------------------------------------
    // Teardown
    // -------------------------------------------------------------------------

    @Override
    public void close() {
        for (int i = nodes.size() - 1; i >= 0; i--) {
            try {
                nodes.get(i).stop();
            } catch (Exception ignored) {
            }
        }
        nodes.clear();
        nodeIds.clear();
    }
}
