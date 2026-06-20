package de.kyle.avenue.cluster;

import de.kyle.avenue.SingleNodeServer;
import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Cluster-aware Avenue node.
 * <p>
 * Wires together:
 * <ul>
 *   <li>A <em>shared</em> {@link TopicSubscriptionHandler} that is the single source of truth
 *       for all subscriptions on this node.</li>
 *   <li>A {@link ClusterManager} that uses the shared handler to deliver incoming cluster
 *       messages to local subscribers, and implements {@link ClusterForwarder} to fan out
 *       local publishes to all connected peers.</li>
 *   <li>A {@link SingleNodeServer} that accepts client connections and uses the same shared
 *       handler plus the {@link ClusterManager} as its {@link ClusterForwarder}.</li>
 * </ul>
 * The cluster transport is started <em>before</em> the client-facing server so that peer
 * links are ready as soon as clients can connect and publish.
 * <p>
 * Lifecycle is identical to {@link SingleNodeServer}: construct → {@link #start()} →
 * {@link #stop()}. Integration tests use {@link #awaitAnyPeer} to synchronize before
 * asserting cross-node delivery.
 */
public class ClusterNode {

    private static final Logger log = LoggerFactory.getLogger(ClusterNode.class);

    private final ClusterManager clusterManager;
    private final SingleNodeServer singleNodeServer;

    /**
     * Builds the cluster node. Does NOT start any I/O. Call {@link #start()} to bind sockets.
     *
     * @param config the full configuration including cluster settings
     */
    public ClusterNode(AvenueConfig config) {
        // Shared subscription table: both ClusterManager (for incoming cluster delivery)
        // and SingleNodeServer (for local client delivery) operate on the same instance.
        TopicSubscriptionHandler sharedHandler = new TopicSubscriptionHandler();

        this.clusterManager = new ClusterManager(config, sharedHandler);

        // Use the cluster-aware SingleNodeServer constructor that accepts an external
        // TopicSubscriptionHandler and a ClusterForwarder. The ClusterManager IS the forwarder.
        this.singleNodeServer = new SingleNodeServer(config, sharedHandler, clusterManager);

        // Fold the cluster counters into the shared metrics snapshot log (null-safe; only set
        // when clustering is active).
        this.singleNodeServer.getMetrics().setClusterMetrics(clusterManager.getClusterMetrics());
    }

    /**
     * Starts the cluster manager (binds cluster port, initiates outbound peer connections)
     * and then the single-node server (binds client port). Both return once their sockets
     * are bound.
     */
    public void start() {
        log.info("Starting cluster node (nodeId={})", clusterManager.getConfig().getNodeId());
        clusterManager.start();
        singleNodeServer.start();
    }

    /** Stops the single-node server and the cluster manager. */
    public void stop() {
        singleNodeServer.stop();
        clusterManager.stop();
    }

    /**
     * Returns the actual bound client-facing TCP port, or {@code -1} if not yet bound.
     */
    public int getClientPort() {
        return singleNodeServer.getPort();
    }

    /**
     * Returns the actual bound cluster TCP port, or {@code -1} if not yet bound.
     */
    public int getClusterPort() {
        return clusterManager.getClusterPort();
    }

    /**
     * Blocks until at least one peer link is established, or the timeout elapses.
     * Designed for test synchronization — not used on the hot path.
     *
     * @return {@code true} if a peer is connected within the timeout
     */
    public boolean awaitAnyPeer(long timeout, TimeUnit unit) throws InterruptedException {
        return clusterManager.awaitAnyPeer(timeout, unit);
    }

    /** Returns the number of currently active peer links. */
    public int getActivePeerCount() {
        return clusterManager.getActivePeerCount();
    }
}
