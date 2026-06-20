package de.kyle.avenue;

import de.kyle.avenue.cluster.ClusterNode;
import de.kyle.avenue.config.AvenueConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Production entry point.
 * <p>
 * Reads the configuration from the config folder / environment and starts either:
 * <ul>
 *   <li>A {@link ClusterNode} when {@code cluster.enabled=true} is set in the config.</li>
 *   <li>A plain {@link SingleNodeServer} otherwise (backwards-compatible default).</li>
 * </ul>
 */
public class AvenueApplication {

    private static final Logger log = LoggerFactory.getLogger(AvenueApplication.class);

    public static void main(String[] args) {
        AvenueConfig config;
        try {
            config = new AvenueConfig();
        } catch (IOException e) {
            log.error("Failed to load configuration", e);
            System.exit(1);
            return;
        }

        if (config.isClusterEnabled()) {
            log.info("Cluster mode enabled (nodeId={})", config.getNodeId());
            ClusterNode node = new ClusterNode(config);
            node.start();
            // Block the main thread until shutdown.
            Runtime.getRuntime().addShutdownHook(new Thread(node::stop));
            try {
                Thread.currentThread().join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } else {
            log.info("Single-node mode");
            // The no-arg constructor of SingleNodeServer loads config itself, starts, and blocks.
            new SingleNodeServer();
        }
    }
}
