package de.kyle.avenue.cluster;

/**
 * Functional interface for forwarding a local publish event to cluster peers.
 * <p>
 * The implementation (provided by {@link ClusterManager}) is responsible for stamping the
 * {@code originNodeId} and building the {@link de.kyle.avenue.packet.cluster.ClusterPublishPacket}.
 * The caller (the publish handler) only supplies the message fields it already knows.
 * <p>
 * The NOOP singleton is used when clustering is disabled, so the publish handler never
 * needs to branch on a null check.
 */
@FunctionalInterface
public interface ClusterForwarder {

    /** No-op forwarder used when clustering is disabled. */
    ClusterForwarder NOOP = (topic, source, data, messageId) -> { };

    /**
     * Forwards a locally-received publish to all connected cluster peers.
     * Implementations MUST be non-blocking; any queueing or I/O is done asynchronously.
     *
     * @param topic     normalized topic key
     * @param source    original client-supplied source identifier
     * @param data      message payload
     * @param messageId per-message UUID string for deduplication
     */
    void forward(String topic, String source, String data, String messageId);
}
