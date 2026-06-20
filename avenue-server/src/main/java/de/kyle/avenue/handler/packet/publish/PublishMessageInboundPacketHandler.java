package de.kyle.avenue.handler.packet.publish;

import de.kyle.avenue.cluster.ClusterForwarder;
import de.kyle.avenue.handler.client.ClientConnectionHandler;
import de.kyle.avenue.handler.packet.PacketHandler;
import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
import de.kyle.avenue.metrics.AvenueMetrics;
import de.kyle.avenue.proto.ClientEnvelope;
import de.kyle.avenue.proto.PublishInbound;
import de.kyle.avenue.serialization.ClientEnvelopes;

import java.util.concurrent.ExecutorService;

/**
 * Handles a {@link PublishInbound} message from a LOCAL client.
 * <p>
 * On receipt, this handler:
 * <ol>
 *   <li>Delivers the message to all local subscribers via
 *       {@link TopicSubscriptionHandler#deliverPacketToSubscribers} (async, unchanged).</li>
 *   <li>Forwards the message to all cluster peers via the injected {@link ClusterForwarder}.
 *       The forwarder call is non-blocking; the implementation queues work onto per-peer
 *       outbound queues.</li>
 * </ol>
 * When clustering is disabled the {@link ClusterForwarder#NOOP} is supplied, keeping this
 * handler's hot path free of any conditional branching.
 * <p>
 * Security gating — a valid token and a non-empty topic — is performed up front by the
 * {@link de.kyle.avenue.handler.packet.InboundPacketHandler} before this handler runs, with the
 * same reject-and-drop outcome as the pre-protobuf {@code @Secured}/{@code @TopicHandler} checks.
 */
public class PublishMessageInboundPacketHandler implements PacketHandler {

    private final TopicSubscriptionHandler topicSubscriptionHandler;
    private final ExecutorService executorService;
    private final ClusterForwarder clusterForwarder;
    private final AvenueMetrics metrics;

    /**
     * Single-node (no-clustering) constructor — fully backwards-compatible.
     */
    public PublishMessageInboundPacketHandler(
            TopicSubscriptionHandler topicSubscriptionHandler,
            ExecutorService executorService
    ) {
        this(topicSubscriptionHandler, executorService, ClusterForwarder.NOOP, new AvenueMetrics());
    }

    /**
     * Cluster-aware constructor without explicit metrics.
     */
    public PublishMessageInboundPacketHandler(
            TopicSubscriptionHandler topicSubscriptionHandler,
            ExecutorService executorService,
            ClusterForwarder clusterForwarder
    ) {
        this(topicSubscriptionHandler, executorService, clusterForwarder, new AvenueMetrics());
    }

    /**
     * Full constructor.
     *
     * @param clusterForwarder non-blocking forwarder; use {@link ClusterForwarder#NOOP} to
     *                         disable forwarding
     * @param metrics          shared metrics registry; {@code messagesPublished} is incremented
     *                         per accepted publish
     */
    public PublishMessageInboundPacketHandler(
            TopicSubscriptionHandler topicSubscriptionHandler,
            ExecutorService executorService,
            ClusterForwarder clusterForwarder,
            AvenueMetrics metrics
    ) {
        this.topicSubscriptionHandler = topicSubscriptionHandler;
        this.executorService = executorService;
        this.clusterForwarder = clusterForwarder;
        this.metrics = metrics;
    }

    @Override
    public void handle(ClientEnvelope envelope, ClientConnectionHandler clientConnectionHandler) {
        PublishInbound publish = envelope.getPublishInbound();

        String topic = publish.getTopic();
        String source = publish.getSource();
        String data = publish.getData();

        // Metric: a local client publish was accepted and is about to be fanned out.
        metrics.incrementMessagesPublished();

        // Local delivery (async, as in the pre-clustering implementation). The outbound queue now
        // carries a fully-built PublishOutbound envelope instead of a JSON packet POJO.
        ClientEnvelope outbound = ClientEnvelopes.publishOutbound(topic, source, data);
        executorService.submit(() -> topicSubscriptionHandler.deliverPacketToSubscribers(topic, outbound));

        // Cluster forward: the forwarder builds the ClusterPublishPacket and assigns the
        // (originEpoch, seq) identity itself via its OriginSequencer — but only after it confirms
        // there are peers to forward to, so a single-node deployment never burns sequence numbers.
        clusterForwarder.forward(
                topicSubscriptionHandler.normalize(topic),
                source,
                data
        );
    }
}
