package de.kyle.avenue.handler.packet.publish;

import de.kyle.avenue.annotation.Secured;
import de.kyle.avenue.annotation.TopicHandler;
import de.kyle.avenue.cluster.ClusterForwarder;
import de.kyle.avenue.handler.client.ClientConnectionHandler;
import de.kyle.avenue.handler.packet.PacketHandler;
import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
import de.kyle.avenue.metrics.AvenueMetrics;
import de.kyle.avenue.packet.publish.PublishMessageOutboundPacket;
import org.json.JSONObject;

import java.util.UUID;
import java.util.concurrent.ExecutorService;

/**
 * Handles a {@code PublishMessageInboundPacket} from a LOCAL client.
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
    @Secured
    @TopicHandler
    public void handle(JSONObject packet, ClientConnectionHandler clientConnectionHandler) {
        JSONObject header = packet.getJSONObject("header");
        JSONObject body = packet.getJSONObject("body");

        String topic = header.getString("topic");
        String source = header.getString("source");
        String data = body.getString("data");

        // Metric: a local client publish was accepted and is about to be fanned out.
        metrics.incrementMessagesPublished();

        // Local delivery (async, as in the pre-clustering implementation).
        PublishMessageOutboundPacket outbound = new PublishMessageOutboundPacket(topic, data, source);
        executorService.submit(() -> topicSubscriptionHandler.deliverPacketToSubscribers(topic, outbound));

        // Cluster forward: a unique messageId per message is generated here and passed to
        // the forwarder so the forwarder can build the ClusterPublishPacket with the correct
        // originNodeId (which the forwarder holds) and this messageId for dedup.
        String messageId = UUID.randomUUID().toString();
        clusterForwarder.forward(
                topicSubscriptionHandler.normalize(topic),
                source,
                data,
                messageId
        );
    }
}
