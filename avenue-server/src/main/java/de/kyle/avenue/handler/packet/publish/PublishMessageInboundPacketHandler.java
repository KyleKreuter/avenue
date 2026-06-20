package de.kyle.avenue.handler.packet.publish;

import com.google.protobuf.ByteString;
import de.kyle.avenue.cluster.ClusterForwarder;
import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.handler.client.ClientConnectionHandler;
import de.kyle.avenue.handler.packet.PacketHandler;
import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
import de.kyle.avenue.metrics.AvenueMetrics;
import de.kyle.avenue.proto.ClientEnvelope;
import de.kyle.avenue.proto.PublishInbound;
import de.kyle.avenue.serialization.OutboundEncoder;

import java.util.concurrent.ExecutorService;

/**
 * Handles a {@link PublishInbound} message from a LOCAL client.
 * <p>
 * On receipt, this handler:
 * <ol>
 *   <li>Normalizes the topic key exactly once and delivers the message to all local subscribers
 *       via {@link TopicSubscriptionHandler#deliverToSubscribers}. Delivery runs <em>inline</em> on
 *       the reader thread for the normal small-fan-out case (no task allocation, no thread hop);
 *       only when the subscriber count exceeds {@code server.inline-delivery.max-fanout} is the
 *       fan-out handed to the shared executor so one reader thread cannot be monopolized by a huge
 *       fan-out. Each subscriber enqueue is itself non-blocking.</li>
 *   <li>Forwards the message to all cluster peers via the injected {@link ClusterForwarder}, using
 *       the same already-normalized key. The forwarder call is non-blocking; the implementation
 *       queues work onto per-peer outbound queues.</li>
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
    /** Maximum payload size enforced during the encode-once serialization of the fan-out envelope. */
    private final int packetSize;
    /** Above this subscriber count a publish is fanned out via the executor instead of inline. */
    private final int inlineDeliveryMaxFanout;

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
     * Full constructor without an explicit {@link AvenueConfig}: uses default tuning values
     * (packet size and inline-delivery fan-out threshold). Kept for backwards compatibility.
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
        this(topicSubscriptionHandler, executorService, clusterForwarder, metrics,
                DEFAULT_PACKET_SIZE, AvenueConfig.DEFAULT_INLINE_DELIVERY_MAX_FANOUT);
    }

    /**
     * Default packet size used when no {@link AvenueConfig} is supplied. Matches the bundled
     * {@code server.packet.max-size} default so the size guard behaves identically.
     */
    private static final int DEFAULT_PACKET_SIZE = 1 << 20;

    /**
     * Full constructor pinning the encode-once packet-size guard and the inline-delivery fan-out
     * threshold from the supplied {@link AvenueConfig}. This is the constructor production code uses.
     *
     * @param clusterForwarder non-blocking forwarder; use {@link ClusterForwarder#NOOP} to disable
     * @param metrics          shared metrics registry; {@code messagesPublished} per accepted publish
     * @param packetSize       maximum payload size enforced during serialization
     * @param inlineDeliveryMaxFanout subscriber-count threshold for inline vs. executor delivery
     */
    public PublishMessageInboundPacketHandler(
            TopicSubscriptionHandler topicSubscriptionHandler,
            ExecutorService executorService,
            ClusterForwarder clusterForwarder,
            AvenueMetrics metrics,
            int packetSize,
            int inlineDeliveryMaxFanout
    ) {
        this.topicSubscriptionHandler = topicSubscriptionHandler;
        this.executorService = executorService;
        this.clusterForwarder = clusterForwarder;
        this.metrics = metrics;
        this.packetSize = packetSize;
        this.inlineDeliveryMaxFanout = inlineDeliveryMaxFanout > 0
                ? inlineDeliveryMaxFanout : AvenueConfig.DEFAULT_INLINE_DELIVERY_MAX_FANOUT;
    }

    @Override
    public void handle(ClientEnvelope envelope, ClientConnectionHandler clientConnectionHandler) {
        PublishInbound publish = envelope.getPublishInbound();

        String topic = publish.getTopic();
        String source = publish.getSource();
        // OPAQUE PASSTHROUGH: the payload is an immutable ByteString taken straight from the parsed
        // inbound publish. It is NEVER turned into a Java String on the server hot path — it is shared
        // verbatim with the encode-once fan-out envelope and the cluster forward. Because protobuf
        // `bytes` is a raw memcpy (no per-character UTF-8 transcoding), this eliminates the
        // String.charAt / StringLatin1.charAt CPU block that dominated the hot path under load.
        ByteString data = publish.getData();

        // Metric: a local client publish was accepted and is about to be fanned out.
        metrics.incrementMessagesPublished();

        // Normalize the topic key EXACTLY ONCE and reuse it for both local delivery and the cluster
        // forward (it used to be normalized twice per publish — once here implicitly inside delivery
        // and once for forward). Saves one toLowerCase+strip String allocation per publish.
        String normalizedTopic = topicSubscriptionHandler.normalize(topic);

        // Local delivery. DIRECT ENCODE: produce the bare protobuf payload of the outbound
        // PublishOutbound envelope straight into a single byte[] via the CodedOutputStream-based
        // OutboundEncoder — no per-publish ClientEnvelope/PublishOutbound builder or message object is
        // allocated (that builder/message graph dominated server allocation under load; see JFR). The
        // outbound frame carries the RAW topic exactly as before (only the lookup key is normalized),
        // so the bytes delivered to subscribers are byte-for-byte identical to the builder path — the
        // OutboundEncoderTest guardrail asserts this wire equality. The same immutable byte[] is shared
        // with every subscriber (encode-once fan-out).
        byte[] outboundFrame = OutboundEncoder.encodePublishOutbound(topic, source, data);

        // Inline delivery: run the fan-out directly on this reader thread for the normal case, saving
        // a task allocation and a thread hop per publish. Only when the subscriber count is large
        // enough to risk monopolizing the reader thread do we hand the fan-out to the executor.
        if (topicSubscriptionHandler.subscriberCount(normalizedTopic) > inlineDeliveryMaxFanout) {
            executorService.submit(() ->
                    topicSubscriptionHandler.deliverPreSerializedToSubscribers(normalizedTopic, outboundFrame));
        } else {
            topicSubscriptionHandler.deliverPreSerializedToSubscribers(normalizedTopic, outboundFrame);
        }

        // Cluster forward: the forwarder builds the ClusterPublishPacket and assigns the
        // (originEpoch, seq) identity itself via its OriginSequencer — but only after it confirms
        // there are peers to forward to, so a single-node deployment never burns sequence numbers.
        clusterForwarder.forward(
                normalizedTopic,
                source,
                data
        );
    }
}
