package de.kyle.avenue.handler.packet;

import de.kyle.avenue.cluster.ClusterForwarder;
import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.handler.authentication.AuthenticationTokenHandler;
import de.kyle.avenue.handler.client.ClientConnection;
import de.kyle.avenue.handler.client.ClientConnectionHandler;
import de.kyle.avenue.handler.packet.auth.AuthTokenRequestInboundPacketHandler;
import de.kyle.avenue.handler.packet.publish.PublishMessageInboundPacketHandler;
import de.kyle.avenue.handler.packet.subscribe.SubscribeInboundPacketHandler;
import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
import de.kyle.avenue.metrics.AvenueMetrics;
import de.kyle.avenue.proto.ClientEnvelope;
import de.kyle.avenue.serialization.WireCodec;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

/**
 * Decodes an inbound client frame into a typed {@link ClientEnvelope} and routes it to the
 * matching {@link PacketHandler} based on the set {@code oneof} case.
 * <p>
 * Dispatch is a plain {@code switch} over {@link ClientEnvelope.MsgCase}. This replaces the old
 * JSON {@code header.name}-based lookup, and the {@code @Secured}/{@code @TopicHandler} annotation
 * flags are replaced by explicit, TYPED per-case checks below:
 * <ul>
 *   <li>{@code AUTH_REQUEST}: unsecured — no token is required to authenticate.</li>
 *   <li>{@code SUBSCRIBE} / {@code PUBLISH_INBOUND}: secured — the message's {@code token} field
 *       must match the local token (constant-time check), AND the {@code topic} field must be
 *       present and non-empty. In proto3 an unset string defaults to {@code ""}, so an empty
 *       topic / token is treated exactly like a missing one was under JSON, preserving the
 *       original reject-and-drop behaviour.</li>
 *   <li>The server-to-client cases ({@code AUTH_RESPONSE}, {@code SUBSCRIBE_ACK},
 *       {@code PUBLISH_OUTBOUND}) and {@code MSG_NOT_SET} never legitimately arrive from a client;
 *       they are rejected as malformed so the connection is dropped (same outcome a stray JSON
 *       name produced).</li>
 * </ul>
 * Any rejection is an {@link IllegalArgumentException}, which the caller
 * ({@link ClientConnectionHandler}) turns into the configured drop/close behaviour.
 * <p>
 * An optional {@link ClusterForwarder} is injected into
 * {@link PublishMessageInboundPacketHandler} to forward local publishes to cluster peers. When
 * clustering is disabled, {@link ClusterForwarder#NOOP} is used automatically.
 */
public class InboundPacketHandler {
    private final AuthenticationTokenHandler authenticationTokenHandler;
    private final AuthTokenRequestInboundPacketHandler authHandler;
    private final SubscribeInboundPacketHandler subscribeHandler;
    private final PublishMessageInboundPacketHandler publishHandler;

    /**
     * Single-node constructor (no clustering). Fully backwards-compatible with existing callers.
     */
    public InboundPacketHandler(
            AuthenticationTokenHandler authenticationTokenHandler,
            TopicSubscriptionHandler topicSubscriptionHandler,
            ExecutorService executorService
    ) {
        this(authenticationTokenHandler, topicSubscriptionHandler, executorService,
                ClusterForwarder.NOOP, new AvenueMetrics());
    }

    /**
     * Cluster-aware constructor without explicit metrics. Uses a standalone metrics registry.
     */
    public InboundPacketHandler(
            AuthenticationTokenHandler authenticationTokenHandler,
            TopicSubscriptionHandler topicSubscriptionHandler,
            ExecutorService executorService,
            ClusterForwarder clusterForwarder
    ) {
        this(authenticationTokenHandler, topicSubscriptionHandler, executorService,
                clusterForwarder, new AvenueMetrics());
    }

    /**
     * Full constructor without an explicit {@link AvenueConfig}: the publish handler uses default
     * tuning values (packet size and inline-delivery fan-out threshold). Kept for backwards
     * compatibility.
     */
    public InboundPacketHandler(
            AuthenticationTokenHandler authenticationTokenHandler,
            TopicSubscriptionHandler topicSubscriptionHandler,
            ExecutorService executorService,
            ClusterForwarder clusterForwarder,
            AvenueMetrics metrics
    ) {
        this.authenticationTokenHandler = authenticationTokenHandler;
        this.authHandler = new AuthTokenRequestInboundPacketHandler(authenticationTokenHandler);
        this.publishHandler = new PublishMessageInboundPacketHandler(
                topicSubscriptionHandler, executorService, clusterForwarder, metrics);
        this.subscribeHandler = new SubscribeInboundPacketHandler(topicSubscriptionHandler);
    }

    /**
     * Config-aware full constructor. Pins the publish handler's encode-once packet-size guard and the
     * inline-delivery fan-out threshold from {@code avenueConfig}. This is the constructor production
     * code ({@link de.kyle.avenue.SingleNodeServer}) uses.
     */
    public InboundPacketHandler(
            AuthenticationTokenHandler authenticationTokenHandler,
            TopicSubscriptionHandler topicSubscriptionHandler,
            ExecutorService executorService,
            ClusterForwarder clusterForwarder,
            AvenueMetrics metrics,
            AvenueConfig avenueConfig
    ) {
        this.authenticationTokenHandler = authenticationTokenHandler;
        this.authHandler = new AuthTokenRequestInboundPacketHandler(authenticationTokenHandler);
        this.publishHandler = new PublishMessageInboundPacketHandler(
                topicSubscriptionHandler, executorService, clusterForwarder, metrics,
                avenueConfig.getPacketSize(), avenueConfig.getInlineDeliveryMaxFanout());
        this.subscribeHandler = new SubscribeInboundPacketHandler(topicSubscriptionHandler);
    }

    /**
     * Decodes a raw client frame and dispatches it to the matching handler after typed security
     * gating.
     *
     * @param frameBytes             the bare protobuf payload of one frame (length prefix already
     *                               consumed)
     * @param maxPacketSize          the configured maximum payload size (oversize -&gt; reject)
     * @param clientConnection       the connection the frame arrived on
     * @throws IllegalArgumentException if the payload is malformed, oversized, an unexpected case,
     *                                  or fails the token/topic gate
     */
    public void handleInboundPacket(
            byte[] frameBytes,
            int maxPacketSize,
            ClientConnection clientConnection
    ) throws IOException {
        ClientEnvelope envelope = WireCodec.decodeClient(frameBytes, maxPacketSize);
        switch (envelope.getMsgCase()) {
            case AUTH_REQUEST -> authHandler.handle(envelope, clientConnection);
            case SUBSCRIBE -> {
                // @Secured + @TopicHandler equivalent: a valid token and a non-empty topic.
                verifyToken(envelope.getSubscribe().getToken());
                requireTopic(envelope.getSubscribe().getTopic());
                subscribeHandler.handle(envelope, clientConnection);
            }
            case PUBLISH_INBOUND -> {
                // @Secured + @TopicHandler equivalent: a valid token and a non-empty topic.
                verifyToken(envelope.getPublishInbound().getToken());
                requireTopic(envelope.getPublishInbound().getTopic());
                publishHandler.handle(envelope, clientConnection);
            }
            // Server-to-client cases and an unset oneof never legitimately originate from a
            // client. Treat them as a desynced/hostile peer and reject (drop/close downstream).
            default -> throw new IllegalArgumentException(
                    "Unexpected client message case: " + envelope.getMsgCase());
        }
    }

    /**
     * Verifies the client-supplied token against the local token using the existing constant-time
     * comparison. An empty token (proto3 default for an unset field) can never match a non-empty
     * configured token, so it is rejected exactly like a missing token was under JSON.
     */
    private void verifyToken(String clientToken) {
        if (!this.authenticationTokenHandler.isValidToken(clientToken)) {
            throw new IllegalArgumentException("Provided token mismatched local token");
        }
    }

    /**
     * Rejects a missing/empty topic. proto3 represents an unset string as {@code ""}, which is the
     * "missing topic" case; this preserves the original {@code @TopicHandler} reject behaviour.
     */
    private void requireTopic(String topic) {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("Packet does not contain a topic");
        }
    }
}
