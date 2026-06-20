package de.kyle.avenue.handler.packet.subscribe;

import de.kyle.avenue.handler.client.ClientConnectionHandler;
import de.kyle.avenue.handler.packet.PacketHandler;
import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
import de.kyle.avenue.proto.ClientEnvelope;
import de.kyle.avenue.proto.Subscribe;
import de.kyle.avenue.serialization.ClientEnvelopes;

/**
 * Registers a subscription for a {@link Subscribe} message and answers with a
 * {@code SubscribeAck} envelope.
 * <p>
 * Security gating — a valid token ({@code @Secured} semantics) and a non-empty topic
 * ({@code @TopicHandler} semantics) — is performed up front by the
 * {@link de.kyle.avenue.handler.packet.InboundPacketHandler} before this handler ever runs, so
 * the topic here is guaranteed non-empty and the caller authenticated. The pre-protobuf
 * implementation could not gate the topic at dispatch time (it travelled in the JSON body, not
 * the header) and therefore relied on a {@code body.getString("topic")} that threw on a missing
 * topic; the typed envelope now lets the dispatcher gate it cleanly with the same reject-and-drop
 * outcome.
 */
public class SubscribeInboundPacketHandler implements PacketHandler {

    private final TopicSubscriptionHandler topicSubscriptionHandler;

    public SubscribeInboundPacketHandler(TopicSubscriptionHandler topicSubscriptionHandler) {
        this.topicSubscriptionHandler = topicSubscriptionHandler;
    }

    @Override
    public void handle(ClientEnvelope envelope, ClientConnectionHandler clientConnectionHandler) {
        Subscribe subscribe = envelope.getSubscribe();
        String topic = subscribe.getTopic();
        // Register the subscription FIRST so it is guaranteed to exist before the client is
        // told it is active. This establishes the ordering: registration happens-before the
        // ack is enqueued, so a publish the client sends after receiving the ack can never
        // race ahead of its own subscription.
        topicSubscriptionHandler.subscribeToTopic(topic, clientConnectionHandler);
        // Echo back the exact normalized key the subscription was stored under, so the client
        // can correlate the ack with the topic it asked for.
        String normalizedTopic = topicSubscriptionHandler.normalize(topic);
        clientConnectionHandler.send(ClientEnvelopes.subscribeAck(normalizedTopic));
    }
}
