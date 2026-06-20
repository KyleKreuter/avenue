package de.kyle.avenue.handler.packet.subscribe;

import de.kyle.avenue.annotation.Secured;
import de.kyle.avenue.handler.client.ClientConnectionHandler;
import de.kyle.avenue.handler.packet.PacketHandler;
import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
import de.kyle.avenue.packet.subscribe.SubscribeAckOutboundPacket;
import org.json.JSONObject;

public class SubscribeInboundPacketHandler implements PacketHandler {

    private final TopicSubscriptionHandler topicSubscriptionHandler;

    public SubscribeInboundPacketHandler(TopicSubscriptionHandler topicSubscriptionHandler) {
        this.topicSubscriptionHandler = topicSubscriptionHandler;
    }

    // NOTE: deliberately NOT @TopicHandler. The subscribe topic travels in the BODY
    // (body.topic), whereas the @TopicHandler dispatch check validates header.topic. Keeping
    // @TopicHandler here would reject every subscribe ("Packet does not contain a topic").
    @Override
    @Secured
    public void handle(JSONObject packet, ClientConnectionHandler clientConnectionHandler) {
        JSONObject body = packet.getJSONObject("body");
        String topic = body.getString("topic");
        // Register the subscription FIRST so it is guaranteed to exist before the client is
        // told it is active. This establishes the ordering: registration happens-before the
        // ack is enqueued, so a publish the client sends after receiving the ack can never
        // race ahead of its own subscription.
        topicSubscriptionHandler.subscribeToTopic(topic, clientConnectionHandler);
        // Echo back the exact normalized key the subscription was stored under, so the client
        // can correlate the ack with the topic it asked for.
        String normalizedTopic = topicSubscriptionHandler.normalize(topic);
        clientConnectionHandler.send(new SubscribeAckOutboundPacket(normalizedTopic));
    }
}
