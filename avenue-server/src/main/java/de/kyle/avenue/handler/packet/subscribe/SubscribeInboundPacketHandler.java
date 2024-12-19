package de.kyle.avenue.handler.packet.subscribe;

import de.kyle.avenue.annotation.Secured;
import de.kyle.avenue.annotation.TopicHandler;
import de.kyle.avenue.handler.client.ClientConnectionHandler;
import de.kyle.avenue.handler.packet.PacketHandler;
import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
import org.json.JSONObject;

public class SubscribeInboundPacketHandler implements PacketHandler {

    private final TopicSubscriptionHandler topicSubscriptionHandler;

    public SubscribeInboundPacketHandler(TopicSubscriptionHandler topicSubscriptionHandler) {
        this.topicSubscriptionHandler = topicSubscriptionHandler;
    }

    @Override
    @Secured
    @TopicHandler
    public void handle(JSONObject packet, ClientConnectionHandler clientConnectionHandler) {
        JSONObject body = packet.getJSONObject("body");
        String topic = body.getString("topic");
        topicSubscriptionHandler.subscribeToTopic(topic, clientConnectionHandler);
    }
}
