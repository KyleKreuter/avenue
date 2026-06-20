package de.kyle.avenue.handler.packet.subscribe;

import de.kyle.avenue.annotation.Secured;
import de.kyle.avenue.handler.client.ClientConnectionHandler;
import de.kyle.avenue.handler.packet.PacketHandler;
import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
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
        topicSubscriptionHandler.subscribeToTopic(topic, clientConnectionHandler);
    }
}
