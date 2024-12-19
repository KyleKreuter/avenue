package de.kyle.avenue.handler.packet.publish;

import de.kyle.avenue.annotation.Secured;
import de.kyle.avenue.annotation.TopicHandler;
import de.kyle.avenue.handler.client.ClientConnectionHandler;
import de.kyle.avenue.handler.packet.PacketHandler;
import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
import de.kyle.avenue.packet.publish.PublishMessageOutboundPacket;
import org.json.JSONObject;

public class PublishMessageInboundPacketHandler implements PacketHandler {
    private final TopicSubscriptionHandler topicSubscriptionHandler;

    public PublishMessageInboundPacketHandler(TopicSubscriptionHandler topicSubscriptionHandler) {
        this.topicSubscriptionHandler = topicSubscriptionHandler;
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

        PublishMessageOutboundPacket outboundPacket = new PublishMessageOutboundPacket(topic, data, source);
        topicSubscriptionHandler.deliverPacketToSubscribers(topic, outboundPacket);
    }
}
