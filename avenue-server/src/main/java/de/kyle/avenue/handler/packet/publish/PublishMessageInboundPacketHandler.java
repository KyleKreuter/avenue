package de.kyle.avenue.handler.packet.publish;

import de.kyle.avenue.annotation.Secured;
import de.kyle.avenue.annotation.TopicHandler;
import de.kyle.avenue.handler.client.ClientConnectionHandler;
import de.kyle.avenue.handler.packet.PacketHandler;
import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
import de.kyle.avenue.packet.publish.PublishMessageOutboundPacket;
import org.json.JSONObject;

import java.util.concurrent.ExecutorService;

public class PublishMessageInboundPacketHandler implements PacketHandler {
    private final TopicSubscriptionHandler topicSubscriptionHandler;
    private final ExecutorService executorService;

    public PublishMessageInboundPacketHandler(
            TopicSubscriptionHandler topicSubscriptionHandler,
            ExecutorService executorService
    ) {
        this.topicSubscriptionHandler = topicSubscriptionHandler;
        this.executorService = executorService;
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
        executorService.submit(() -> topicSubscriptionHandler.deliverPacketToSubscribers(topic, outboundPacket));
    }
}
