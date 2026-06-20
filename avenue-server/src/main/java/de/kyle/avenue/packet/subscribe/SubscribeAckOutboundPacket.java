package de.kyle.avenue.packet.subscribe;

import de.kyle.avenue.packet.OutboundPacket;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

/**
 * Server-to-client acknowledgment that a subscription has been registered.
 * <p>
 * This packet closes a protocol gap: subscriber and publisher live on separate TCP
 * connections (and separate handler threads). Without an acknowledgment a client cannot know
 * when its subscription is actually registered on the server, so a publish issued "right
 * after" a subscribe could be processed before the subscription exists, racing delivery.
 * <p>
 * The server emits this packet AFTER the subscription has been registered (so registration
 * happens-before the ack reaches the client). The acknowledged, already-normalized
 * {@code topic} travels in the body, mirroring {@link SubscribeInboundPacket} where the topic
 * also lives in the body. This packet is purely server-to-client and therefore has no inbound
 * handler.
 */
public class SubscribeAckOutboundPacket implements OutboundPacket {
    private final String topic;

    public SubscribeAckOutboundPacket(String topic) {
        this.topic = topic;
    }

    @Override
    public byte[] getHeader() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name", this.getClass().getSimpleName());
        String jsonString = jsonObject.toString();
        if (jsonString == null) {
            throw new RuntimeException("An error occurred while trying to format the header");
        }
        return jsonString.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getBody() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("topic", this.topic);
        String jsonString = jsonObject.toString();
        if (jsonString == null) {
            throw new RuntimeException("An error occurred while trying to format the body");
        }
        return jsonString.getBytes(StandardCharsets.UTF_8);
    }
}
