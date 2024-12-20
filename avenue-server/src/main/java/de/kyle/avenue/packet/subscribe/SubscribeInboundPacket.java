package de.kyle.avenue.packet.subscribe;

import de.kyle.avenue.handler.packet.PacketHandler;
import de.kyle.avenue.handler.packet.subscribe.SubscribeInboundPacketHandler;
import de.kyle.avenue.packet.InboundPacket;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

public class SubscribeInboundPacket implements InboundPacket {
    private final String topic;
    private final String token;

    public SubscribeInboundPacket(String topic, String token) {
        this.topic = topic;
        this.token = token;
    }

    @Override
    public Class<? extends PacketHandler> getHandler() {
        return SubscribeInboundPacketHandler.class;
    }

    @Override
    public byte[] getHeader() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name", this.getClass().getName().getBytes(StandardCharsets.UTF_8));
        jsonObject.put("token", this.token);
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
