package de.kyle.avenue.packet.publish;

import de.kyle.avenue.handler.packet.PacketHandler;
import de.kyle.avenue.handler.packet.publish.PublishMessageInboundPacketHandler;
import de.kyle.avenue.packet.InboundPacket;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

public class PublishMessageInboundPacket implements InboundPacket {
    private final String topic;
    private final String data;
    private final String source;

    public PublishMessageInboundPacket(String topic, String data, String source) {
        this.data = data;
        this.topic = topic;
        this.source = source;
    }

    @Override
    public Class<? extends PacketHandler> getHandler() {
        return PublishMessageInboundPacketHandler.class;
    }

    @Override
    public byte[] getHeader() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name", this.getClass().getName().getBytes(StandardCharsets.UTF_8));
        jsonObject.put("topic", this.topic);
        jsonObject.put("source", this.source);
        String jsonString = jsonObject.toString();
        if (jsonString == null) {
            throw new RuntimeException("An error occurred while trying to format the header");
        }
        return jsonString.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getBody() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("data", this.data);
        String jsonString = jsonObject.toString();
        if (jsonString == null) {
            throw new RuntimeException("An error occurred while trying to format the body");
        }
        return jsonString.getBytes(StandardCharsets.UTF_8);
    }
}