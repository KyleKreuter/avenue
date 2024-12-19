package de.kyle.avenue.packet.publish;

import de.kyle.avenue.packet.OutboundPacket;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

public class PublishMessageOutboundPacket implements OutboundPacket {
    public final String data;
    private final String topic;
    private final String source;

    public PublishMessageOutboundPacket(String topic, String data, String source) {
        this.topic = topic;
        this.data = data;
        this.source = source;
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
