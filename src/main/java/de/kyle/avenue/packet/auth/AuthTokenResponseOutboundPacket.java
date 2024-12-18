package de.kyle.avenue.packet.auth;

import de.kyle.avenue.packet.OutboundPacket;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

public class AuthTokenResponseOutboundPacket implements OutboundPacket {
    private final String token;

    public AuthTokenResponseOutboundPacket(String token) {
        this.token = token;
    }

    @Override
    public byte[] getHeader() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name", this.getClass().getName().getBytes(StandardCharsets.UTF_8));
        String jsonString = jsonObject.toString();
        if (jsonString == null) {
            throw new RuntimeException("The provided secret is formatted badly");
        }
        return jsonString.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getBody() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("token", this.token);
        String jsonString = jsonObject.toString();
        if (jsonString == null) {
            throw new RuntimeException("The provided secret is formatted badly");
        }
        return jsonString.getBytes(StandardCharsets.UTF_8);
    }
}
