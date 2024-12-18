package de.kyle.avenue.packet.auth;

import de.kyle.avenue.handler.packet.PacketHandler;
import de.kyle.avenue.packet.InboundPacket;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

public class AuthTokenRequestInboundPacket implements InboundPacket {
    private final String secret;

    public AuthTokenRequestInboundPacket(String secret) {
        this.secret = secret;
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
        jsonObject.put("secret", this.secret);
        String jsonString = jsonObject.toString();
        if (jsonString == null) {
            throw new RuntimeException("The provided secret is formatted badly");
        }
        return jsonString.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public PacketHandler getHandler() {
        return null;
    }
}