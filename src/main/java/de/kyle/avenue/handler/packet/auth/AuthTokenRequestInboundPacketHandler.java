package de.kyle.avenue.handler.packet.auth;

import de.kyle.avenue.handler.authentication.AuthenticationTokenHandler;
import de.kyle.avenue.handler.client.ClientConnectionHandler;
import de.kyle.avenue.handler.packet.PacketHandler;
import de.kyle.avenue.packet.auth.AuthTokenResponseOutboundPacket;
import org.json.JSONObject;

import java.io.IOException;

public class AuthTokenRequestInboundPacketHandler implements PacketHandler {
    private final AuthenticationTokenHandler authenticationTokenHandler;

    public AuthTokenRequestInboundPacketHandler(
            AuthenticationTokenHandler authenticationTokenHandler
    ) {
        this.authenticationTokenHandler = authenticationTokenHandler;
    }

    @Override
    public void handle(JSONObject packet, ClientConnectionHandler clientConnectionHandler) {
        if (!packet.getJSONObject("body").has("secret")) {
            throw new IllegalArgumentException("AuthTokenRequestInboundPacket does not contain a secret");
        }
        String secret = packet.getJSONObject("body").getString("secret");
        String token = authenticationTokenHandler.getToken(secret);
        try {
            clientConnectionHandler.send(new AuthTokenResponseOutboundPacket(token));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
