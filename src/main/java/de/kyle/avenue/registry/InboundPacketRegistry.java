package de.kyle.avenue.registry;

import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.handler.authentication.AuthenticationTokenHandler;
import de.kyle.avenue.handler.authentication.Secured;
import de.kyle.avenue.handler.client.ClientConnectionHandler;
import de.kyle.avenue.handler.packet.PacketHandler;
import de.kyle.avenue.handler.packet.auth.AuthTokenRequestInboundPacketHandler;
import org.json.JSONObject;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InboundPacketRegistry {
    private final Map<String, PacketHandler> packethandlerMap = new ConcurrentHashMap<>();
    private final AvenueConfig avenueConfig;

    public InboundPacketRegistry(
            AuthenticationTokenHandler authenticationTokenHandler,
            AvenueConfig avenueConfig
    ) {
        this.avenueConfig = avenueConfig;
        this.packethandlerMap.put("AuthTokenRequestInboundPacket", new AuthTokenRequestInboundPacketHandler(authenticationTokenHandler));
    }

    public void handleInboundPacket(JSONObject packet, ClientConnectionHandler clientConnectionHandler) throws IOException, NoSuchMethodException {
        if (!packet.has("header")) {
            throw new IOException("Packet received does not contain a header field");
        }
        Object headerO = packet.get("header");
        if (!(headerO instanceof JSONObject header)) {
            throw new IOException("Packet has header field but was not parsed correctly");
        }
        if (!header.has("name")) {
            throw new IOException("Packet has no name field in the header");
        }
        String packetName = header.getString("name");
        if (!packethandlerMap.containsKey(packetName)) {
            throw new IllegalArgumentException("Packet with the provided name not found");
        }
        PacketHandler packetHandler = packethandlerMap.get(packetName);
        Method handleMethod = packetHandler.getClass().getDeclaredMethod("handle", JSONObject.class, ClientConnectionHandler.class);
        //Prüfen ob der Handler secured ist
        if (Arrays.stream(handleMethod.getAnnotations()).anyMatch(annotation -> annotation.annotationType().equals(Secured.class))) {
            if (!header.has("token")) {
                throw new IllegalArgumentException("Packet does not contain a token");
            }
            String clientToken = header.getString("token");
            if (!this.avenueConfig.getAuthenticationToken().equals(clientToken)) {
                throw new IllegalArgumentException("Provided token mismatched local token");
            }
        }
        packetHandler.handle(packet, clientConnectionHandler);
    }
}
