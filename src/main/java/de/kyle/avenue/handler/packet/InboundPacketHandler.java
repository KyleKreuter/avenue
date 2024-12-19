package de.kyle.avenue.handler.packet;

import de.kyle.avenue.annotation.Secured;
import de.kyle.avenue.annotation.TopicHandler;
import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.handler.authentication.AuthenticationTokenHandler;
import de.kyle.avenue.handler.client.ClientConnectionHandler;
import de.kyle.avenue.handler.packet.auth.AuthTokenRequestInboundPacketHandler;
import de.kyle.avenue.handler.packet.publish.PublishMessageInboundPacketHandler;
import de.kyle.avenue.handler.packet.subscribe.SubscribeInboundPacketHandler;
import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
import org.json.JSONObject;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InboundPacketHandler {
    private final Map<String, PacketHandler> packethandlerMap = new ConcurrentHashMap<>();
    private final AuthenticationTokenHandler authenticationTokenHandler;

    public InboundPacketHandler(
            AuthenticationTokenHandler authenticationTokenHandler,
            TopicSubscriptionHandler topicSubscriptionHandler
    ) {
        this.authenticationTokenHandler = authenticationTokenHandler;
        this.packethandlerMap.put("AuthTokenRequestInboundPacket", new AuthTokenRequestInboundPacketHandler(authenticationTokenHandler));
        this.packethandlerMap.put("PublishMessageInboundPacket", new PublishMessageInboundPacketHandler(topicSubscriptionHandler));
        this.packethandlerMap.put("SubscribeInboundPacket", new SubscribeInboundPacketHandler(topicSubscriptionHandler));
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
        Method handle = packetHandler.getClass().getDeclaredMethod("handle", JSONObject.class, ClientConnectionHandler.class);

        //Prüfen ob der handler secured ist
        handleSecuredPacketHandler(header, handle);

        //Prüfen ob der packet handler ein topic handler ist
        handleTopicPacketHandler(header, handle);

        packetHandler.handle(packet, clientConnectionHandler);
    }

    private void handleSecuredPacketHandler(JSONObject header, Method handleMethod) {
        if (Arrays.stream(handleMethod.getAnnotations()).anyMatch(annotation -> annotation.annotationType().equals(Secured.class))) {
            if (!header.has("token")) {
                throw new IllegalArgumentException("Packet does not contain a token");
            }
            String clientToken = header.getString("token");
            if (!this.authenticationTokenHandler.isValidToken(clientToken)) {
                throw new IllegalArgumentException("Provided token mismatched local token");
            }
        }
    }

    private void handleTopicPacketHandler(JSONObject header, Method handleMethod) {
        if (Arrays.stream(handleMethod.getAnnotations()).anyMatch(annotation -> annotation.annotationType().equals(TopicHandler.class))) {
            if (!header.has("topic")) {
                throw new IllegalArgumentException("Packet does not contain a topic");
            }
        }
    }
}
