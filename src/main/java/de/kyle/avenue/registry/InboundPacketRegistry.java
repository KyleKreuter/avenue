package de.kyle.avenue.registry;

import de.kyle.avenue.handler.packet.PacketHandler;
import de.kyle.avenue.handler.packet.auth.AuthTokenRequestInboundPacketHandler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InboundPacketRegistry {
    private Map<String, PacketHandler> packethandlerMap = new ConcurrentHashMap<>();

    public InboundPacketRegistry() {
        this.packethandlerMap.put("AuthTokenRequestInboundPacket", new AuthTokenRequestInboundPacketHandler());
    }

    public PacketHandler getPacketHandler(String packetName) {
        if (!packethandlerMap.containsKey(packetName)) {
            throw new IllegalArgumentException("Packet with the provided name not found");
        }
        return packethandlerMap.get(packetName);
    }
}
