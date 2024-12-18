package de.kyle.avenue.handler.packet;

import de.kyle.avenue.handler.client.DefaultClientConnectionHandler;
import de.kyle.avenue.packet.InboundPacket;

public interface PacketHandler {
    void handle(InboundPacket packet, DefaultClientConnectionHandler defaultClientConnectionHandler);
}