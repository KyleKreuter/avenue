package de.kyle.avenue.packet;

import de.kyle.avenue.handler.packet.PacketHandler;

public interface InboundPacket extends Packet {
    Class<? extends PacketHandler> getHandler();
}
