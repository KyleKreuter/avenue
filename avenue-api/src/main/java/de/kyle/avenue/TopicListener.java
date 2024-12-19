package de.kyle.avenue;

import de.kyle.avenue.packet.OutboundPacket;

public interface TopicListener {
    void onMessage(OutboundPacket packet);
}
