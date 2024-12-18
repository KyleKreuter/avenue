package de.kyle.avenue.serialization;

import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.packet.Packet;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class PacketSerializer {
    private final byte[] HEADER = "_H_".getBytes(StandardCharsets.UTF_8);
    private final byte[] BODY = "_B_".getBytes(StandardCharsets.UTF_8);

    public byte[] serialize(Packet packet) throws IllegalArgumentException {
        if (packet == null || packet.getHeader() == null || packet.getBody() == null) {
            throw new IllegalArgumentException("Packet, header, or body cannot be null");
        }
        byte[] packetHeader = packet.getHeader();
        byte[] packetBody = packet.getBody();

        int overhead = (HEADER.length + BODY.length) * 2;

        int totalLength = packetHeader.length + packetBody.length + overhead;

        if (totalLength > AvenueConfig.PACKET_SIZE) {
            throw new IllegalArgumentException(
                    String.format("Packet is too large (%d bytes), exceeding maximum size of %d bytes",
                            totalLength, AvenueConfig.PACKET_SIZE));
        }

        ByteBuffer buffer = ByteBuffer.allocate(totalLength);
        buffer.put(HEADER);
        buffer.put(packetHeader);
        buffer.put(HEADER);
        buffer.put(BODY);
        buffer.put(packetBody);
        buffer.put(BODY);
        return buffer.array();
    }
}
