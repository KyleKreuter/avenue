package de.kyle.avenue.serialization;

import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.packet.Packet;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

/**
 * Serializes a {@link Packet} into the wire payload of a single message.
 * <p>
 * The wire payload is a single UTF-8 encoded JSON object of the form
 * {@code {"header":{...},"body":{...}}}. This method returns ONLY the payload
 * bytes; the 4-byte length prefix that frames a message on the stream is added
 * by the stream layer (see {@link PacketFraming}).
 */
public class PacketSerializer {
    private final int packetSize;

    public PacketSerializer(AvenueConfig avenueConfig) {
        this.packetSize = avenueConfig.getPacketSize();
    }

    public PacketSerializer(int packetSize) {
        this.packetSize = packetSize;
    }

    /**
     * Builds the UTF-8 JSON payload {@code {"header":...,"body":...}} for the given packet.
     *
     * @param packet the packet to serialize
     * @return the UTF-8 encoded payload bytes (without length prefix)
     * @throws IllegalArgumentException if the packet is invalid or exceeds the configured maximum size
     */
    public byte[] serialize(Packet packet) throws IllegalArgumentException {
        if (packet == null || packet.getHeader() == null || packet.getBody() == null) {
            throw new IllegalArgumentException("Packet, header, or body cannot be null");
        }

        // Parse the per-packet header/body bytes back into JSON and wrap them into a
        // single envelope object. This avoids any fragile string markers in the payload.
        JSONObject header = new JSONObject(new String(packet.getHeader(), StandardCharsets.UTF_8));
        JSONObject body = new JSONObject(new String(packet.getBody(), StandardCharsets.UTF_8));

        JSONObject envelope = new JSONObject();
        envelope.put("header", header);
        envelope.put("body", body);

        byte[] payload = envelope.toString().getBytes(StandardCharsets.UTF_8);

        if (payload.length > packetSize) {
            throw new IllegalArgumentException(
                    String.format("Packet is too large (%d bytes), exceeding maximum size of %d bytes",
                            payload.length, packetSize));
        }
        return payload;
    }
}
