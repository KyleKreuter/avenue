package de.kyle.avenue.serialization;

import de.kyle.avenue.config.AvenueConfig;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

/**
 * Deserializes a single message payload back into its {@code header}/{@code body} envelope.
 * <p>
 * The input is the bare UTF-8 JSON payload {@code {"header":{...},"body":{...}}}; the 4-byte
 * length prefix has already been consumed by the stream layer (see {@link PacketFraming}).
 */
public class PacketDeserializer {
    private final int packetSize;

    public PacketDeserializer(AvenueConfig avenueConfig) {
        this.packetSize = avenueConfig.getPacketSize();
    }

    public PacketDeserializer(int packetSize) {
        this.packetSize = packetSize;
    }

    /**
     * Parses the payload bytes into a JSON object exposing {@code header} and {@code body}.
     *
     * @param packet the UTF-8 JSON payload bytes (without length prefix)
     * @return a {@link JSONObject} with {@code header} and {@code body} child objects
     * @throws IllegalArgumentException if the payload is null, too large, or malformed
     */
    public JSONObject deserialize(byte[] packet) throws IllegalArgumentException {
        if (packet == null) {
            throw new IllegalArgumentException("Packet cannot be null");
        }

        if (packet.length > packetSize) {
            throw new IllegalArgumentException(
                    String.format("Packet is too large (%d bytes), exceeding maximum size of %d bytes",
                            packet.length, packetSize));
        }

        JSONObject envelope = new JSONObject(new String(packet, StandardCharsets.UTF_8));

        if (!envelope.has("header") || !envelope.has("body")) {
            throw new IllegalArgumentException("Packet mismatches format: missing header or body");
        }

        JSONObject deserializedObject = new JSONObject();
        deserializedObject.put("header", envelope.getJSONObject("header"));
        deserializedObject.put("body", envelope.getJSONObject("body"));
        return deserializedObject;
    }
}
