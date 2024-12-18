package de.kyle.avenue.serialization;

import de.kyle.avenue.config.AvenueConfig;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PacketDeserializer {
    private final Pattern PATTERN = Pattern.compile("_H_(.*?)_H__B_(.*?)_B_");
    private final AvenueConfig avenueConfig;

    public PacketDeserializer(AvenueConfig avenueConfig){
        this.avenueConfig = avenueConfig;
    }

    public JSONObject deserialize(byte[] packet) throws IllegalArgumentException {
        if (packet == null) {
            throw new IllegalArgumentException("Packet cannot be null");
        }

        if (packet.length > avenueConfig.getPacketSize()) {
            throw new IllegalArgumentException(
                    String.format("Packet is too large (%d bytes), exceeding maximum size of %d bytes",
                            packet.length, avenueConfig.getPacketSize()));
        }

        String packetString = new String(packet, StandardCharsets.UTF_8);

        Matcher matcher = PATTERN.matcher(packetString);

        if (!matcher.find()) {
            throw new IllegalArgumentException("Packet mismatches format");
        }
        byte[] header = matcher.group(1).getBytes(StandardCharsets.UTF_8);
        byte[] body = matcher.group(2).getBytes(StandardCharsets.UTF_8);

        String headerString = new String(header, StandardCharsets.UTF_8);
        String bodyString = new String(body, StandardCharsets.UTF_8);

        JSONObject deserializedObject = new JSONObject();
        deserializedObject.put("header", headerString);
        deserializedObject.put("body", bodyString);
        return deserializedObject;
    }
}
