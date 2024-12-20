package de.kyle.avenue.serialization;

import de.kyle.avenue.packet.Packet;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

public class SerializationTest {

    @Test
    public void valid_packet_to_valid_json() {
        Packet packet = new Packet() {
            @Override
            public byte[] getHeader() {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("name", "TestPacket");
                return jsonObject.toString().getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public byte[] getBody() {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("data", "value");
                return jsonObject.toString().getBytes(StandardCharsets.UTF_8);
            }
        };
        PacketSerializer packetSerializer = new PacketSerializer(512);
        byte[] serialized = packetSerializer.serialize(packet);
        Assertions.assertTrue(serialized.length < 512);
        PacketDeserializer packetDeserializer = new PacketDeserializer(512);
        JSONObject jsonObject = packetDeserializer.deserialize(serialized);
        Assertions.assertDoesNotThrow(() -> jsonObject.getJSONObject("header"));
        Assertions.assertDoesNotThrow(() -> jsonObject.getJSONObject("body"));
        Assertions.assertEquals("TestPacket", jsonObject.getJSONObject("header").getString("name"));
        Assertions.assertEquals("value", jsonObject.getJSONObject("body").getString("data"));
    }

    @Test
    public void maximum_packet_size_exceeded() {
        Packet packet = new Packet() {
            @Override
            public byte[] getHeader() {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("name", "TestPacket");
                return jsonObject.toString().getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public byte[] getBody() {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("data", "value");
                return jsonObject.toString().getBytes(StandardCharsets.UTF_8);
            }
        };
        PacketSerializer packetSerializer = new PacketSerializer(2);
        Assertions.assertThrows(IllegalArgumentException.class, () -> packetSerializer.serialize(packet));
    }
}
