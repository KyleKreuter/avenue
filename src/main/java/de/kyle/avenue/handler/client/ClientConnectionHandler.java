package de.kyle.avenue.handler.client;

import de.kyle.avenue.handler.packet.PacketHandler;
import de.kyle.avenue.packet.OutboundPacket;
import de.kyle.avenue.registry.InboundPacketRegistry;
import de.kyle.avenue.serialization.PacketDeserializer;
import de.kyle.avenue.serialization.PacketSerializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;

public class ClientConnectionHandler implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ClientConnectionHandler.class);
    private final Socket client;
    private final InputStream inputStream;
    private final OutputStream outputStream;
    private final PacketDeserializer packetDeserializer;
    private final PacketSerializer packetSerializer;
    private final InboundPacketRegistry inboundPacketRegistry;
    private boolean running;

    public ClientConnectionHandler(
            Socket client,
            PacketDeserializer packetDeserializer,
            PacketSerializer packetSerializer,
            InboundPacketRegistry inboundPacketRegistry
    ) throws IOException {
        this.client = client;
        this.inputStream = client.getInputStream();
        this.outputStream = client.getOutputStream();
        this.running = true;
        this.packetSerializer = packetSerializer;
        this.packetDeserializer = packetDeserializer;
        this.inboundPacketRegistry = inboundPacketRegistry;
    }

    public Socket getClient() {
        return this.client;
    }

    public List<String> getTopics() {
        return List.of();
    }

    public void listen() throws IOException {
        try (DataInputStream dataInputStream = new DataInputStream(this.inputStream)) {
            while (this.running) {
                byte[] packetBytes = dataInputStream.readAllBytes();
                JSONObject packet = packetDeserializer.deserialize(packetBytes);
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
                PacketHandler packetHandler = inboundPacketRegistry.getPacketHandler(packetName);
                packetHandler.handle(packet, this);
            }
        } finally {
            shutdown();
        }
    }

    public void send(OutboundPacket packet) throws IOException {
        byte[] serializedPacket = packetSerializer.serialize(packet);
        outputStream.write(serializedPacket);
        outputStream.flush();
    }

    public void shutdown() {
        log.info("Closing connection to {}", this.client.getInetAddress().getHostAddress());
        try {
            this.running = false;
            this.inputStream.close();
            this.outputStream.close();
            this.client.close();
        } catch (IOException e) {
            log.warn("An error occurred while closing connection", e);
        }
    }


    @Override
    public void run() {
        try {
            listen();
        } catch (IOException e) {
            shutdown();
        }
    }
}
