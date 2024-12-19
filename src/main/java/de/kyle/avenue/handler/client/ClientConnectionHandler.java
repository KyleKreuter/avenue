package de.kyle.avenue.handler.client;

import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.handler.packet.InboundPacketHandler;
import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
import de.kyle.avenue.packet.OutboundPacket;
import de.kyle.avenue.serialization.PacketDeserializer;
import de.kyle.avenue.serialization.PacketSerializer;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ClientConnectionHandler implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ClientConnectionHandler.class);
    private final Socket client;
    private final InputStream inputStream;
    private final OutputStream outputStream;
    private final PacketDeserializer packetDeserializer;
    private final PacketSerializer packetSerializer;
    private final InboundPacketHandler inboundPacketHandler;
    private final TopicSubscriptionHandler topicSubscriptionHandler;
    private final AvenueConfig avenueConfig;
    private final Lock sendPacketLock = new ReentrantLock();
    private boolean running;

    public ClientConnectionHandler(
            Socket client,
            PacketDeserializer packetDeserializer,
            PacketSerializer packetSerializer,
            InboundPacketHandler inboundPacketHandler,
            AvenueConfig avenueConfig,
            TopicSubscriptionHandler topicSubscriptionHandler
    ) throws IOException {
        this.client = client;
        this.inputStream = client.getInputStream();
        this.outputStream = client.getOutputStream();
        this.running = true;
        this.packetSerializer = packetSerializer;
        this.packetDeserializer = packetDeserializer;
        this.inboundPacketHandler = inboundPacketHandler;
        this.avenueConfig = avenueConfig;
        this.topicSubscriptionHandler = topicSubscriptionHandler;
    }

    private void listen() throws IOException {
        try (DataInputStream dataInputStream = new DataInputStream(this.inputStream)) {
            while (this.running) {
                byte[] packetBytes = dataInputStream.readAllBytes();
                JSONObject packet = packetDeserializer.deserialize(packetBytes);
                try {
                    inboundPacketHandler.handleInboundPacket(packet, this);
                } catch (IllegalArgumentException | JSONException | NoSuchMethodException e) {
                    if (avenueConfig.isDropUnknownPackets()) {
                        log.error("Unknown or malformed packet was received, dropping client", e);
                        throw new RuntimeException(e);
                    } else {
                        log.warn("Unknown or malformed packet was received but 'drop-unknown' is turned off. Client is still allowed to send packets", e);
                    }
                }
            }
        } finally {
            shutdown();
        }
    }

    public void send(OutboundPacket packet) throws IOException {
        sendPacketLock.lock();
        try {
            byte[] serializedPacket = packetSerializer.serialize(packet);
            outputStream.write(serializedPacket);
            outputStream.flush();
        } finally {
            sendPacketLock.unlock();
        }
    }

    public void shutdown() {
        log.info("Closing connection to {}", this.client.getInetAddress().getHostAddress());
        try {
            this.running = false;
            this.inputStream.close();
            this.outputStream.close();
            this.client.close();
            this.topicSubscriptionHandler.unsubscribeFromAllTopics(this);
        } catch (IOException e) {
            log.warn("An error occurred while closing connection", e);
        }
    }

    @Override
    public void run() {
        try {
            listen();
        } catch (IOException e) {
            log.error("An error occurred", e);
        }finally {
            shutdown();
        }
    }
}
