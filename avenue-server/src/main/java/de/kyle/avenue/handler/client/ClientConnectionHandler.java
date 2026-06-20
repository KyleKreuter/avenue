package de.kyle.avenue.handler.client;

import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.handler.packet.InboundPacketHandler;
import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
import de.kyle.avenue.packet.OutboundPacket;
import de.kyle.avenue.serialization.PacketDeserializer;
import de.kyle.avenue.serialization.PacketFraming;
import de.kyle.avenue.serialization.PacketSerializer;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Handles a single client connection.
 * <p>
 * Outbound writes are decoupled from delivery via a bounded per-client queue and a dedicated
 * writer running on a virtual thread. Producers ({@link #enqueue(OutboundPacket)}) never block
 * on the socket, so one slow subscriber can no longer cause head-of-line blocking for the
 * fan-out to all other subscribers. Only the writer thread touches the {@link DataOutputStream},
 * which is why no write lock is needed any more.
 */
public class ClientConnectionHandler implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ClientConnectionHandler.class);

    private final Socket client;
    private final InputStream inputStream;
    private final OutputStream outputStream;
    private final DataOutputStream dataOutputStream;
    private final PacketDeserializer packetDeserializer;
    private final PacketSerializer packetSerializer;
    private final InboundPacketHandler inboundPacketHandler;
    private final TopicSubscriptionHandler topicSubscriptionHandler;
    private final AvenueConfig avenueConfig;

    /** Bounded outbound queue. Backpressure is applied via offer() with a short timeout. */
    private final BlockingQueue<OutboundPacket> outboundQueue;
    private final long offerTimeoutMillis;
    private Thread writerThread;

    private volatile boolean running;

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
        this.dataOutputStream = new DataOutputStream(this.outputStream);
        this.running = true;
        this.packetSerializer = packetSerializer;
        this.packetDeserializer = packetDeserializer;
        this.inboundPacketHandler = inboundPacketHandler;
        this.avenueConfig = avenueConfig;
        this.topicSubscriptionHandler = topicSubscriptionHandler;
        this.outboundQueue = new LinkedBlockingQueue<>(avenueConfig.getOutboundQueueCapacity());
        this.offerTimeoutMillis = avenueConfig.getOutboundQueueOfferTimeoutMillis();
    }

    private void listen() throws IOException {
        try (DataInputStream dataInputStream = new DataInputStream(this.inputStream)) {
            // Length-prefix framing: read frames in a loop so a single connection can
            // carry many messages instead of blocking on readAllBytes() until EOF.
            while (this.running) {
                byte[] packetBytes = PacketFraming.readFrame(dataInputStream, avenueConfig.getPacketSize());
                JSONObject packet = packetDeserializer.deserialize(packetBytes);
                try {
                    inboundPacketHandler.handleInboundPacket(packet, this);
                } catch (IllegalArgumentException | JSONException e) {
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

    /**
     * Serially drains the outbound queue and writes frames to the socket. Runs on its own
     * virtual thread so that a slow or stalled socket never blocks packet delivery to other
     * clients. Exits when {@link #running} becomes false and the queue is drained, or on the
     * first write error.
     */
    private void writerLoop() {
        try {
            while (this.running || !outboundQueue.isEmpty()) {
                OutboundPacket packet = outboundQueue.poll(200, TimeUnit.MILLISECONDS);
                if (packet == null) {
                    continue;
                }
                byte[] serializedPacket = packetSerializer.serialize(packet);
                // Single, named place that prepends the 4-byte length prefix before the payload.
                PacketFraming.writeFrame(dataOutputStream, serializedPacket);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            log.warn("Outbound write failed, closing connection", e);
        } finally {
            shutdown();
        }
    }

    /**
     * Enqueues a packet for asynchronous delivery. Never blocks the caller on the socket.
     * If the per-client queue stays full beyond the configured offer timeout the client is
     * considered too slow and gets disconnected (slow-consumer backpressure policy).
     */
    public void enqueue(OutboundPacket packet) {
        if (!this.running) {
            return;
        }
        try {
            boolean accepted = outboundQueue.offer(packet, offerTimeoutMillis, TimeUnit.MILLISECONDS);
            if (!accepted) {
                log.warn("Outbound queue for {} is full, dropping slow client",
                        this.client.getInetAddress().getHostAddress());
                shutdown();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            shutdown();
        }
    }

    /**
     * Backwards-compatible direct-send entry point used by handlers that answer a request
     * inline (e.g. the auth-token response). Delegates to the asynchronous queue.
     */
    public void send(OutboundPacket packet) {
        enqueue(packet);
    }

    public synchronized void shutdown() {
        if (!this.running) {
            return;
        }
        this.running = false;
        log.info("Closing connection to {}", this.client.getInetAddress().getHostAddress());
        // Wake the writer so it can observe running == false and finish promptly.
        if (this.writerThread != null) {
            this.writerThread.interrupt();
        }
        try {
            this.inputStream.close();
            this.outputStream.close();
            this.client.close();
        } catch (IOException e) {
            log.warn("An error occurred while closing connection", e);
        } finally {
            this.topicSubscriptionHandler.unsubscribeFromAllTopics(this);
        }
    }

    @Override
    public void run() {
        // Start the dedicated outbound writer on a virtual thread before reading inbound data.
        this.writerThread = Thread.ofVirtual()
                .name("client-writer-" + client.getInetAddress().getHostAddress())
                .start(this::writerLoop);
        try {
            listen();
        } catch (IOException e) {
            log.error("An error occurred", e);
        } finally {
            shutdown();
        }
    }
}
