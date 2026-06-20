package de.kyle.avenue.handler.client;

import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.handler.packet.InboundPacketHandler;
import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
import de.kyle.avenue.metrics.AvenueMetrics;
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
import java.net.SocketTimeoutException;
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
 * <p>
 * Liveness (E16): when {@code server.client.idle-timeout-ms > 0} a {@link Socket#setSoTimeout
 * read timeout} is applied. If no byte arrives from the client within that window the read
 * throws a {@link SocketTimeoutException} and the connection is closed, so dead / half-open
 * connections are reaped instead of leaking forever.
 * <p>
 * Backpressure (E17): the {@link BackpressurePolicy} decides what happens when the bounded
 * outbound queue stays full beyond the offer timeout — either disconnect the slow consumer
 * (default) or drop the individual message and keep the connection.
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
    private final AvenueMetrics metrics;
    private final Runnable onDisconnect;

    /** Bounded outbound queue. Backpressure is applied via offer() with a short timeout. */
    private final BlockingQueue<OutboundPacket> outboundQueue;
    private final long offerTimeoutMillis;
    private final BackpressurePolicy backpressurePolicy;
    private final long idleTimeoutMillis;
    private Thread writerThread;

    private volatile boolean running;

    /** Backwards-compatible constructor (no metrics, no disconnect callback). */
    public ClientConnectionHandler(
            Socket client,
            PacketDeserializer packetDeserializer,
            PacketSerializer packetSerializer,
            InboundPacketHandler inboundPacketHandler,
            AvenueConfig avenueConfig,
            TopicSubscriptionHandler topicSubscriptionHandler
    ) throws IOException {
        this(client, packetDeserializer, packetSerializer, inboundPacketHandler, avenueConfig,
                topicSubscriptionHandler, new AvenueMetrics(), () -> { });
    }

    /**
     * Full constructor.
     *
     * @param metrics      shared metrics registry (delivery / drop / disconnect counters)
     * @param onDisconnect callback invoked exactly once when this connection closes, so the
     *                     server can decrement its active-connection gauge
     */
    public ClientConnectionHandler(
            Socket client,
            PacketDeserializer packetDeserializer,
            PacketSerializer packetSerializer,
            InboundPacketHandler inboundPacketHandler,
            AvenueConfig avenueConfig,
            TopicSubscriptionHandler topicSubscriptionHandler,
            AvenueMetrics metrics,
            Runnable onDisconnect
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
        this.metrics = metrics;
        this.onDisconnect = onDisconnect;
        this.outboundQueue = new LinkedBlockingQueue<>(avenueConfig.getOutboundQueueCapacity());
        this.offerTimeoutMillis = avenueConfig.getOutboundQueueOfferTimeoutMillis();
        this.backpressurePolicy = avenueConfig.getBackpressurePolicy();
        this.idleTimeoutMillis = avenueConfig.getClientIdleTimeoutMillis();
    }

    private void listen() throws IOException {
        // Liveness: apply a read idle-timeout so dead/half-open connections are reaped.
        // 0 disables it (blocking read, original behaviour).
        if (idleTimeoutMillis > 0) {
            this.client.setSoTimeout((int) Math.min(idleTimeoutMillis, Integer.MAX_VALUE));
        }
        try (DataInputStream dataInputStream = new DataInputStream(this.inputStream)) {
            // Length-prefix framing: read frames in a loop so a single connection can
            // carry many messages instead of blocking on readAllBytes() until EOF.
            while (this.running) {
                byte[] packetBytes;
                try {
                    packetBytes = PacketFraming.readFrame(dataInputStream, avenueConfig.getPacketSize());
                } catch (SocketTimeoutException e) {
                    // No bytes within the idle window -> treat the connection as dead and close it.
                    log.info("Idle timeout ({} ms) reached for {}, closing dead connection",
                            idleTimeoutMillis, remoteAddress());
                    break;
                }
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
     * <p>
     * If the per-client queue stays full beyond the configured offer timeout the configured
     * {@link BackpressurePolicy} decides the outcome:
     * <ul>
     *   <li>{@code DISCONNECT_SLOW_CONSUMER} (default) — the slow client is disconnected.</li>
     *   <li>{@code DROP_MESSAGE} — the individual packet is dropped, the connection stays open.</li>
     * </ul>
     */
    public void enqueue(OutboundPacket packet) {
        if (!this.running) {
            return;
        }
        try {
            boolean accepted = outboundQueue.offer(packet, offerTimeoutMillis, TimeUnit.MILLISECONDS);
            if (accepted) {
                metrics.incrementMessagesDelivered();
                metrics.recordOutboundQueueDepth(outboundQueue.size());
                return;
            }
            // Queue still full after the offer timeout: apply the configured backpressure policy.
            if (backpressurePolicy == BackpressurePolicy.DROP_MESSAGE) {
                metrics.incrementDroppedMessages();
                log.warn("Outbound queue for {} is full, dropping message (DROP_MESSAGE policy)",
                        remoteAddress());
            } else {
                metrics.incrementSlowConsumerDisconnects();
                log.warn("Outbound queue for {} is full, disconnecting slow consumer "
                        + "(DISCONNECT_SLOW_CONSUMER policy)", remoteAddress());
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
        log.info("Closing connection to {}", remoteAddress());
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
            // Decrement the server's active-connection gauge exactly once.
            this.onDisconnect.run();
        }
    }

    private String remoteAddress() {
        try {
            return this.client.getInetAddress().getHostAddress();
        } catch (Exception e) {
            return "unknown";
        }
    }

    @Override
    public void run() {
        // Start the dedicated outbound writer on a virtual thread before reading inbound data.
        this.writerThread = Thread.ofVirtual()
                .name("client-writer-" + remoteAddress())
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
