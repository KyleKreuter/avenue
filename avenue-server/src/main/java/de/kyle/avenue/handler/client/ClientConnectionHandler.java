package de.kyle.avenue.handler.client;

import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.handler.packet.InboundPacketHandler;
import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
import de.kyle.avenue.metrics.AvenueMetrics;
import de.kyle.avenue.proto.ClientEnvelope;
import de.kyle.avenue.serialization.PacketFraming;
import de.kyle.avenue.serialization.WireCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Handles a single client connection.
 * <p>
 * Outbound writes are decoupled from delivery via a bounded per-client queue and a dedicated
 * writer running on a virtual thread. Producers ({@link #enqueue(ClientEnvelope)}) never block
 * on the socket, so one slow subscriber can no longer cause head-of-line blocking for the
 * fan-out to all other subscribers. Only the writer thread touches the {@link DataOutputStream},
 * which is why no write lock is needed any more.
 * <p>
 * Wire format: the queue carries {@link OutboundFrame}s. High-rate publish fan-out enqueues a
 * {@link OutboundFrame.PreSerialized} whose payload bytes were serialized <em>once</em> in
 * {@link TopicSubscriptionHandler#deliverPacketToSubscribers} and shared across every subscriber
 * (encode-once fan-out, O(1) instead of O(N) serialization); the writer just frames the bytes via
 * {@link PacketFraming#writeFrameNoFlush}. Low-rate request/response answers (auth-token response,
 * subscribe-ack) enqueue a {@link OutboundFrame.Envelope} via {@link #enqueue(ClientEnvelope)};
 * the writer serializes those lazily via {@link WireCodec#encodeClient}. Inbound frames are read
 * raw and handed to the {@link InboundPacketHandler}, which decodes and dispatches them.
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

    /**
     * Size of the {@link BufferedOutputStream} the writer flushes once per batch. Large enough that a
     * full coalesced batch of small fan-out frames accumulates without an intermediate auto-flush.
     */
    private static final int OUTPUT_BUFFER_BYTES = 64 * 1024;

    /**
     * Size of the {@link BufferedInputStream} layered under the {@link DataInputStream}. Symmetric to
     * the buffered write side: it coalesces many small length-prefixed frames into far fewer read
     * syscalls under load, while still surfacing the {@link SocketTimeoutException} that drives the
     * idle-timeout cutoff (the buffer only fills on an actual read, so a truly idle socket still
     * blocks in {@code read()} and times out exactly as before).
     */
    private static final int INPUT_BUFFER_BYTES = 64 * 1024;

    private final Socket client;
    private final InputStream inputStream;
    private final OutputStream outputStream;
    private final DataOutputStream dataOutputStream;
    private final InboundPacketHandler inboundPacketHandler;
    private final TopicSubscriptionHandler topicSubscriptionHandler;
    private final AvenueConfig avenueConfig;
    private final AvenueMetrics metrics;
    private final Runnable onDisconnect;

    /** Bounded outbound queue. Backpressure is applied via offer() with a short timeout. */
    private final BlockingQueue<OutboundFrame> outboundQueue;
    private final long offerTimeoutMillis;
    private final BackpressurePolicy backpressurePolicy;
    private final long idleTimeoutMillis;
    /** Max frames the writer coalesces into one buffered flush (write-batching). 1 = legacy flush-per-frame. */
    private final int batchMaxFrames;
    private Thread writerThread;

    private volatile boolean running;

    /** Backwards-compatible constructor (no metrics, no disconnect callback). */
    public ClientConnectionHandler(
            Socket client,
            InboundPacketHandler inboundPacketHandler,
            AvenueConfig avenueConfig,
            TopicSubscriptionHandler topicSubscriptionHandler
    ) throws IOException {
        this(client, inboundPacketHandler, avenueConfig,
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
            InboundPacketHandler inboundPacketHandler,
            AvenueConfig avenueConfig,
            TopicSubscriptionHandler topicSubscriptionHandler,
            AvenueMetrics metrics,
            Runnable onDisconnect
    ) throws IOException {
        this.client = client;
        this.inputStream = client.getInputStream();
        this.outputStream = client.getOutputStream();
        // Write-batching: layer a BufferedOutputStream UNDER the DataOutputStream so the writer can
        // accumulate many fan-out frames (writeFrameNoFlush) and push them with a single flush() per
        // batch — far fewer write syscalls / TCP segments under load.
        this.dataOutputStream =
                new DataOutputStream(new BufferedOutputStream(this.outputStream, OUTPUT_BUFFER_BYTES));
        this.running = true;
        this.inboundPacketHandler = inboundPacketHandler;
        this.avenueConfig = avenueConfig;
        this.topicSubscriptionHandler = topicSubscriptionHandler;
        this.metrics = metrics;
        this.onDisconnect = onDisconnect;
        this.outboundQueue = new LinkedBlockingQueue<>(avenueConfig.getOutboundQueueCapacity());
        this.offerTimeoutMillis = avenueConfig.getOutboundQueueOfferTimeoutMillis();
        this.backpressurePolicy = avenueConfig.getBackpressurePolicy();
        this.idleTimeoutMillis = avenueConfig.getClientIdleTimeoutMillis();
        this.batchMaxFrames = Math.max(1, avenueConfig.getServerBatchMaxFrames());
    }

    private void listen() throws IOException {
        // Liveness: apply a read idle-timeout so dead/half-open connections are reaped.
        // 0 disables it (blocking read, original behaviour).
        if (idleTimeoutMillis > 0) {
            this.client.setSoTimeout((int) Math.min(idleTimeoutMillis, Integer.MAX_VALUE));
        }
        // Buffered read: coalesce many small frames into fewer read syscalls, symmetric to the
        // already-buffered write side. The idle-timeout still works: a buffered read only blocks (and
        // thus only throws SocketTimeoutException) when the buffer is empty and the socket has no
        // bytes — exactly the dead/idle case the timeout is meant to reap.
        try (DataInputStream dataInputStream =
                     new DataInputStream(new BufferedInputStream(this.inputStream, INPUT_BUFFER_BYTES))) {
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
                try {
                    // The handler decodes the raw protobuf frame and dispatches by oneof case.
                    inboundPacketHandler.handleInboundPacket(
                            packetBytes, avenueConfig.getPacketSize(), this);
                } catch (IllegalArgumentException e) {
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
        // Reused per-batch scratch list so a single iteration coalesces every already-queued frame
        // into one buffered flush (write-batching). batchMaxFrames - 1 because the blocking poll
        // already took the first frame of the batch.
        List<OutboundFrame> batch = new ArrayList<>(batchMaxFrames);
        try {
            while (this.running || !outboundQueue.isEmpty()) {
                OutboundFrame first = outboundQueue.poll(200, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }
                // Opportunistically grab everything ALREADY waiting (FIFO, no artificial wait) so one
                // flush amortizes the whole burst. At low load drainTo adds nothing and the batch is
                // a single frame — identical to the legacy per-frame flush, no added latency.
                writeFrameNoFlush(first);
                if (batchMaxFrames > 1) {
                    batch.clear();
                    outboundQueue.drainTo(batch, batchMaxFrames - 1);
                    for (OutboundFrame frame : batch) {
                        writeFrameNoFlush(frame);
                    }
                }
                // Exactly one flush per batch pushes the coalesced bytes in a single syscall. When the
                // queue is empty the buffer ends up empty, so the next lone frame still goes out
                // promptly.
                PacketFraming.flush(dataOutputStream);
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
     * Writes one outbound frame's bytes into the buffered stream WITHOUT flushing.
     * <p>
     * A {@link OutboundFrame.PreSerialized} (the publish fan-out path) is written verbatim — its
     * payload was already serialized once and shared across all subscribers. A
     * {@link OutboundFrame.Envelope} (the low-rate auth/subscribe-ack path) is serialized lazily here.
     */
    private void writeFrameNoFlush(OutboundFrame frame) throws IOException {
        byte[] payload = switch (frame) {
            case OutboundFrame.PreSerialized pre -> pre.payload();
            case OutboundFrame.Envelope env ->
                    WireCodec.encodeClient(env.envelope(), avenueConfig.getPacketSize());
        };
        PacketFraming.writeFrameNoFlush(dataOutputStream, payload);
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
    public void enqueue(ClientEnvelope envelope) {
        // Low-rate path (auth response / subscribe-ack): keep the typed envelope and let the writer
        // serialize it lazily. Per-frame serialization cost is irrelevant at this rate.
        enqueueFrame(new OutboundFrame.Envelope(envelope));
    }

    /**
     * Enqueues an already-serialized client frame for asynchronous delivery (encode-once fan-out).
     * The supplied bytes are the bare protobuf payload (no length prefix) of a {@link ClientEnvelope}
     * and are written verbatim by the writer. Used by
     * {@link TopicSubscriptionHandler#deliverPacketToSubscribers}, which serializes the
     * {@code PublishOutbound} envelope exactly once and hands the same immutable {@code byte[]} to
     * every subscriber. The bytes must never be mutated after being passed in, as they are shared.
     */
    public void enqueuePreSerialized(byte[] payload) {
        enqueueFrame(new OutboundFrame.PreSerialized(payload));
    }

    /**
     * Common enqueue with the configured backpressure policy. Never blocks the caller on the socket;
     * applies the bounded-queue offer timeout and the {@link BackpressurePolicy} on overflow, and
     * updates the delivery / drop / disconnect metrics exactly as before.
     */
    private void enqueueFrame(OutboundFrame frame) {
        if (!this.running) {
            return;
        }
        try {
            boolean accepted = outboundQueue.offer(frame, offerTimeoutMillis, TimeUnit.MILLISECONDS);
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
    public void send(ClientEnvelope envelope) {
        enqueue(envelope);
    }

    /**
     * Serializes a {@link ClientEnvelope} into its bare protobuf payload bytes (no length prefix)
     * once, applying the same oversized-payload guard the writer used to apply per frame. The
     * returned immutable {@code byte[]} is meant to be shared across all subscribers of a fan-out via
     * {@link #enqueuePreSerialized(byte[])}, so the publish envelope is serialized exactly once per
     * publish instead of once per subscriber.
     *
     * @param envelope the envelope to serialize
     * @param maxSize  the configured maximum payload size in bytes
     * @return the encoded payload bytes
     */
    public static byte[] encodeForFanOut(ClientEnvelope envelope, int maxSize) {
        return WireCodec.encodeClient(envelope, maxSize);
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
