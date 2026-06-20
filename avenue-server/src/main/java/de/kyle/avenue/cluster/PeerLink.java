package de.kyle.avenue.cluster;

import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
import de.kyle.avenue.metrics.ClusterMetrics;
import de.kyle.avenue.packet.Packet;
import de.kyle.avenue.packet.cluster.ClusterHeartbeatPacket;
import de.kyle.avenue.packet.cluster.ClusterPublishPacket;
import de.kyle.avenue.packet.publish.PublishMessageOutboundPacket;
import de.kyle.avenue.serialization.PacketFraming;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages a single peer-to-peer cluster link over a TCP socket.
 * <p>
 * A {@code PeerLink} is created around an already-authenticated socket (handshake was
 * performed externally by {@link ClusterHandshake}). It owns three virtual threads:
 * <ul>
 *   <li><b>Reader</b> – reads incoming frames and dispatches
 *       {@link ClusterPublishPacket} / {@link ClusterHeartbeatPacket}. Monitors a
 *       heartbeat watchdog and closes the link on timeout.</li>
 *   <li><b>Writer</b> – drains a bounded outbound queue of {@link OutboundItem}s and writes them
 *       to the socket. {@link OutboundItem.PreSerializedFrame}s are written verbatim (no re-parse,
 *       no re-serialize); {@link OutboundItem.ControlFrame}s are serialized lazily.</li>
 *   <li><b>Heartbeat sender</b> – periodically enqueues a heartbeat control frame.</li>
 * </ul>
 * The link signals its own closure via an {@code onClose} {@link Runnable} so the owning
 * {@link ClusterManager} can schedule a reconnect.
 */
public class PeerLink {

    private static final Logger log = LoggerFactory.getLogger(PeerLink.class);

    private static final int OUTBOUND_QUEUE_CAPACITY = 1024;

    /**
     * Decides whether a received cluster publish (identified by its origin key and sequence
     * number) should be delivered. Implemented by {@link ClusterManager} over its per-origin
     * {@link OriginSequenceTracker}s; returns {@code false} for duplicates.
     */
    @FunctionalInterface
    public interface OriginAcceptor {
        boolean accept(String originKey, long seq);
    }

    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    private final String remoteNodeId;
    private final String localNodeId;
    private final int maxPacketSize;
    private final long heartbeatIntervalMs;
    private final TopicSubscriptionHandler topicSubscriptionHandler;
    private final OriginAcceptor originAcceptor;
    private final ClusterMetrics metrics;
    private final Clock clock;
    private final Runnable onClose;

    /** Bounded per-peer outbound queue carrying pre-serialized data frames and control frames. */
    private final BlockingQueue<OutboundItem> outboundQueue = new LinkedBlockingQueue<>(OUTBOUND_QUEUE_CAPACITY);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean closedOnce = new AtomicBoolean(false);

    /** Clock-time of the last received heartbeat; read by reader, written by reader only. */
    private long lastHeartbeatNanos;

    /**
     * Creates a {@code PeerLink} around an already-open, authenticated socket using the system
     * clock. Call {@link #start()} to begin the reader / writer threads.
     */
    public PeerLink(
            Socket socket,
            String remoteNodeId,
            String localNodeId,
            int maxPacketSize,
            long heartbeatIntervalMs,
            TopicSubscriptionHandler topicSubscriptionHandler,
            OriginAcceptor originAcceptor,
            ClusterMetrics metrics,
            Runnable onClose
    ) throws IOException {
        this(socket, remoteNodeId, localNodeId, maxPacketSize, heartbeatIntervalMs,
                topicSubscriptionHandler, originAcceptor, metrics, Clock.SYSTEM, onClose);
    }

    /**
     * Full constructor with an injectable {@link Clock} for the heartbeat watchdog. The default
     * public constructor delegates here with {@link Clock#SYSTEM}; tests use this overload to
     * drive the watchdog deterministically.
     */
    public PeerLink(
            Socket socket,
            String remoteNodeId,
            String localNodeId,
            int maxPacketSize,
            long heartbeatIntervalMs,
            TopicSubscriptionHandler topicSubscriptionHandler,
            OriginAcceptor originAcceptor,
            ClusterMetrics metrics,
            Clock clock,
            Runnable onClose
    ) throws IOException {
        this.socket = socket;
        this.in = new DataInputStream(socket.getInputStream());
        this.out = new DataOutputStream(socket.getOutputStream());
        this.remoteNodeId = remoteNodeId;
        this.localNodeId = localNodeId;
        this.maxPacketSize = maxPacketSize;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.topicSubscriptionHandler = topicSubscriptionHandler;
        this.originAcceptor = originAcceptor;
        this.metrics = metrics;
        this.clock = clock;
        this.onClose = onClose;
    }

    /**
     * Starts the reader, writer and heartbeat-sender virtual threads.
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        lastHeartbeatNanos = clock.nanoTime();
        Thread.ofVirtual().name("cluster-writer-" + remoteNodeId).start(this::writerLoop);
        Thread.ofVirtual().name("cluster-reader-" + remoteNodeId).start(this::readerLoop);
        Thread.ofVirtual().name("cluster-hb-sender-" + remoteNodeId).start(this::heartbeatSenderLoop);
    }

    /** Returns the remote peer's node ID. */
    public String getRemoteNodeId() {
        return remoteNodeId;
    }

    /** Returns {@code true} if the link is currently connected and running. */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Enqueues a pre-serialized cluster publish frame for async forwarding to the peer. The same
     * immutable {@link OutboundItem.PreSerializedFrame} instance is safe to hand to many peer
     * links. If the queue is full the frame is silently dropped (at-most-once delivery) and the
     * drop is counted.
     */
    public void forward(OutboundItem.PreSerializedFrame frame) {
        if (!running.get()) {
            return;
        }
        if (!outboundQueue.offer(frame)) {
            metrics.incrementMessagesDropped();
            log.warn("Cluster outbound queue to {} is full, dropping publish frame", remoteNodeId);
        }
    }

    /**
     * Closes the link and releases all resources. Invokes the {@code onClose} callback
     * exactly once.
     */
    public void close() {
        if (!closedOnce.compareAndSet(false, true)) {
            return;
        }
        running.set(false);
        log.info("Closing cluster link to peer {}", remoteNodeId);
        // Unblock the writer.
        outboundQueue.clear();
        try {
            socket.close();
        } catch (IOException e) {
            log.warn("Error closing cluster socket to {}: {}", remoteNodeId, e.getMessage());
        }
        onClose.run();
    }

    // -------------------------------------------------------------------------
    // Reader loop
    // -------------------------------------------------------------------------

    private void readerLoop() {
        long watchdogNanos = heartbeatIntervalMs * 3 * 1_000_000L;
        try {
            while (running.get()) {
                // Heartbeat watchdog check before blocking read.
                if (clock.nanoTime() - lastHeartbeatNanos > watchdogNanos) {
                    log.warn("Heartbeat timeout from peer {}, closing link", remoteNodeId);
                    break;
                }

                byte[] frame;
                try {
                    // Set a socket read timeout so we can periodically check the watchdog.
                    socket.setSoTimeout((int) heartbeatIntervalMs);
                    frame = PacketFraming.readFrame(in, maxPacketSize);
                } catch (java.net.SocketTimeoutException e) {
                    // Timed-out read: loop to re-check heartbeat watchdog.
                    continue;
                } catch (EOFException | SocketException e) {
                    break; // Normal closure
                } catch (IllegalArgumentException e) {
                    log.warn("Invalid frame from peer {}: {}", remoteNodeId, e.getMessage());
                    break;
                }

                JSONObject envelope = safeParseJson(frame);
                if (envelope == null) {
                    continue;
                }
                dispatch(envelope);
            }
        } catch (IOException e) {
            if (running.get()) {
                log.warn("Cluster reader error from peer {}: {}", remoteNodeId, e.getMessage());
            }
        } finally {
            close();
        }
    }

    private JSONObject safeParseJson(byte[] frame) {
        try {
            return new JSONObject(new String(frame, StandardCharsets.UTF_8));
        } catch (Exception e) {
            log.warn("Malformed JSON frame from peer {}", remoteNodeId);
            return null;
        }
    }

    private void dispatch(JSONObject envelope) {
        JSONObject header = envelope.optJSONObject("header");
        if (header == null) {
            return;
        }
        String name = header.optString("name", "");
        switch (name) {
            case "ClusterPublishPacket" -> handleClusterPublish(envelope);
            case "ClusterHeartbeatPacket" -> lastHeartbeatNanos = clock.nanoTime();
            default -> log.debug("Unknown cluster packet '{}' from peer {}, ignoring", name, remoteNodeId);
        }
    }

    private void handleClusterPublish(JSONObject envelope) {
        ClusterPublishPacket packet;
        try {
            packet = ClusterPublishPacket.fromJson(envelope);
        } catch (Exception e) {
            log.warn("Malformed ClusterPublishPacket from peer {}: {}", remoteNodeId, e.getMessage());
            return;
        }

        // Ordered deduplication keyed by (originNodeId, originEpoch). The single-hop full-mesh
        // rule means each message normally arrives once; this filters loops/replays defensively.
        String originKey = packet.getOriginNodeId() + ":" + packet.getOriginEpoch();
        if (!originAcceptor.accept(originKey, packet.getSeq())) {
            metrics.incrementMessagesDeduped();
            log.debug("Dropping duplicate origin={} seq={} from peer {}",
                    originKey, packet.getSeq(), remoteNodeId);
            return;
        }

        metrics.incrementMessagesReceived();

        // Deliver locally only; never re-forward (single-hop rule).
        PublishMessageOutboundPacket outbound = new PublishMessageOutboundPacket(
                packet.getTopic(),
                packet.getData(),
                packet.getSource()
        );
        topicSubscriptionHandler.deliverPacketToSubscribers(packet.getTopic(), outbound);
    }

    // -------------------------------------------------------------------------
    // Writer loop
    // -------------------------------------------------------------------------

    private void writerLoop() {
        try {
            while (running.get()) {
                OutboundItem item;
                try {
                    item = outboundQueue.poll(200, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                if (item == null) {
                    continue;
                }
                writeItem(item);
            }
            // Drain remaining queued items after stop is signalled.
            OutboundItem remaining;
            while ((remaining = outboundQueue.poll()) != null) {
                writeItem(remaining);
            }
        } catch (IOException e) {
            if (running.get()) {
                log.warn("Cluster writer error to peer {}: {}", remoteNodeId, e.getMessage());
            }
        } finally {
            close();
        }
    }

    /**
     * Writes a single outbound item. Pre-serialized data frames are written verbatim; control
     * frames are serialized to an envelope here (low-rate traffic only).
     */
    private void writeItem(OutboundItem item) throws IOException {
        byte[] payload = switch (item) {
            case OutboundItem.PreSerializedFrame frame -> frame.payload();
            case OutboundItem.ControlFrame control -> serializeEnvelope(control.packet());
        };
        synchronized (out) {
            PacketFraming.writeFrame(out, payload);
        }
    }

    /** Serializes a {@link Packet} into the {@code {"header":{...},"body":{...}}} envelope bytes. */
    static byte[] serializeEnvelope(Packet packet) {
        JSONObject envelope = new JSONObject();
        envelope.put("header", new JSONObject(new String(packet.getHeader(), StandardCharsets.UTF_8)));
        envelope.put("body", new JSONObject(new String(packet.getBody(), StandardCharsets.UTF_8)));
        return envelope.toString().getBytes(StandardCharsets.UTF_8);
    }

    // -------------------------------------------------------------------------
    // Heartbeat sender
    // -------------------------------------------------------------------------

    private void heartbeatSenderLoop() {
        try {
            while (running.get()) {
                Thread.sleep(heartbeatIntervalMs);
                if (!running.get()) {
                    break;
                }
                ClusterHeartbeatPacket hb = new ClusterHeartbeatPacket(localNodeId);
                if (!outboundQueue.offer(new OutboundItem.ControlFrame(hb))) {
                    log.warn("Queue to peer {} full, heartbeat dropped", remoteNodeId);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
