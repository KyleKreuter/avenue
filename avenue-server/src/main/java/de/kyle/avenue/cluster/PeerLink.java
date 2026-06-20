package de.kyle.avenue.cluster;

import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
import de.kyle.avenue.metrics.ClusterMetrics;
import de.kyle.avenue.packet.Packet;
import de.kyle.avenue.packet.cluster.ClusterAckPacket;
import de.kyle.avenue.packet.cluster.ClusterGapPacket;
import de.kyle.avenue.packet.cluster.ClusterHeartbeatPacket;
import de.kyle.avenue.packet.cluster.ClusterPublishPacket;
import de.kyle.avenue.packet.cluster.ClusterResumePacket;
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
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages a single peer-to-peer cluster link over a TCP socket, with Phase C at-least-once
 * delivery layered on top of the Phase A transport.
 * <p>
 * A {@code PeerLink} is created around an already-authenticated socket (handshake performed by
 * {@link ClusterHandshake}). It owns virtual threads:
 * <ul>
 *   <li><b>Reader</b> – reads incoming frames and dispatches publishes / heartbeats / ACKs /
 *       resume / gap packets. Runs a heartbeat watchdog and closes the link on timeout.</li>
 *   <li><b>Writer</b> – the single thread allowed to block. It pulls publish items handed over
 *       non-blockingly through the shared ingest queue, {@code append}s them to the per-target
 *       {@link ReplayBuffer} (which applies backpressure <em>here</em>, never on the local path)
 *       and writes them. It also drains a small control-frame queue (heartbeat / ack / resume /
 *       gap / backfill) so those interleave with live publishes.</li>
 *   <li><b>Heartbeat sender</b> – periodically enqueues a heartbeat control frame.</li>
 *   <li><b>ACK sender</b> – periodically sends a cumulative {@link ClusterAckPacket} with the
 *       receiving-side contiguous high-water mark, but only when it advanced (no idle spam).</li>
 * </ul>
 * <h2>Roles</h2>
 * A link is bidirectional, so it plays both roles at once:
 * <ul>
 *   <li><b>Sender</b> of this node's publishes towards the remote: backed by the
 *       {@link ReplayBuffer} (owned by {@link ClusterManager}, keyed by the remote node id, so it
 *       survives this link). Handles backfill when the remote sends a resume request, and gap
 *       signalling when the requested range is no longer buffered.</li>
 *   <li><b>Receiver</b> of the remote's publishes: applies ordered dedup via the per-origin
 *       {@link OriginSequenceTracker}s (owned by {@link ClusterManager}), sends cumulative ACKs and,
 *       on connect, a resume request so the remote can backfill.</li>
 * </ul>
 */
public class PeerLink {

    private static final Logger log = LoggerFactory.getLogger(PeerLink.class);

    private static final int CONTROL_QUEUE_CAPACITY = 1024;

    /**
     * Callbacks the link uses to reach the receiving-side state owned by {@link ClusterManager}:
     * the per-origin dedup/ordering trackers and the resume/gap bookkeeping. Keeping these in the
     * manager lets the trackers and replay buffers outlive any single link.
     */
    public interface ReceiverContext {

        /**
         * Records a received publish and returns whether it should be delivered now. Implementations
         * apply ordered dedup (and, in strict mode, reorder buffering) over the per-origin tracker.
         */
        boolean accept(String originKey, long seq);

        /** Returns the current contiguous high-water mark for the given origin (0 if unknown). */
        long contiguousSeq(String originKey);

        /** Resets the given origin's tracker to {@code newContiguous}, accepting loss below it. */
        void resetTo(String originKey, long newContiguous);

        /**
         * Returns the resume points this node wants the remote to backfill from: one
         * {@link ClusterResumePacket.Resume} per known origin, each carrying our last contiguous seq.
         * Empty on a fresh node with no prior cluster state.
         */
        List<ClusterResumePacket.Resume> resumePoints();
    }

    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    private final String remoteNodeId;
    private final String localNodeId;
    private final long localEpoch;
    private final int maxPacketSize;
    private final long heartbeatIntervalMs;
    private final long ackIntervalMs;
    private final boolean strictOrdering;
    private final TopicSubscriptionHandler topicSubscriptionHandler;
    private final ReceiverContext receiver;
    private final ReplayBuffer replayBuffer;
    /**
     * Per-target publish handoff queue, owned by {@link ClusterManager} so it (like the replay
     * buffer) survives a link teardown: messages published while this target is partitioned
     * accumulate here and are drained by the next link's writer. The writer is the only consumer.
     */
    private final BlockingQueue<ReplayBuffer.Entry> ingestQueue;
    private final ClusterMetrics metrics;
    private final Clock clock;
    private final Runnable onClose;

    /** Control/backfill frames written verbatim by the writer (heartbeat, ack, resume, gap, replay). */
    private final BlockingQueue<OutboundItem> controlQueue = new LinkedBlockingQueue<>(CONTROL_QUEUE_CAPACITY);

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean closedOnce = new AtomicBoolean(false);

    /** Clock-time of the last received heartbeat; read by reader, written by reader only. */
    private long lastHeartbeatNanos;

    /** Last contiguous high-water mark we ACKed to the remote, to suppress idle ACK spam. */
    private long lastAckedSeqSent = -1L;

    /**
     * Strict-ordering reorder buffer: per origin, the seqs received ahead of the contiguous
     * frontier, awaiting their predecessors. Touched only by the single reader thread, so no lock
     * is needed. Empty (and unused) in the default non-strict mode.
     */
    private final java.util.Map<String, java.util.TreeMap<Long, ClusterPublishPacket>> strictPending =
            new java.util.HashMap<>();

    /**
     * Full constructor with an injectable {@link Clock} for the heartbeat watchdog.
     */
    public PeerLink(
            Socket socket,
            String remoteNodeId,
            String localNodeId,
            long localEpoch,
            int maxPacketSize,
            long heartbeatIntervalMs,
            long ackIntervalMs,
            boolean strictOrdering,
            TopicSubscriptionHandler topicSubscriptionHandler,
            ReceiverContext receiver,
            ReplayBuffer replayBuffer,
            BlockingQueue<ReplayBuffer.Entry> ingestQueue,
            ClusterMetrics metrics,
            Clock clock,
            Runnable onClose
    ) throws IOException {
        this.socket = socket;
        this.in = new DataInputStream(socket.getInputStream());
        this.out = new DataOutputStream(socket.getOutputStream());
        this.remoteNodeId = remoteNodeId;
        this.localNodeId = localNodeId;
        this.localEpoch = localEpoch;
        this.maxPacketSize = maxPacketSize;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.ackIntervalMs = ackIntervalMs;
        this.strictOrdering = strictOrdering;
        this.topicSubscriptionHandler = topicSubscriptionHandler;
        this.receiver = receiver;
        this.replayBuffer = replayBuffer;
        this.ingestQueue = ingestQueue;
        this.metrics = metrics;
        this.clock = clock;
        this.onClose = onClose;
    }

    /** Starts the reader, writer, heartbeat-sender and ack-sender virtual threads. */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        lastHeartbeatNanos = clock.nanoTime();
        // Send our resume request first so the remote can backfill before live traffic flows.
        sendResumeRequest();
        Thread.ofVirtual().name("cluster-writer-" + remoteNodeId).start(this::writerLoop);
        Thread.ofVirtual().name("cluster-reader-" + remoteNodeId).start(this::readerLoop);
        Thread.ofVirtual().name("cluster-hb-sender-" + remoteNodeId).start(this::heartbeatSenderLoop);
        Thread.ofVirtual().name("cluster-ack-sender-" + remoteNodeId).start(this::ackSenderLoop);
    }

    public String getRemoteNodeId() {
        return remoteNodeId;
    }

    public boolean isRunning() {
        return running.get();
    }

    /**
     * Closes the link and releases all resources; invokes {@code onClose} exactly once. The shared
     * ingest queue and replay buffer are NOT cleared here: they belong to {@link ClusterManager} and
     * must survive the link so a reconnect can resume and backfill.
     */
    public void close() {
        if (!closedOnce.compareAndSet(false, true)) {
            return;
        }
        running.set(false);
        log.info("Closing cluster link to peer {}", remoteNodeId);
        controlQueue.clear();
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
                if (clock.nanoTime() - lastHeartbeatNanos > watchdogNanos) {
                    log.warn("Heartbeat timeout from peer {}, closing link", remoteNodeId);
                    break;
                }
                byte[] frame;
                try {
                    socket.setSoTimeout((int) heartbeatIntervalMs);
                    frame = PacketFraming.readFrame(in, maxPacketSize);
                } catch (java.net.SocketTimeoutException e) {
                    continue;
                } catch (EOFException | SocketException e) {
                    break;
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
            case "ClusterAckPacket" -> handleAck(envelope);
            case "ClusterResumePacket" -> handleResume(envelope);
            case "ClusterGapPacket" -> handleGap(envelope);
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
        // ORDERING: cross-origin is intentionally unordered — each origin has an independent
        // sequence space, so there is no defined order between messages from different origins.
        // Within one origin, FIFO is naturally preserved in the lossless path by the monotonic seq
        // over FIFO TCP. The strict-ordering flag additionally holds back forward gaps until the
        // missing seqs fill (or are declared lost via a gap), at the cost of a small reorder buffer.
        String originKey = packet.getOriginNodeId() + ":" + packet.getOriginEpoch();
        if (strictOrdering) {
            handlePublishStrict(originKey, packet);
        } else {
            deliverIfNew(originKey, packet);
        }
    }

    /** Non-strict: deliver immediately; the tracker drops duplicates and tolerates forward gaps. */
    private void deliverIfNew(String originKey, ClusterPublishPacket packet) {
        if (!receiver.accept(originKey, packet.getSeq())) {
            metrics.incrementMessagesDeduped();
            log.debug("Dropping duplicate origin={} seq={} from peer {}",
                    originKey, packet.getSeq(), remoteNodeId);
            return;
        }
        deliver(packet);
    }

    /**
     * Strict: buffer out-of-order seqs in {@link #strictPending} and only deliver once the origin's
     * contiguous frontier reaches them, so subscribers see a per-origin in-order stream. Duplicates
     * (seq &lt;= contiguous, or already pending) are dropped. A forward gap is filled by backfill;
     * a declared-lost gap (via {@link ClusterGapPacket}) advances the frontier and flushes.
     */
    private void handlePublishStrict(String originKey, ClusterPublishPacket packet) {
        long seq = packet.getSeq();
        long contiguous = receiver.contiguousSeq(originKey);
        var pending = strictPending.computeIfAbsent(originKey, k -> new java.util.TreeMap<>());

        if (seq <= contiguous || pending.containsKey(seq)) {
            metrics.incrementMessagesDeduped();
            return;
        }
        pending.put(seq, packet);
        // Flush every now-contiguous entry in order.
        while (true) {
            long next = receiver.contiguousSeq(originKey) + 1;
            ClusterPublishPacket head = pending.remove(next);
            if (head == null) {
                break;
            }
            if (receiver.accept(originKey, next)) {
                deliver(head);
            } else {
                metrics.incrementMessagesDeduped();
            }
        }
    }

    private void deliver(ClusterPublishPacket packet) {
        metrics.incrementMessagesReceived();
        PublishMessageOutboundPacket outbound = new PublishMessageOutboundPacket(
                packet.getTopic(), packet.getData(), packet.getSource());
        topicSubscriptionHandler.deliverPacketToSubscribers(packet.getTopic(), outbound);
    }

    /** Cumulative ACK from the remote: evict everything up to ackedSeq from our replay buffer. */
    private void handleAck(JSONObject envelope) {
        ClusterAckPacket ack;
        try {
            ack = ClusterAckPacket.fromJson(envelope);
        } catch (Exception e) {
            log.warn("Malformed ClusterAckPacket from peer {}: {}", remoteNodeId, e.getMessage());
            return;
        }
        metrics.incrementAcksReceived();
        for (ClusterAckPacket.Ack a : ack.getAcks()) {
            // Single counter source: our outbound buffer holds only origin=self, so any ack for
            // self applies. We accept all entries cumulatively (origin filter is the manager's job;
            // here the buffer simply advances its head).
            if (a.originNodeId().equals(localNodeId)) {
                replayBuffer.ackUpTo(a.ackedSeq());
            }
        }
    }

    /** Remote (the receiver) tells us where to resume; backfill from our replay buffer. */
    private void handleResume(JSONObject envelope) {
        ClusterResumePacket resume;
        try {
            resume = ClusterResumePacket.fromJson(envelope);
        } catch (Exception e) {
            log.warn("Malformed ClusterResumePacket from peer {}: {}", remoteNodeId, e.getMessage());
            return;
        }
        for (ClusterResumePacket.Resume r : resume.getResumes()) {
            // We only sourced messages with origin == self over this link.
            if (!r.originNodeId().equals(localNodeId)) {
                continue;
            }
            backfill(r.originEpoch(), r.lastContiguousSeq());
        }
        // If the remote sent no resume entry for us yet (fresh node), still backfill from 0 so a
        // peer that connected after we already produced messages catches up.
        if (resume.getResumes().stream().noneMatch(r -> r.originNodeId().equals(localNodeId))) {
            backfill(localEpoch, 0L);
        }
    }

    /**
     * Re-sends every buffered entry beyond {@code lastContiguousSeq}, or signals a
     * {@link ClusterGapPacket} when the requested range has been evicted.
     */
    private void backfill(long originEpoch, long lastContiguousSeq) {
        ReplayBuffer.ReplayResult result = replayBuffer.replayFrom(lastContiguousSeq);
        if (result.gap()) {
            log.info("Replay gap for origin {}:{} requested from {}, firstAvailable={} — sending gap",
                    localNodeId, originEpoch, lastContiguousSeq, result.firstAvailableSeq());
            metrics.incrementClusterGapEvents();
            controlQueue.offer(new OutboundItem.ControlFrame(new ClusterGapPacket(
                    localNodeId, localNodeId, originEpoch, result.firstAvailableSeq())));
            return;
        }
        List<ReplayBuffer.Entry> entries = result.entries();
        if (entries.isEmpty()) {
            return;
        }
        log.info("Backfilling {} message(s) to peer {} from seq>{}",
                entries.size(), remoteNodeId, lastContiguousSeq);
        for (ReplayBuffer.Entry e : entries) {
            // Re-send verbatim; the receiver's tracker drops anything it already delivered.
            controlQueue.offer(new OutboundItem.PreSerializedFrame(e.payload()));
        }
        metrics.addClusterBackfillMessages(entries.size());
    }

    /** Remote signalled that messages we expected are unrecoverable; resync forward. */
    private void handleGap(JSONObject envelope) {
        ClusterGapPacket gap;
        try {
            gap = ClusterGapPacket.fromJson(envelope);
        } catch (Exception e) {
            log.warn("Malformed ClusterGapPacket from peer {}: {}", remoteNodeId, e.getMessage());
            return;
        }
        String originKey = gap.getOriginNodeId() + ":" + gap.getOriginEpoch();
        long newContiguous = gap.getFirstAvailableSeq() - 1;
        log.info("Accepting data loss for origin {}: resync contiguousSeq -> {}", originKey, newContiguous);
        receiver.resetTo(originKey, newContiguous);
        metrics.incrementClusterGapEvents();
        if (strictOrdering) {
            flushStrictAfterGap(originKey);
        }
    }

    /**
     * After a declared gap advanced the frontier, drop now-stale pending entries and flush any that
     * became contiguous, so strict-mode delivery resumes past the lost range.
     */
    private void flushStrictAfterGap(String originKey) {
        var pending = strictPending.get(originKey);
        if (pending == null) {
            return;
        }
        pending.headMap(receiver.contiguousSeq(originKey) + 1).clear();
        while (true) {
            long next = receiver.contiguousSeq(originKey) + 1;
            ClusterPublishPacket head = pending.remove(next);
            if (head == null) {
                break;
            }
            if (receiver.accept(originKey, next)) {
                deliver(head);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Writer loop
    // -------------------------------------------------------------------------

    private void writerLoop() {
        try {
            while (running.get()) {
                // Control / backfill frames first (low rate, includes ordering-critical replays).
                OutboundItem control;
                boolean didWork = false;
                while ((control = controlQueue.poll()) != null) {
                    writeItem(control);
                    didWork = true;
                }
                // Then one live publish: append to the replay buffer (may block under backpressure
                // — this is the ONLY place blocking is allowed) and send if it was buffered.
                ReplayBuffer.Entry entry = ingestQueue.poll(didWork ? 0 : 100, TimeUnit.MILLISECONDS);
                if (entry != null) {
                    if (replayBuffer.append(entry.seq(), entry.payload())) {
                        writeItem(new OutboundItem.PreSerializedFrame(entry.payload()));
                    } else {
                        // The entry was dropped under backpressure (ring full past the offer
                        // timeout, or DROP_GAP). Tell the receiver to resync its contiguous frontier
                        // forward past the lost seq so it can keep ACKing — otherwise its
                        // high-water mark would stall and the ring could never drain again.
                        writeItem(new OutboundItem.ControlFrame(new ClusterGapPacket(
                                localNodeId, localNodeId, localEpoch, entry.seq() + 1)));
                    }
                }
            }
            // Drain any remaining control frames after stop is signalled.
            OutboundItem remaining;
            while ((remaining = controlQueue.poll()) != null) {
                writeItem(remaining);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            if (running.get()) {
                log.warn("Cluster writer error to peer {}: {}", remoteNodeId, e.getMessage());
            }
        } finally {
            close();
        }
    }

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
                if (!controlQueue.offer(new OutboundItem.ControlFrame(hb))) {
                    log.warn("Queue to peer {} full, heartbeat dropped", remoteNodeId);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // -------------------------------------------------------------------------
    // ACK sender (receiving side)
    // -------------------------------------------------------------------------

    private void ackSenderLoop() {
        try {
            while (running.get()) {
                Thread.sleep(ackIntervalMs);
                if (!running.get()) {
                    break;
                }
                sendAckIfAdvanced();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Sends a cumulative ACK for every origin we have received from, but only when the aggregate
     * high-water mark advanced since the last ACK, to avoid idle spam. The remote's publishes carry
     * origin == remoteNodeId in the single-hop topology, so we ack that origin's tracker.
     */
    private void sendAckIfAdvanced() {
        List<ClusterResumePacket.Resume> points = receiver.resumePoints();
        long maxSeq = -1L;
        java.util.List<ClusterAckPacket.Ack> acks = new java.util.ArrayList<>();
        for (ClusterResumePacket.Resume r : points) {
            acks.add(new ClusterAckPacket.Ack(r.originNodeId(), r.originEpoch(), r.lastContiguousSeq()));
            maxSeq = Math.max(maxSeq, r.lastContiguousSeq());
        }
        if (acks.isEmpty() || maxSeq <= lastAckedSeqSent) {
            return; // nothing received yet, or nothing new to acknowledge
        }
        lastAckedSeqSent = maxSeq;
        if (controlQueue.offer(new OutboundItem.ControlFrame(new ClusterAckPacket(localNodeId, acks)))) {
            metrics.incrementAcksSent();
        }
    }

    private void sendResumeRequest() {
        List<ClusterResumePacket.Resume> points = receiver.resumePoints();
        controlQueue.offer(new OutboundItem.ControlFrame(new ClusterResumePacket(localNodeId, points)));
    }
}
