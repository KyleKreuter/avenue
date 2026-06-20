package de.kyle.avenue.cluster;

import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
import de.kyle.avenue.metrics.ClusterMetrics;
import de.kyle.avenue.packet.Packet;
import de.kyle.avenue.packet.cluster.ClusterAckPacket;
import de.kyle.avenue.packet.cluster.ClusterGapPacket;
import de.kyle.avenue.packet.cluster.ClusterHeartbeatPacket;
import de.kyle.avenue.packet.cluster.ClusterInterestPacket;
import de.kyle.avenue.packet.cluster.ClusterInterestSyncPacket;
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
 * delivery layered on top of the Phase A transport and Phase D interest-based routing on top of
 * that.
 * <p>
 * A {@code PeerLink} is created around an already-authenticated socket (handshake performed by
 * {@link ClusterHandshake}). It owns virtual threads:
 * <ul>
 *   <li><b>Reader</b> – reads incoming frames and dispatches publishes / heartbeats / ACKs /
 *       resume / gap / interest packets. Runs a heartbeat watchdog and closes the link on
 *       timeout.</li>
 *   <li><b>Writer</b> – the single thread allowed to block. It pulls publish items handed over
 *       non-blockingly through the shared ingest queue, {@code append}s them to the per-target
 *       {@link ReplayBuffer} (which applies backpressure <em>here</em>, never on the local path)
 *       and writes them. It also drains a small control-frame queue (heartbeat / ack / resume /
 *       gap / backfill / interest) so those interleave with live publishes.</li>
 *   <li><b>Heartbeat sender</b> – periodically enqueues a heartbeat control frame.</li>
 *   <li><b>ACK sender</b> – periodically sends a cumulative {@link ClusterAckPacket} with the
 *       receiving-side contiguous high-water mark, but only when it advanced (no idle spam).</li>
 * </ul>
 * <h2>Phase D — per-link reliability sequence</h2>
 * Before Phase D, every peer received every origin sequence number, so the origin seq stream over a
 * link was dense and could drive cumulative ACK / backfill directly. With interest-based routing a
 * link only carries the publishes of the topics that peer is interested in, so the <em>origin</em>
 * seq stream it sees is sparse (full of holes) — which would break contiguous ACK/backfill.
 * <p>
 * The fix: the at-least-once layer keys on a separate, dense, per-link reliability sequence
 * ({@code linkSeq}) that the <b>sender</b> assigns per target (see
 * {@link ClusterManager.TargetState}). ACK / resume / gap / replay all operate on {@code linkSeq}
 * via the {@link ReceiverContext}. The publish frame still carries the
 * {@code (originNodeId, originEpoch, originSeq)} triple, which the receiver uses purely for
 * <b>deduplication</b> — the two sequence spaces are fully independent. So:
 * <ul>
 *   <li><b>Reliability</b> (no lost/duplicated frames on the wire): {@code linkSeq}, contiguous.</li>
 *   <li><b>Dedup</b> (never deliver the same logical message twice to local subscribers):
 *       {@code (origin, seq)}, unchanged from Phase C.</li>
 * </ul>
 */
public class PeerLink {

    private static final Logger log = LoggerFactory.getLogger(PeerLink.class);

    private static final int CONTROL_QUEUE_CAPACITY = 1024;

    /**
     * Callbacks the link uses to reach state owned by {@link ClusterManager} that must outlive any
     * single link.
     * <p>
     * Phase D split the receiving-side state into two concerns:
     * <ul>
     *   <li><b>Reliability</b> — keyed by the sending peer's node id over the dense per-link
     *       {@code linkSeq}: {@link #acceptLink}, {@link #contiguousLinkSeq}, {@link #resetLinkTo},
     *       {@link #linkResumePoint}. Drives cumulative ACK / resume / gap / backfill.</li>
     *   <li><b>Dedup</b> — keyed by {@code (originNodeId, originEpoch)} over the origin seq:
     *       {@link #acceptOrigin}. Ensures a given logical publish is delivered to local
     *       subscribers at most once even though it may arrive on this link more than once (e.g. a
     *       backfill re-send, or a broadcast-grace flood reaching an already-covered node).</li>
     * </ul>
     */
    public interface ReceiverContext {

        /**
         * Records a received {@code linkSeq} from the sending peer and reports whether it is new on
         * the wire (i.e. not a re-sent/duplicate frame). Drives the contiguous reliability frontier.
         */
        boolean acceptLink(String senderNodeId, long linkSeq);

        /** Current contiguous reliability high-water mark for the link from {@code senderNodeId}. */
        long contiguousLinkSeq(String senderNodeId);

        /** Resets the link reliability frontier to {@code newContiguous}, accepting loss below it. */
        void resetLinkTo(String senderNodeId, long newContiguous);

        /** Our resume point for this link: last contiguous {@code linkSeq} we received from the peer. */
        long linkResumePoint(String senderNodeId);

        /**
         * Deduplication check on the origin identity. Returns {@code true} if this is the first time
         * we have seen {@code (originKey, originSeq)} and it should be delivered, {@code false} if it
         * is a duplicate to drop.
         */
        boolean acceptOrigin(String originKey, long originSeq);

        /**
         * Applies a received interest update (delta) from a peer to the routing table. Called for
         * {@link ClusterInterestPacket}s arriving on this link.
         */
        void onInterestDelta(String nodeId, long version, String op, String topic);

        /** Applies a received full interest-state sync from a peer to the routing table. */
        void onInterestSync(String nodeId, long version, java.util.Set<String> topics);
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
     * <p>
     * Phase D: entries carry the sender-assigned per-link {@code linkSeq} (see
     * {@link ClusterManager.TargetState}); the {@link ReplayBuffer} keys on that.
     */
    private final BlockingQueue<ReplayBuffer.Entry> ingestQueue;
    private final ClusterMetrics metrics;
    private final Clock clock;
    private final Runnable onClose;
    /** Supplies the full interest-state sync to send on link-up and never block the caller. */
    private final java.util.function.Supplier<ClusterInterestSyncPacket> interestSyncSupplier;

    /** Control/backfill frames written verbatim by the writer (heartbeat, ack, resume, gap, replay). */
    private final BlockingQueue<OutboundItem> controlQueue = new LinkedBlockingQueue<>(CONTROL_QUEUE_CAPACITY);

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean closedOnce = new AtomicBoolean(false);

    /** Clock-time of the last received heartbeat; read by reader, written by reader only. */
    private long lastHeartbeatNanos;

    /** Last contiguous reliability high-water mark we ACKed to the remote, to suppress idle spam. */
    private long lastAckedSeqSent = -1L;

    /**
     * Strict-ordering reorder buffer over the dense per-link {@code linkSeq}: seqs received ahead of
     * the contiguous frontier, awaiting their predecessors. Touched only by the single reader
     * thread, so no lock is needed. Empty (and unused) in the default non-strict mode.
     */
    private final java.util.TreeMap<Long, ClusterPublishPacket> strictPending = new java.util.TreeMap<>();

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
            java.util.function.Supplier<ClusterInterestSyncPacket> interestSyncSupplier,
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
        this.interestSyncSupplier = interestSyncSupplier;
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
        // Phase D: immediately push our full interest state so the new peer routes correctly from
        // the very first publish (initial anti-entropy sync, analogous to the resume request).
        sendInitialInterestSync();
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

    /** Enqueues an arbitrary control packet (e.g. an interest delta/sync) for the writer to send. */
    public boolean enqueueControl(Packet packet) {
        if (!running.get()) {
            return false;
        }
        return controlQueue.offer(new OutboundItem.ControlFrame(packet));
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
            case "ClusterInterestPacket" -> handleInterest(envelope);
            case "ClusterInterestSyncPacket" -> handleInterestSync(envelope);
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
        // RELIABILITY first, over the dense per-link linkSeq: this is the contiguous wire stream the
        // ACK/backfill machinery tracks. A linkSeq we have already seen (a re-sent backfill frame, or
        // a frame already covered) is a wire duplicate and must not advance anything twice.
        boolean newOnWire = receiver.acceptLink(remoteNodeId, packet.getLinkSeq());

        // DEDUP over the origin identity, independent of linkSeq: even a wire-new frame may carry a
        // logical message we already delivered (e.g. it reached us once via interest routing and once
        // via a broadcast-grace flood from a different code path). Drop those for the subscriber.
        // ORDERING: cross-origin is intentionally unordered; within one origin FIFO holds in the
        // lossless path. Strict mode additionally holds forward gaps in the linkSeq stream.
        String originKey = packet.getOriginNodeId() + ":" + packet.getOriginEpoch();
        if (strictOrdering) {
            handlePublishStrict(originKey, packet, newOnWire);
        } else {
            if (!newOnWire) {
                metrics.incrementMessagesDeduped();
                return;
            }
            deliverIfNew(originKey, packet);
        }
    }

    /** Non-strict: deliver immediately; the origin tracker drops logical duplicates. */
    private void deliverIfNew(String originKey, ClusterPublishPacket packet) {
        if (!receiver.acceptOrigin(originKey, packet.getSeq())) {
            metrics.incrementMessagesDeduped();
            log.debug("Dropping duplicate origin={} seq={} from peer {}",
                    originKey, packet.getSeq(), remoteNodeId);
            return;
        }
        deliver(packet);
    }

    /**
     * Strict: buffer out-of-order {@code linkSeq}s in {@link #strictPending} and only deliver once
     * the link's contiguous frontier reaches them, so subscribers see an in-order stream. Logical
     * dedup over the origin identity still applies at delivery time.
     */
    private void handlePublishStrict(String originKey, ClusterPublishPacket packet, boolean newOnWire) {
        long linkSeq = packet.getLinkSeq();
        if (!newOnWire) {
            // Already accepted on the wire (duplicate/backfill): nothing to buffer.
            metrics.incrementMessagesDeduped();
            return;
        }
        // acceptLink already advanced the contiguous frontier for in-order arrivals. For forward
        // gaps it recorded the seq in its window but the frontier did not move; we still must hold
        // delivery until the gap fills. Buffer and flush in linkSeq order.
        long contiguous = receiver.contiguousLinkSeq(remoteNodeId);
        if (linkSeq <= contiguous) {
            // The frontier already covers this seq (it filled an immediate gap): deliver now.
            deliverIfNew(originKey, packet);
            flushStrictPending();
            return;
        }
        strictPending.put(linkSeq, packet);
        flushStrictPending();
    }

    /** Flushes every now-contiguous pending entry in linkSeq order. */
    private void flushStrictPending() {
        while (true) {
            long next = receiver.contiguousLinkSeq(remoteNodeId);
            ClusterPublishPacket head = strictPending.get(next);
            if (head == null) {
                // The contiguous frontier sits at `next`; the entry that fills `next` may itself be
                // pending under its own linkSeq. Deliver any pending entry whose linkSeq <= frontier.
                java.util.Map.Entry<Long, ClusterPublishPacket> first = strictPending.firstEntry();
                if (first == null || first.getKey() > next) {
                    break;
                }
                strictPending.pollFirstEntry();
                String ok = first.getValue().getOriginNodeId() + ":" + first.getValue().getOriginEpoch();
                deliverIfNew(ok, first.getValue());
                continue;
            }
            strictPending.remove(next);
            String ok = head.getOriginNodeId() + ":" + head.getOriginEpoch();
            deliverIfNew(ok, head);
        }
    }

    private void deliver(ClusterPublishPacket packet) {
        metrics.incrementMessagesReceived();
        PublishMessageOutboundPacket outbound = new PublishMessageOutboundPacket(
                packet.getTopic(), packet.getData(), packet.getSource());
        topicSubscriptionHandler.deliverPacketToSubscribers(packet.getTopic(), outbound);
    }

    /** Cumulative ACK from the remote: evict everything up to the acked {@code linkSeq} from our ring. */
    private void handleAck(JSONObject envelope) {
        ClusterAckPacket ack;
        try {
            ack = ClusterAckPacket.fromJson(envelope);
        } catch (Exception e) {
            log.warn("Malformed ClusterAckPacket from peer {}: {}", remoteNodeId, e.getMessage());
            return;
        }
        metrics.incrementAcksReceived();
        // The reliability identity for our outbound ring is THIS link (localNodeId -> remoteNodeId).
        // The peer echoes our local node id; any matching ack advances the ring head over linkSeq.
        for (ClusterAckPacket.Ack a : ack.getAcks()) {
            if (a.originNodeId().equals(localNodeId)) {
                replayBuffer.ackUpTo(a.ackedSeq());
            }
        }
    }

    /** Remote (the receiver) tells us where to resume; backfill from our replay buffer over linkSeq. */
    private void handleResume(JSONObject envelope) {
        ClusterResumePacket resume;
        try {
            resume = ClusterResumePacket.fromJson(envelope);
        } catch (Exception e) {
            log.warn("Malformed ClusterResumePacket from peer {}: {}", remoteNodeId, e.getMessage());
            return;
        }
        boolean handled = false;
        for (ClusterResumePacket.Resume r : resume.getResumes()) {
            // We only sourced frames over this link with reliability identity == localNodeId.
            if (r.originNodeId().equals(localNodeId)) {
                backfill(r.lastContiguousSeq());
                handled = true;
            }
        }
        // Fresh peer with no resume entry for us yet: backfill from 0 so it catches up.
        if (!handled) {
            backfill(0L);
        }
    }

    /**
     * Re-sends every buffered entry beyond {@code lastContiguousLinkSeq}, or signals a
     * {@link ClusterGapPacket} when the requested range has been evicted.
     */
    private void backfill(long lastContiguousLinkSeq) {
        ReplayBuffer.ReplayResult result = replayBuffer.replayFrom(lastContiguousLinkSeq);
        if (result.gap()) {
            log.info("Replay gap for link {} requested from {}, firstAvailable={} — sending gap",
                    localNodeId, lastContiguousLinkSeq, result.firstAvailableSeq());
            metrics.incrementClusterGapEvents();
            controlQueue.offer(new OutboundItem.ControlFrame(new ClusterGapPacket(
                    localNodeId, localNodeId, localEpoch, result.firstAvailableSeq())));
            return;
        }
        List<ReplayBuffer.Entry> entries = result.entries();
        if (entries.isEmpty()) {
            return;
        }
        log.info("Backfilling {} message(s) to peer {} from linkSeq>{}",
                entries.size(), remoteNodeId, lastContiguousLinkSeq);
        for (ReplayBuffer.Entry e : entries) {
            // Re-send verbatim; the receiver's link tracker drops anything it already accepted.
            controlQueue.offer(new OutboundItem.PreSerializedFrame(e.payload()));
        }
        metrics.addClusterBackfillMessages(entries.size());
    }

    /** Remote signalled that frames we expected are unrecoverable; resync the link frontier forward. */
    private void handleGap(JSONObject envelope) {
        ClusterGapPacket gap;
        try {
            gap = ClusterGapPacket.fromJson(envelope);
        } catch (Exception e) {
            log.warn("Malformed ClusterGapPacket from peer {}: {}", remoteNodeId, e.getMessage());
            return;
        }
        long newContiguous = gap.getFirstAvailableSeq() - 1;
        log.info("Accepting data loss on link from {}: resync contiguous linkSeq -> {}",
                remoteNodeId, newContiguous);
        receiver.resetLinkTo(remoteNodeId, newContiguous);
        metrics.incrementClusterGapEvents();
        if (strictOrdering) {
            flushStrictAfterGap();
        }
    }

    /** After a declared gap advanced the link frontier, drop stale pending entries and flush. */
    private void flushStrictAfterGap() {
        long contiguous = receiver.contiguousLinkSeq(remoteNodeId);
        strictPending.headMap(contiguous + 1).clear();
        flushStrictPending();
    }

    // -------------------------------------------------------------------------
    // Interest handling (Phase D, receiving side)
    // -------------------------------------------------------------------------

    private void handleInterest(JSONObject envelope) {
        ClusterInterestPacket interest;
        try {
            interest = ClusterInterestPacket.fromJson(envelope);
        } catch (Exception e) {
            log.warn("Malformed ClusterInterestPacket from peer {}: {}", remoteNodeId, e.getMessage());
            return;
        }
        metrics.incrementInterestUpdatesReceived();
        receiver.onInterestDelta(interest.getNodeId(), interest.getInterestVersion(),
                interest.getOp(), interest.getTopic());
    }

    private void handleInterestSync(JSONObject envelope) {
        ClusterInterestSyncPacket sync;
        try {
            sync = ClusterInterestSyncPacket.fromJson(envelope);
        } catch (Exception e) {
            log.warn("Malformed ClusterInterestSyncPacket from peer {}: {}", remoteNodeId, e.getMessage());
            return;
        }
        metrics.incrementInterestUpdatesReceived();
        receiver.onInterestSync(sync.getNodeId(), sync.getInterestVersion(), sync.getTopics());
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
                // — this is the ONLY place blocking is allowed) and send if it was buffered. The
                // entry's seq() is the per-link linkSeq assigned by the sender for THIS target.
                ReplayBuffer.Entry entry = ingestQueue.poll(didWork ? 0 : 100, TimeUnit.MILLISECONDS);
                if (entry != null) {
                    if (replayBuffer.append(entry.seq(), entry.payload())) {
                        writeItem(new OutboundItem.PreSerializedFrame(entry.payload()));
                    } else {
                        // Dropped under backpressure: tell the receiver to resync its contiguous
                        // link frontier forward past the lost linkSeq so it keeps ACKing.
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
     * Sends a cumulative ACK carrying the contiguous reliability high-water mark for this link, but
     * only when it advanced since the last ACK, to avoid idle spam. The reliability identity is the
     * sending peer's node id (remoteNodeId) over linkSeq.
     */
    private void sendAckIfAdvanced() {
        long contiguous = receiver.contiguousLinkSeq(remoteNodeId);
        if (contiguous <= lastAckedSeqSent) {
            return; // nothing new to acknowledge
        }
        lastAckedSeqSent = contiguous;
        ClusterAckPacket.Ack ack = new ClusterAckPacket.Ack(remoteNodeId, 0L, contiguous);
        if (controlQueue.offer(new OutboundItem.ControlFrame(
                new ClusterAckPacket(localNodeId, List.of(ack))))) {
            metrics.incrementAcksSent();
        }
    }

    private void sendResumeRequest() {
        // Our resume point for the link the remote sends to us: last contiguous linkSeq received.
        long contiguous = receiver.linkResumePoint(remoteNodeId);
        ClusterResumePacket.Resume point = new ClusterResumePacket.Resume(remoteNodeId, 0L, contiguous);
        controlQueue.offer(new OutboundItem.ControlFrame(
                new ClusterResumePacket(localNodeId, List.of(point))));
    }

    private void sendInitialInterestSync() {
        ClusterInterestSyncPacket sync = interestSyncSupplier.get();
        if (sync != null && controlQueue.offer(new OutboundItem.ControlFrame(sync))) {
            metrics.incrementInterestUpdatesSent();
        }
    }
}
