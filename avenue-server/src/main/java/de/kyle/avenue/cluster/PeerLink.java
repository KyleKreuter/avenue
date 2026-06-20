package de.kyle.avenue.cluster;

import de.kyle.avenue.cluster.events.ClusterEvents;
import de.kyle.avenue.cluster.membership.SwimMessageSink;
import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
import de.kyle.avenue.metrics.ClusterMetrics;
import de.kyle.avenue.proto.ClientEnvelope;
import de.kyle.avenue.proto.ClusterAck;
import de.kyle.avenue.proto.ClusterEnvelope;
import de.kyle.avenue.proto.ClusterGap;
import de.kyle.avenue.proto.ClusterInterest;
import de.kyle.avenue.proto.ClusterInterestSync;
import de.kyle.avenue.proto.ClusterPublish;
import de.kyle.avenue.proto.ClusterResume;
import de.kyle.avenue.proto.SwimAck;
import de.kyle.avenue.proto.SwimJoin;
import de.kyle.avenue.proto.SwimLeave;
import de.kyle.avenue.proto.SwimPing;
import de.kyle.avenue.proto.SwimPingReq;
import de.kyle.avenue.proto.SwimPingReqAck;
import de.kyle.avenue.serialization.ClientEnvelopes;
import de.kyle.avenue.serialization.PacketFraming;
import de.kyle.avenue.serialization.WireCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages a single peer-to-peer cluster link over a TCP socket, with Phase C at-least-once
 * delivery layered on top of the Phase A transport and Phase D interest-based routing on top of
 * that.
 * <p>
 * Wire format (Step 2 of the JSON-&gt;protobuf migration): the 4-byte length framing
 * ({@link PacketFraming}) is unchanged, but each frame's payload is now a serialized
 * {@link ClusterEnvelope} (protobuf) instead of a UTF-8 JSON {@code {"header","body"}} object.
 * Dispatch switches on the envelope's {@code oneof} case ({@link ClusterEnvelope#getMsgCase()})
 * rather than a {@code header.name} string.
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
 *   <li><b>ACK sender</b> – periodically sends a cumulative {@link ClusterAck} with the
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
     * Size of the {@link BufferedOutputStream} the writer flushes once per batch. Large enough that a
     * full coalesced batch of small publish/control frames accumulates without an intermediate
     * auto-flush, so the batch turns into one (or few) write syscalls / TCP segments.
     */
    private static final int OUTPUT_BUFFER_BYTES = 64 * 1024;

    /**
     * Idle ingest-poll budget for the writer loop. Kept small so a control frame the reader enqueues
     * — notably a time-sensitive SWIM ack (Phase E) — is flushed within a few ms instead of waiting
     * out a long publish poll. Virtual threads make the brief parking negligible.
     */
    private static final long CONTROL_IDLE_POLL_MS = 5L;

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
         * {@link ClusterInterest} envelopes arriving on this link.
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
    /** Max frames the writer coalesces into one flush (write-batching). 1 = legacy per-frame flush. */
    private final int batchMaxFrames;
    /** Optional linger window (micros) the writer parks to accumulate more frames. 0 = opportunistic. */
    private final long batchMaxDelayMicros;
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
    /** Supplies the full interest-state sync envelope to send on link-up and never block the caller. */
    private final java.util.function.Supplier<ClusterEnvelope> interestSyncSupplier;

    /**
     * Phase E — receiving-side SWIM membership sink. Nullable (single-node / cluster-disabled). Set
     * via {@link #setSwimSink(SwimMessageSink)} right after construction so the large existing
     * constructor signature stays untouched.
     */
    private volatile SwimMessageSink swimSink;

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
    private final java.util.TreeMap<Long, ClusterPublish> strictPending = new java.util.TreeMap<>();

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
            int batchMaxFrames,
            long batchMaxDelayMicros,
            TopicSubscriptionHandler topicSubscriptionHandler,
            ReceiverContext receiver,
            ReplayBuffer replayBuffer,
            BlockingQueue<ReplayBuffer.Entry> ingestQueue,
            ClusterMetrics metrics,
            Clock clock,
            java.util.function.Supplier<ClusterEnvelope> interestSyncSupplier,
            Runnable onClose
    ) throws IOException {
        this.socket = socket;
        this.in = new DataInputStream(socket.getInputStream());
        // Write-batching: layer a BufferedOutputStream UNDER the DataOutputStream so the writer can
        // accumulate many frames (writeFrameNoFlush) and push them with a single flush() per batch.
        // The HMAC handshake already completed on the raw socket streams before this link was built,
        // so the buffer cannot swallow any handshake frame.
        this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), OUTPUT_BUFFER_BYTES));
        this.remoteNodeId = remoteNodeId;
        this.localNodeId = localNodeId;
        this.localEpoch = localEpoch;
        this.maxPacketSize = maxPacketSize;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.ackIntervalMs = ackIntervalMs;
        this.strictOrdering = strictOrdering;
        this.batchMaxFrames = batchMaxFrames > 0 ? batchMaxFrames : 1;
        this.batchMaxDelayMicros = Math.max(0L, batchMaxDelayMicros);
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

    /** Sets the SWIM membership sink that incoming SWIM packets on this link are routed to (Phase E). */
    public void setSwimSink(SwimMessageSink swimSink) {
        this.swimSink = swimSink;
    }

    public String getRemoteNodeId() {
        return remoteNodeId;
    }

    public boolean isRunning() {
        return running.get();
    }

    /** Enqueues an arbitrary control envelope (e.g. an interest delta/sync, SWIM packet) to send. */
    public boolean enqueueControl(ClusterEnvelope envelope) {
        if (!running.get()) {
            return false;
        }
        return controlQueue.offer(new OutboundItem.ControlFrame(envelope));
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
                    ClusterEvents.heartbeatTimeout(remoteNodeId);
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
                ClusterEnvelope envelope;
                try {
                    envelope = WireCodec.decodeCluster(frame, maxPacketSize);
                } catch (IllegalArgumentException e) {
                    log.warn("Malformed cluster envelope from peer {}: {}", remoteNodeId, e.getMessage());
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

    /** Dispatches a decoded envelope on its {@code oneof} case (replaces the JSON header.name switch). */
    private void dispatch(ClusterEnvelope env) {
        switch (env.getMsgCase()) {
            case CLUSTER_PUBLISH -> handleClusterPublish(env.getClusterPublish());
            case HEARTBEAT -> lastHeartbeatNanos = clock.nanoTime();
            case CLUSTER_ACK -> handleAck(env.getClusterAck());
            case CLUSTER_RESUME -> handleResume(env.getClusterResume());
            case CLUSTER_GAP -> handleGap(env.getClusterGap());
            case CLUSTER_INTEREST -> handleInterest(env.getClusterInterest());
            case CLUSTER_INTEREST_SYNC -> handleInterestSync(env.getClusterInterestSync());
            case SWIM_PING -> handleSwimPing(env.getSwimPing());
            case SWIM_ACK -> handleSwimAck(env.getSwimAck());
            case SWIM_PING_REQ -> handleSwimPingReq(env.getSwimPingReq());
            case SWIM_PING_REQ_ACK -> handleSwimPingReqAck(env.getSwimPingReqAck());
            case SWIM_JOIN -> handleSwimJoin(env.getSwimJoin());
            case SWIM_JOIN_ACK -> handleSwimJoinAck(env.getSwimJoinAck());
            case SWIM_LEAVE -> handleSwimLeave(env.getSwimLeave());
            // Auth envelopes only occur during the handshake (handled by ClusterHandshake); on a live
            // link they are unexpected, as is MSG_NOT_SET — ignore both rather than drop the link.
            default -> log.debug("Ignoring cluster envelope case {} from peer {}",
                    env.getMsgCase(), remoteNodeId);
        }
    }

    private void handleClusterPublish(ClusterPublish packet) {
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
    private void deliverIfNew(String originKey, ClusterPublish packet) {
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
    private void handlePublishStrict(String originKey, ClusterPublish packet, boolean newOnWire) {
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
            ClusterPublish head = strictPending.get(next);
            if (head == null) {
                // The contiguous frontier sits at `next`; the entry that fills `next` may itself be
                // pending under its own linkSeq. Deliver any pending entry whose linkSeq <= frontier.
                java.util.Map.Entry<Long, ClusterPublish> first = strictPending.firstEntry();
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

    private void deliver(ClusterPublish packet) {
        metrics.incrementMessagesReceived();
        // Fan out to local subscribers as a client-plane PublishOutbound envelope (the same wire
        // message a locally-published message produces).
        ClientEnvelope outbound = ClientEnvelopes.publishOutbound(
                packet.getTopic(), packet.getSource(), packet.getData());
        topicSubscriptionHandler.deliverPacketToSubscribers(packet.getTopic(), outbound);
    }

    /** Cumulative ACK from the remote: evict everything up to the acked {@code linkSeq} from our ring. */
    private void handleAck(ClusterAck ack) {
        metrics.incrementAcksReceived();
        // The reliability identity for our outbound ring is THIS link (localNodeId -> remoteNodeId).
        // The peer echoes our local node id; any matching ack advances the ring head over linkSeq.
        for (ClusterAck.AckEntry a : ack.getAcksList()) {
            if (a.getOriginNodeId().equals(localNodeId)) {
                replayBuffer.ackUpTo(a.getAckedSeq());
            }
        }
    }

    /** Remote (the receiver) tells us where to resume; backfill from our replay buffer over linkSeq. */
    private void handleResume(ClusterResume resume) {
        boolean handled = false;
        for (ClusterResume.ResumeEntry r : resume.getResumeList()) {
            // We only sourced frames over this link with reliability identity == localNodeId.
            if (r.getOriginNodeId().equals(localNodeId)) {
                backfill(r.getLastContiguousSeq());
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
     * {@link ClusterGap} when the requested range has been evicted.
     */
    private void backfill(long lastContiguousLinkSeq) {
        ReplayBuffer.ReplayResult result = replayBuffer.replayFrom(lastContiguousLinkSeq);
        if (result.gap()) {
            ClusterEvents.gap(remoteNodeId, result.firstAvailableSeq());
            metrics.incrementClusterGapEvents();
            controlQueue.offer(new OutboundItem.ControlFrame(ClusterEnvelopes.gap(
                    localNodeId, localNodeId, localEpoch, result.firstAvailableSeq())));
            return;
        }
        java.util.List<ReplayBuffer.Entry> entries = result.entries();
        if (entries.isEmpty()) {
            return;
        }
        for (ReplayBuffer.Entry e : entries) {
            // Re-send verbatim; the receiver's link tracker drops anything it already accepted.
            controlQueue.offer(new OutboundItem.PreSerializedFrame(e.payload()));
        }
        metrics.addClusterBackfillMessages(entries.size());
        ClusterEvents.backfillCompleted(remoteNodeId, entries.size(), lastContiguousLinkSeq);
    }

    /** Remote signalled that frames we expected are unrecoverable; resync the link frontier forward. */
    private void handleGap(ClusterGap gap) {
        long newContiguous = gap.getFirstAvailableSeq() - 1;
        ClusterEvents.dataLoss(remoteNodeId, newContiguous);
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

    private void handleInterest(ClusterInterest interest) {
        metrics.incrementInterestUpdatesReceived();
        receiver.onInterestDelta(interest.getNodeId(), interest.getInterestVersion(),
                interest.getOp(), interest.getTopic());
    }

    private void handleInterestSync(ClusterInterestSync sync) {
        metrics.incrementInterestUpdatesReceived();
        receiver.onInterestSync(sync.getNodeId(), sync.getInterestVersion(),
                new java.util.LinkedHashSet<>(sync.getTopicsList()));
    }

    // -------------------------------------------------------------------------
    // SWIM membership handling (Phase E, receiving side)
    // -------------------------------------------------------------------------

    private void handleSwimPing(SwimPing p) {
        SwimMessageSink sink = swimSink;
        if (sink == null) {
            return;
        }
        sink.onSwimPing(remoteNodeId, p.getSeqNo(), ClusterEnvelopes.fromProtoList(p.getPiggybackList()));
    }

    private void handleSwimAck(SwimAck p) {
        SwimMessageSink sink = swimSink;
        if (sink == null) {
            return;
        }
        sink.onSwimAck(remoteNodeId, p.getSeqNo(), ClusterEnvelopes.fromProtoList(p.getPiggybackList()));
    }

    private void handleSwimPingReq(SwimPingReq p) {
        SwimMessageSink sink = swimSink;
        if (sink == null) {
            return;
        }
        sink.onSwimPingReq(remoteNodeId, p.getTargetNodeId(), p.getSeqNo(),
                ClusterEnvelopes.fromProtoList(p.getPiggybackList()));
    }

    private void handleSwimPingReqAck(SwimPingReqAck p) {
        SwimMessageSink sink = swimSink;
        if (sink == null) {
            return;
        }
        sink.onSwimPingReqAck(remoteNodeId, p.getTargetNodeId(), p.getSeqNo(),
                ClusterEnvelopes.fromProtoList(p.getPiggybackList()));
    }

    private void handleSwimJoin(SwimJoin p) {
        SwimMessageSink sink = swimSink;
        if (sink == null) {
            return;
        }
        sink.onSwimJoin(remoteNodeId, p.getHost(), p.getClusterPort(), p.getIncarnation());
    }

    private void handleSwimJoinAck(de.kyle.avenue.proto.SwimJoinAck p) {
        SwimMessageSink sink = swimSink;
        if (sink == null) {
            return;
        }
        sink.onSwimJoinAck(remoteNodeId, ClusterEnvelopes.fromProtoList(p.getMembersList()));
    }

    private void handleSwimLeave(SwimLeave p) {
        SwimMessageSink sink = swimSink;
        if (sink == null) {
            return;
        }
        sink.onSwimLeave(remoteNodeId, p.getIncarnation());
    }

    // -------------------------------------------------------------------------
    // Writer loop
    // -------------------------------------------------------------------------

    private void writerLoop() {
        try {
            while (running.get()) {
                int framesWritten = 0;

                // Control / backfill frames first (low rate, includes ordering-critical replays).
                // Drain everything currently queued — batchMaxFrames caps how many we coalesce before
                // forcing a flush, so a huge backfill burst still flushes in bounded chunks.
                OutboundItem control;
                while (framesWritten < batchMaxFrames && (control = controlQueue.poll()) != null) {
                    writeFrameNoFlush(control);
                    framesWritten++;
                }
                boolean didWork = framesWritten > 0;

                // Then live publishes: append to the replay buffer (may block under backpressure —
                // this is the ONLY place blocking is allowed) and write if buffered. The entry's
                // seq() is the per-link linkSeq assigned by the sender for THIS target.
                //
                // Idle wait budget: keep the FIRST poll short (CONTROL_IDLE_POLL_MS) so a control
                // frame enqueued by the reader thread — most importantly a time-sensitive SWIM ack —
                // is flushed promptly rather than waiting out a long ingest poll. This bounds SWIM
                // probe RTT well under the probe timeout without spinning.
                ReplayBuffer.Entry entry =
                        ingestQueue.poll(didWork ? 0 : CONTROL_IDLE_POLL_MS, TimeUnit.MILLISECONDS);
                if (entry != null) {
                    // First publish of the batch: this is the ONLY append allowed to BLOCK (ring full
                    // under backpressure). Control frames were just drained above, so if we block here
                    // they have already been written and will be flushed when the block resolves —
                    // crucially the writer does NOT stay blocked across multiple appends, so heartbeats
                    // and ACKs enqueued during a backpressure burst still get a turn each loop.
                    if (framesWritten > 0 && replayBuffer.isFull()) {
                        // Flush the control frames we hold before blocking, so the peer sees our
                        // heartbeat/ACK (and any backfill) instead of it sitting in the buffer.
                        flushOut();
                        framesWritten = 0;
                    }
                    framesWritten += writeAppend(entry);
                    // Opportunistic coalescing: pull more publishes ONLY while the ring has headroom
                    // (non-blocking appends), so we never monopolize the writer across a blocking
                    // append. At low load this adds a few frames per flush; under backpressure it
                    // degrades to one publish per loop, exactly the legacy ACK-tight cadence.
                    framesWritten = drainPublishBatch(framesWritten);
                }

                // Exactly one flush per batch: at low load the batch is a single frame (identical to
                // the legacy per-frame flush, no added latency); under load one flush amortizes many
                // frames. An empty iteration (idle poll timed out) leaves the buffer empty, so a lone
                // heartbeat/ack enqueued next iteration still goes out promptly.
                if (framesWritten > 0) {
                    flushOut();
                }
            }
            // Drain any remaining control frames after stop is signalled, then flush so nothing is
            // left buffered on shutdown.
            int tail = 0;
            OutboundItem remaining;
            while ((remaining = controlQueue.poll()) != null) {
                writeFrameNoFlush(remaining);
                tail++;
            }
            if (tail > 0) {
                flushOut();
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

    /**
     * Opportunistically coalesces additional publishes into the current batch, but ONLY while the
     * replay ring has headroom so every append here is non-blocking. The moment the ring is full (the
     * next append would block) it stops, so the writer returns to the top of the loop to drain control
     * frames (heartbeats / ACKs) and perform the single blocking append there. This is what keeps a
     * sustained backpressure burst from starving heartbeats — under backpressure the batch is just one
     * publish per loop (the legacy cadence); with ring headroom it sweeps up to {@link #batchMaxFrames}
     * already-queued frames into one flush. FIFO order and the replay-buffer/linkSeq bookkeeping are
     * preserved exactly. With a positive {@link #batchMaxDelayMicros} linger it parks briefly for a few
     * more frames; the default {@code 0} sweeps only what is already present (no added latency).
     */
    private int drainPublishBatch(int framesWritten) throws InterruptedException, IOException {
        long lingerDeadlineNanos = batchMaxDelayMicros > 0
                ? System.nanoTime() + batchMaxDelayMicros * 1_000L : 0L;
        while (framesWritten < batchMaxFrames && !replayBuffer.isFull()) {
            ReplayBuffer.Entry next = ingestQueue.poll();
            if (next == null) {
                if (lingerDeadlineNanos == 0L) {
                    break; // opportunistic: nothing else immediately available
                }
                long remaining = lingerDeadlineNanos - System.nanoTime();
                if (remaining <= 0) {
                    break;
                }
                java.util.concurrent.locks.LockSupport.parkNanos(remaining);
                next = ingestQueue.poll();
                if (next == null) {
                    break;
                }
            }
            framesWritten += writeAppend(next);
        }
        return framesWritten;
    }

    /**
     * Appends a publish entry to the replay buffer and writes its frame (no flush), returning the
     * number of frames written (always 1). {@link ReplayBuffer#append} may BLOCK when the ring is full
     * — callers must therefore only invoke this when they have already flushed any pending control
     * frames (so a heartbeat/ACK is not held behind the block) and are prepared for the writer to
     * park. On a backpressure drop it writes a gap control frame so the receiver resyncs its contiguous
     * link frontier past the lost linkSeq — identical bookkeeping to the pre-batching path.
     */
    private int writeAppend(ReplayBuffer.Entry entry) throws IOException {
        if (replayBuffer.append(entry.seq(), entry.payload())) {
            writeFrameNoFlush(new OutboundItem.PreSerializedFrame(entry.payload()));
        } else {
            writeFrameNoFlush(new OutboundItem.ControlFrame(ClusterEnvelopes.gap(
                    localNodeId, localNodeId, localEpoch, entry.seq() + 1)));
        }
        return 1;
    }

    /** Writes a single framed item into the buffered stream WITHOUT flushing (write-batching). */
    private void writeFrameNoFlush(OutboundItem item) throws IOException {
        byte[] payload = switch (item) {
            case OutboundItem.PreSerializedFrame frame -> frame.payload();
            case OutboundItem.ControlFrame control -> serializeEnvelope(control.envelope());
        };
        synchronized (out) {
            PacketFraming.writeFrameNoFlush(out, payload);
        }
    }

    /** Flushes the buffered output stream once at a batch boundary. */
    private void flushOut() throws IOException {
        synchronized (out) {
            PacketFraming.flush(out);
        }
    }

    /** Serializes a {@link ClusterEnvelope} into its bare protobuf payload bytes (no length prefix). */
    static byte[] serializeEnvelope(ClusterEnvelope envelope) {
        return WireCodec.encodeCluster(envelope);
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
                if (!controlQueue.offer(new OutboundItem.ControlFrame(ClusterEnvelopes.heartbeat(localNodeId)))) {
                    ClusterEvents.queueFull(remoteNodeId, "heartbeat");
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
        if (controlQueue.offer(new OutboundItem.ControlFrame(
                ClusterEnvelopes.ack(localNodeId, remoteNodeId, 0L, contiguous)))) {
            metrics.incrementAcksSent();
        }
    }

    private void sendResumeRequest() {
        // Our resume point for the link the remote sends to us: last contiguous linkSeq received.
        long contiguous = receiver.linkResumePoint(remoteNodeId);
        controlQueue.offer(new OutboundItem.ControlFrame(
                ClusterEnvelopes.resume(localNodeId, remoteNodeId, 0L, contiguous)));
    }

    private void sendInitialInterestSync() {
        ClusterEnvelope sync = interestSyncSupplier.get();
        if (sync != null && controlQueue.offer(new OutboundItem.ControlFrame(sync))) {
            metrics.incrementInterestUpdatesSent();
        }
    }
}
