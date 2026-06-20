package de.kyle.avenue.net;

import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.handler.client.BackpressurePolicy;
import de.kyle.avenue.handler.client.ClientConnection;
import de.kyle.avenue.metrics.AvenueMetrics;
import de.kyle.avenue.proto.ClientEnvelope;
import de.kyle.avenue.serialization.WireCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * NIO event-loop counterpart of
 * {@link de.kyle.avenue.handler.client.ClientConnectionHandler}: a single client connection served
 * by a hand-rolled selector loop instead of a dedicated reader/writer thread pair.
 * <p>
 * One {@link NioIoWorker} owns this connection (its {@link SocketChannel}, {@link SelectionKey} and
 * the per-connection read accumulation buffer); all read-side state is therefore touched only on
 * that worker's thread and needs no synchronization. The outbound side, by contrast, is written from
 * <em>any</em> thread: the publish fan-out runs on the publisher's I/O worker, which may be a
 * different worker than the one owning the subscriber. {@link #enqueuePreSerialized(byte[])} and
 * {@link #enqueue(ClientEnvelope)} therefore push a fully-framed frame onto a lock-guarded outbound
 * deque and ask the owning worker (via its cross-thread task queue + {@code selector.wakeup()}) to
 * enable {@code OP_WRITE} — never touching {@code interestOps} from a foreign thread.
 * <p>
 * Wire format parity with the blocking transport: {@link #enqueuePreSerialized(byte[])} receives the
 * <em>bare</em> protobuf payload (no length prefix), exactly as the blocking writer does, and frames
 * it here with the same 4-byte big-endian length prefix
 * ({@link de.kyle.avenue.serialization.PacketFraming}). The same immutable payload {@code byte[]} is
 * shared across all subscribers (encode-once fan-out); the framed {@link ByteBuffer} is built
 * per-connection and never shared.
 * <p>
 * Backpressure parity: the outbound deque is bounded by {@code server.outbound.queue.capacity}. When
 * a producer cannot enqueue because the deque is full the configured {@link BackpressurePolicy} is
 * applied — {@code DISCONNECT_SLOW_CONSUMER} closes the connection, {@code DROP_MESSAGE} discards the
 * frame — updating the very same metrics the blocking transport updates.
 */
public final class NioClientConnection implements ClientConnection {

    private static final Logger log = LoggerFactory.getLogger(NioClientConnection.class);

    private final SocketChannel channel;
    private final NioIoWorker worker;
    private final AvenueConfig config;
    private final AvenueMetrics metrics;
    private final Runnable onDisconnect;
    private final int maxPacketSize;
    private final int outboundCapacity;
    private final long offerTimeoutNanos;
    private final BackpressurePolicy backpressurePolicy;

    /** Set once the connection is registered with its worker's selector. */
    private volatile SelectionKey key;

    /**
     * Read accumulation buffer. Only the owning worker thread touches it, so it is unsynchronized.
     * Grows on demand for oversized frames (still bounded by {@code maxPacketSize}); compacted after
     * each frame extraction so partial frames survive across reads.
     */
    private ByteBuffer readBuffer = ByteBuffer.allocate(16 * 1024);

    /**
     * Outbound queue of already-framed buffers (4-byte length prefix + payload). Guarded by
     * {@link #outboundLock}. A {@link ByteBuffer} is partially drained on a short write, so the head
     * buffer keeps its position between {@code OP_WRITE} passes.
     */
    private final Deque<ByteBuffer> outbound = new ArrayDeque<>();
    private final Object outboundLock = new Object();

    /**
     * Nanotime when the outbound queue first rose above {@link #outboundCapacity} and stayed there,
     * or {@code 0} while it is below the soft cap. Drives the offer-timeout grace before the
     * backpressure policy is applied, mirroring the blocking transport's {@code offer(timeout)}: a
     * transient burst is tolerated (the frame is still enqueued, never lost), only a queue that stays
     * full beyond the offer timeout is treated as a genuine slow consumer. Guarded by
     * {@link #outboundLock}.
     */
    private long overflowSinceNanos;

    /** Last time (nanos) a byte was read from this connection, for the idle-timeout sweep. */
    private volatile long lastReadNanos = System.nanoTime();

    /**
     * Coalescing flag for the cross-thread {@code OP_WRITE} enable: a foreign-thread producer enqueues
     * this connection on its owning worker's pending-writes queue only when this flag flips from
     * {@code false} to {@code true}, so a burst of fan-out frames produces a single queue entry
     * instead of one per frame.
     */
    private final AtomicBoolean writeEnableQueued = new AtomicBoolean();

    /** Guards the single-shot close semantics (cancel key, unsubscribe, onDisconnect run once). */
    private volatile boolean closed;

    NioClientConnection(
            SocketChannel channel,
            NioIoWorker worker,
            AvenueConfig config,
            AvenueMetrics metrics,
            Runnable onDisconnect
    ) {
        this.channel = channel;
        this.worker = worker;
        this.config = config;
        this.metrics = metrics;
        this.onDisconnect = onDisconnect;
        this.maxPacketSize = config.getPacketSize();
        this.outboundCapacity = Math.max(1, config.getOutboundQueueCapacity());
        this.offerTimeoutNanos = Math.max(0, config.getOutboundQueueOfferTimeoutMillis()) * 1_000_000L;
        this.backpressurePolicy = config.getBackpressurePolicy();
    }

    // ------------------------------------------------------------------
    // Wiring (called by the owning worker on registration)
    // ------------------------------------------------------------------

    SocketChannel channel() {
        return channel;
    }

    void setKey(SelectionKey key) {
        this.key = key;
    }

    long lastReadNanos() {
        return lastReadNanos;
    }

    /**
     * Atomically marks that this connection has been enqueued for an {@code OP_WRITE} enable.
     *
     * @return {@code true} if the caller won the race and should enqueue it (flag flipped
     *         {@code false -> true}); {@code false} if it was already queued
     */
    boolean markWriteEnableQueued() {
        return writeEnableQueued.compareAndSet(false, true);
    }

    /** Clears the write-enable coalescing flag once the worker has processed the enqueue. */
    void clearWriteEnableQueued() {
        writeEnableQueued.set(false);
    }

    // ------------------------------------------------------------------
    // ClientConnection contract — outbound (callable from any thread)
    // ------------------------------------------------------------------

    @Override
    public void enqueue(ClientEnvelope envelope) {
        // Low-rate request/response path (auth response, subscribe-ack): serialize the bare payload
        // and frame it like a fan-out frame, preserving FIFO order with the publish path.
        byte[] payload = WireCodec.encodeClient(envelope, maxPacketSize);
        enqueueFramed(frame(payload));
    }

    @Override
    public void send(ClientEnvelope envelope) {
        enqueue(envelope);
    }

    @Override
    public void enqueuePreSerialized(byte[] payload) {
        // Same convention as the blocking transport: the bytes are the BARE payload, framed here.
        enqueueFramed(frame(payload));
    }

    /**
     * Frames a bare payload as {@code [4-byte big-endian length][payload]} into a single read-only
     * {@link ByteBuffer}. Matches {@link de.kyle.avenue.serialization.PacketFraming} exactly.
     */
    private static ByteBuffer frame(byte[] payload) {
        ByteBuffer buf = ByteBuffer.allocate(4 + payload.length);
        buf.putInt(payload.length);
        buf.put(payload);
        buf.flip();
        return buf;
    }

    /**
     * Enqueues an already-framed buffer applying the bounded-queue backpressure policy, then asks the
     * owning worker to enable {@code OP_WRITE}. Never blocks the caller on the socket. Mirrors
     * {@link de.kyle.avenue.handler.client.ClientConnectionHandler}'s metric updates.
     */
    private void enqueueFramed(ByteBuffer framed) {
        if (closed) {
            return;
        }
        int depth;
        boolean sustainedOverflow = false;
        boolean dropped = false;
        synchronized (outboundLock) {
            long now = System.nanoTime();
            if (outbound.size() < outboundCapacity) {
                // Under the soft cap: enqueue normally and clear any pending overflow timer.
                outbound.addLast(framed);
                overflowSinceNanos = 0;
            } else {
                // At/over the soft cap. Start the grace timer on the first overflow; once it has been
                // full continuously for longer than the offer timeout the consumer is genuinely slow.
                if (overflowSinceNanos == 0) {
                    overflowSinceNanos = now;
                }
                boolean graceExpired = now - overflowSinceNanos >= offerTimeoutNanos;
                if (graceExpired && backpressurePolicy == BackpressurePolicy.DROP_MESSAGE) {
                    // DROP_MESSAGE: discard this frame, keep the connection (matches blocking).
                    dropped = true;
                } else if (graceExpired) {
                    // DISCONNECT_SLOW_CONSUMER: tear the connection down (scheduled below).
                    sustainedOverflow = true;
                } else {
                    // Still within the grace window: tolerate the transient burst and enqueue the
                    // frame (the blocking transport's offer() also succeeds within the timeout, so no
                    // message is lost for a short spike).
                    outbound.addLast(framed);
                }
            }
            depth = outbound.size();
        }
        if (dropped) {
            metrics.incrementDroppedMessages();
            log.warn("Outbound queue for {} is full, dropping message (DROP_MESSAGE policy)",
                    remoteAddress());
            return;
        }
        if (sustainedOverflow) {
            metrics.incrementSlowConsumerDisconnects();
            log.warn("Outbound queue for {} stayed full beyond the offer timeout, disconnecting slow "
                    + "consumer (DISCONNECT_SLOW_CONSUMER policy)", remoteAddress());
            // Closing must run on the owning worker (touches the selection key); schedule it.
            worker.closeConnection(this);
            return;
        }
        metrics.incrementMessagesDelivered();
        metrics.recordOutboundQueueDepth(depth);
        // Cross-thread wakeup: register OP_WRITE interest on the OWNING worker, never touch
        // interestOps from this (possibly foreign) thread.
        worker.enableWrite(this);
    }

    // ------------------------------------------------------------------
    // Read side — only ever called on the owning worker thread
    // ------------------------------------------------------------------

    /**
     * Reads available bytes into the accumulation buffer and extracts every complete frame, handing
     * each to {@code frameConsumer}. Mirrors {@link de.kyle.avenue.serialization.PacketFraming}
     * length-prefix semantics: a negative or oversized length is fatal (the worker closes the
     * connection). Partial frames are retained across reads.
     *
     * @return {@code false} if the peer closed the connection (EOF), {@code true} otherwise
     * @throws IOException            on a read error
     * @throws FrameLengthException   if an announced frame length is negative or exceeds the maximum
     */
    boolean readAndDispatch(FrameConsumer frameConsumer) throws IOException, FrameLengthException {
        ensureReadCapacity(0);
        int read = channel.read(readBuffer);
        if (read == -1) {
            return false; // peer closed
        }
        if (read == 0) {
            return true;
        }
        lastReadNanos = System.nanoTime();
        readBuffer.flip();
        try {
            while (true) {
                if (readBuffer.remaining() < 4) {
                    break; // not even a length prefix yet
                }
                readBuffer.mark();
                int length = readBuffer.getInt();
                if (length < 0 || length > maxPacketSize) {
                    throw new FrameLengthException(length, maxPacketSize);
                }
                if (readBuffer.remaining() < length) {
                    // Full payload not here yet: rewind to the start of this frame and wait for more.
                    readBuffer.reset();
                    break;
                }
                byte[] frame = new byte[length];
                readBuffer.get(frame);
                frameConsumer.accept(frame);
                if (closed) {
                    break;
                }
            }
        } finally {
            // Keep the unconsumed remainder for the next read; grow if a large frame is mid-arrival.
            readBuffer.compact();
        }
        return true;
    }

    /**
     * Ensures the read buffer can hold at least one full max-sized frame plus its prefix, growing it
     * (preserving already-buffered bytes) when a partial oversized frame would otherwise not fit.
     */
    private void ensureReadCapacity(int additional) {
        if (readBuffer.hasRemaining() && additional == 0) {
            return;
        }
        if (!readBuffer.hasRemaining()) {
            int needed = Math.min(maxPacketSize + 4, readBuffer.capacity() * 2);
            if (needed <= readBuffer.capacity()) {
                needed = readBuffer.capacity() * 2;
            }
            ByteBuffer bigger = ByteBuffer.allocate(needed);
            readBuffer.flip();
            bigger.put(readBuffer);
            readBuffer = bigger;
        }
    }

    // ------------------------------------------------------------------
    // Write side — only ever called on the owning worker thread
    // ------------------------------------------------------------------

    /**
     * Drains as many queued frames into the socket as it will accept (write-batching: many small
     * frames coalesce into few {@code write} syscalls). On a short write the partially-written head
     * buffer is left at the front so the next {@code OP_WRITE} pass resumes it.
     *
     * @return {@code true} if the outbound queue is now empty (the worker may drop {@code OP_WRITE})
     * @throws IOException on a write error
     */
    boolean writeOutbound() throws IOException {
        while (true) {
            ByteBuffer head;
            synchronized (outboundLock) {
                head = outbound.peekFirst();
            }
            if (head == null) {
                return true; // drained
            }
            channel.write(head);
            if (head.hasRemaining()) {
                return false; // socket buffer full; keep OP_WRITE, resume later
            }
            synchronized (outboundLock) {
                // The head we just fully wrote is still the front element; remove it.
                if (!outbound.isEmpty() && outbound.peekFirst() == head) {
                    outbound.removeFirst();
                }
                // Draining back under the soft cap clears the overflow grace timer so a consumer that
                // recovers is not penalized for an earlier transient spike.
                if (outbound.size() < outboundCapacity) {
                    overflowSinceNanos = 0;
                }
            }
        }
    }

    boolean hasPendingWrites() {
        synchronized (outboundLock) {
            return !outbound.isEmpty();
        }
    }

    // ------------------------------------------------------------------
    // Close — single-shot
    // ------------------------------------------------------------------

    /**
     * Closes the connection exactly once: cancels the selection key, closes the channel,
     * unsubscribes from all topics and runs the disconnect callback (which decrements the
     * active-connection gauge). Must be invoked on the owning worker thread.
     */
    void close(java.util.function.Consumer<ClientConnection> unsubscribe) {
        if (closed) {
            return;
        }
        closed = true;
        log.info("Closing NIO connection to {}", remoteAddress());
        SelectionKey k = this.key;
        if (k != null) {
            k.cancel();
        }
        try {
            channel.close();
        } catch (IOException e) {
            log.warn("Error while closing NIO channel", e);
        } finally {
            unsubscribe.accept(this);
            onDisconnect.run();
        }
    }

    boolean isClosed() {
        return closed;
    }

    String remoteAddress() {
        try {
            return String.valueOf(channel.getRemoteAddress());
        } catch (Exception e) {
            return "unknown";
        }
    }

    /** Functional sink for a fully-read frame's bare payload bytes. */
    @FunctionalInterface
    interface FrameConsumer {
        void accept(byte[] frameBytes) throws IOException;
    }

    /** Signals a protocol-fatal frame length (negative or oversized) so the worker can close. */
    static final class FrameLengthException extends Exception {
        FrameLengthException(int length, int max) {
            super(String.format("Invalid frame length %d (max allowed %d)", length, max));
        }
    }
}
