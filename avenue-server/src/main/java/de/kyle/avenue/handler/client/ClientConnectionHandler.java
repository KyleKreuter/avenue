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
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Handles a single client connection.
 * <p>
 * Outbound writes are decoupled from delivery via a bounded per-client queue and a dedicated
 * writer running on a virtual thread. Producers ({@link #enqueue(ClientEnvelope)}) never block
 * on the socket, so one slow subscriber can no longer cause head-of-line blocking for the
 * fan-out to all other subscribers. Only the writer thread touches the {@link DataOutputStream},
 * which is why no write lock is needed any more.
 * <p>
 * <h2>Outbound path: swap buffers instead of a queue of frame objects</h2>
 * Outbound bytes do not travel as objects. Producers append the <em>already framed</em> bytes
 * (4-byte length prefix + payload) straight into a pre-allocated {@code byte[]} fill buffer; the
 * writer swaps that buffer for its own and flushes the whole batch with a single
 * {@code write}/{@code flush}. Enqueueing is therefore a {@code memcpy} plus two counter updates —
 * no {@code LinkedBlockingQueue$Node}, no per-frame wrapper object, no {@code BufferedOutputStream}
 * copy in between.
 * <p>
 * This replaced a bounded {@code LinkedBlockingQueue<OutboundFrame>}, whose node and
 * {@code PreSerialized} wrapper together accounted for ~26 % of server allocation at fan-out 16
 * (JFR). The publish fan-out still serializes each envelope exactly once and shares the same
 * immutable {@code byte[]} across all subscribers (encode-once, O(1) instead of O(N)); each
 * subscriber connection merely copies those bytes into its own buffer.
 * <p>
 * Frames larger than one buffer are never rejected: they go to an {@link #overflow} queue that is
 * strictly FIFO-coupled with the fill buffer, so a payload up to {@code server.packet.max-size}
 * stays deliverable whatever {@code server.outbound.ring-bytes} is set to. Low-rate
 * request/response answers ({@link #enqueue(ClientEnvelope)}) are serialized via
 * {@link WireCodec#encodeClient} at enqueue time and travel the same path, preserving FIFO order
 * with the publish stream. Inbound frames are read raw and handed to the
 * {@link InboundPacketHandler}, which decodes and dispatches them.
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
public class ClientConnectionHandler implements ClientConnection, Runnable {
    private static final Logger log = LoggerFactory.getLogger(ClientConnectionHandler.class);

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
    private final InboundPacketHandler inboundPacketHandler;
    private final TopicSubscriptionHandler topicSubscriptionHandler;
    private final AvenueConfig avenueConfig;
    private final AvenueMetrics metrics;
    private final Runnable onDisconnect;

    private final long offerTimeoutMillis;
    private final BackpressurePolicy backpressurePolicy;
    private final long idleTimeoutMillis;
    private Thread writerThread;

    private volatile boolean running;

    // ------------------------------------------------------------------
    // Outbound swap buffers
    // ------------------------------------------------------------------

    /** Guards every field below; held only for memcpy + counter updates, never across a socket write. */
    private final ReentrantLock outboundLock = new ReentrantLock();
    /** Signalled when the writer has drained, i.e. buffer space became available for producers. */
    private final Condition notFull = outboundLock.newCondition();
    /** Signalled when a producer appended work for the writer. */
    private final Condition notEmpty = outboundLock.newCondition();

    /** Producers append framed bytes here; swapped with {@link #drainBuffer} by the writer. */
    private byte[] fillBuffer;
    /** The writer's buffer. Only the writer touches its contents, and only outside the lock. */
    private byte[] drainBuffer;
    /** Bytes currently used in {@link #fillBuffer}. */
    private int fillLength;
    /** Frames currently in {@link #fillBuffer}; the portion of {@link #pendingFrames} it accounts for. */
    private int fillFrames;
    /** Capacity of a single buffer, from {@code server.outbound.ring-bytes}. */
    private final int bufferCapacity;

    /**
     * Payloads too large to fit in a buffer at all. Strictly FIFO-coupled with the fill buffer: once
     * this is non-empty, ordinary frames must wait for it to drain rather than overtake it.
     */
    private final Deque<byte[]> overflow = new ArrayDeque<>();

    /**
     * Frames buffered but not yet handed to the socket, across fill buffer and overflow. Preserves
     * the {@code server.outbound.queue.capacity} semantics (a bound in <em>frames</em>) that the
     * bounded queue provided, and feeds the queue-depth metric. Written under the lock, read without
     * it by the writer's spin phase, hence volatile.
     */
    private volatile int pendingFrames;

    /** Frame-count bound, mirroring the previous bounded queue's capacity. */
    private final int frameCapacity;

    /**
     * {@link Thread#onSpinWait()} iterations the writer burns looking for more work before it parks.
     * Defaults to {@code 0} (off): spinning shifts CPU out of the park/unpark machinery but raises
     * total CPU and costs throughput at low fan-out — see {@code AvenueConfig#serverWriterSpins} for
     * the measurements.
     */
    private final int writerSpins;


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
        // No BufferedOutputStream: the swap buffers already coalesce a whole batch, so wrapping the
        // socket stream would only add a second copy of every byte on the way out.
        this.outputStream = client.getOutputStream();
        this.running = true;
        this.inboundPacketHandler = inboundPacketHandler;
        this.avenueConfig = avenueConfig;
        this.topicSubscriptionHandler = topicSubscriptionHandler;
        this.metrics = metrics;
        this.onDisconnect = onDisconnect;
        this.offerTimeoutMillis = avenueConfig.getOutboundQueueOfferTimeoutMillis();
        this.backpressurePolicy = avenueConfig.getBackpressurePolicy();
        this.idleTimeoutMillis = avenueConfig.getClientIdleTimeoutMillis();
        this.frameCapacity = Math.max(1, avenueConfig.getOutboundQueueCapacity());
        this.writerSpins = Math.max(0, avenueConfig.getServerWriterSpins());
        this.bufferCapacity = Math.max(1024, avenueConfig.getServerOutboundRingBytes());
        this.fillBuffer = new byte[bufferCapacity];
        this.drainBuffer = new byte[bufferCapacity];
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
        try {
            while (true) {
                // Spin before parking: under sustained load the next frame is usually microseconds
                // away, and parking would cost a virtual-thread unmount plus an unpark from the
                // producer. A short spin picks that frame up without ever suspending.
                for (int i = 0; i < writerSpins && pendingFrames == 0 && running; i++) {
                    Thread.onSpinWait();
                }

                byte[] batch;
                int batchLength;
                byte[] oversized = null;
                outboundLock.lock();
                try {
                    while (running && pendingFrames == 0) {
                        // Bounded wait so a closing connection is noticed even without a signal.
                        if (!notEmpty.await(200, TimeUnit.MILLISECONDS)) {
                            break;
                        }
                    }
                    if (!running && pendingFrames == 0) {
                        return;
                    }
                    if (pendingFrames == 0) {
                        continue; // spurious/timed-out wake-up
                    }
                    if (fillLength > 0) {
                        // Swap: the writer takes the filled buffer and hands its (now free) one back,
                        // so producers can keep appending while this batch goes to the socket. The
                        // frames leave the accounting right here, mirroring the old drainTo() which
                        // also freed queue slots before the bytes reached the socket.
                        byte[] swap = drainBuffer;
                        drainBuffer = fillBuffer;
                        fillBuffer = swap;
                        batch = drainBuffer;
                        batchLength = fillLength;
                        fillLength = 0;
                        pendingFrames -= fillFrames;
                        fillFrames = 0;
                    } else {
                        // Nothing in the fill buffer: the pending work is an oversized frame.
                        batch = null;
                        batchLength = 0;
                        oversized = overflow.pollFirst();
                        if (oversized != null) {
                            pendingFrames--;
                        }
                    }
                    // Space freed: let producers blocked on the offer timeout continue.
                    notFull.signalAll();
                } finally {
                    outboundLock.unlock();
                }

                // Socket write happens OUTSIDE the lock, so a slow consumer never blocks producers.
                if (oversized != null) {
                    writeFramed(oversized);
                } else {
                    outputStream.write(batch, 0, batchLength);
                }
                outputStream.flush();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            log.warn("Outbound write failed, closing connection", e);
        } finally {
            shutdown();
        }
    }

    /** Writes one oversized payload (too large for a swap buffer) with its length prefix. */
    private void writeFramed(byte[] payload) throws IOException {
        byte[] prefix = new byte[4];
        prefix[0] = (byte) (payload.length >>> 24);
        prefix[1] = (byte) (payload.length >>> 16);
        prefix[2] = (byte) (payload.length >>> 8);
        prefix[3] = (byte) payload.length;
        outputStream.write(prefix);
        outputStream.write(payload);
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
    @Override
    public void enqueue(ClientEnvelope envelope) {
        // Low-rate path (auth response / subscribe-ack): serialize here so the bytes can go into the
        // same buffer as the publish stream, which is what preserves FIFO order between them. Per-frame
        // serialization cost is irrelevant at this rate.
        enqueueFrame(WireCodec.encodeClient(envelope, avenueConfig.getPacketSize()));
    }

    /**
     * Enqueues an already-serialized client frame for asynchronous delivery (encode-once fan-out).
     * The supplied bytes are the bare protobuf payload (no length prefix) of a {@link ClientEnvelope}
     * and are written verbatim by the writer. Used by
     * {@link TopicSubscriptionHandler#deliverPacketToSubscribers}, which serializes the
     * {@code PublishOutbound} envelope exactly once and hands the same immutable {@code byte[]} to
     * every subscriber. The bytes must never be mutated after being passed in, as they are shared.
     */
    @Override
    public void enqueuePreSerialized(byte[] payload) {
        enqueueFrame(payload);
    }

    /**
     * Copies one bare payload, framed, into the outbound buffer and applies the configured
     * backpressure policy. Never blocks the caller on the socket: the lock is only ever held for a
     * {@code memcpy} and a few counter updates, never across a socket write.
     * <p>
     * Capacity is bounded exactly as the previous bounded queue was — by frame count
     * ({@code server.outbound.queue.capacity}) — plus the byte capacity of one buffer. A producer
     * that finds no room waits up to the offer timeout for the writer to drain, then falls to the
     * {@link BackpressurePolicy}, matching the old {@code offer(frame, timeout)} semantics including
     * its metric updates.
     */
    private void enqueueFrame(byte[] payload) {
        if (!this.running) {
            return;
        }
        int needed = 4 + payload.length;
        boolean accepted = false;
        boolean interrupted = false;
        int depth = 0;
        outboundLock.lock();
        try {
            long remainingNanos = TimeUnit.MILLISECONDS.toNanos(offerTimeoutMillis);
            while (running && !hasRoomFor(needed)) {
                if (remainingNanos <= 0) {
                    break;
                }
                remainingNanos = notFull.awaitNanos(remainingNanos);
            }
            if (!running) {
                return;
            }
            if (hasRoomFor(needed)) {
                append(payload, needed);
                pendingFrames++;
                depth = pendingFrames;
                accepted = true;
                // Signal unconditionally, exactly like the LinkedBlockingQueue this replaced.
                // An earlier version only signalled when the writer was observably parked
                // ("it will notice the work by itself otherwise"). That lost wake-ups: the
                // AtLeastOnceTest cluster path went from ~13 s to 25-36 s and started failing its
                // deadline. A signal with no waiter is cheap — the lock is already held — and
                // guessing when the writer needs one is not worth a stalled connection.
                notEmpty.signal();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            interrupted = true;
        } finally {
            outboundLock.unlock();
        }

        // shutdown() is synchronized AND takes outboundLock, so it must never be called while this
        // thread still holds outboundLock: another thread already inside shutdown() would be waiting
        // for that very lock while holding the monitor this thread needs — a lock-order inversion
        // that deadlocks the connection (symptom: handshakes hang with no auth response).
        if (interrupted) {
            shutdown();
            return;
        }

        if (accepted) {
            metrics.incrementMessagesDelivered();
            metrics.recordOutboundQueueDepth(depth);
            return;
        }
        // Still no room after the offer timeout: apply the configured backpressure policy.
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
    }

    /**
     * Whether a framed message of {@code needed} bytes can be accepted right now. Caller holds
     * {@link #outboundLock}.
     * <p>
     * Oversized frames (larger than a whole buffer) are always admissible up to the frame bound —
     * they take the {@link #overflow} path, which is what keeps payloads up to
     * {@code server.packet.max-size} deliverable no matter how small the buffers are. Ordinary
     * frames must additionally wait out any oversized backlog, so ordering is never violated.
     */
    private boolean hasRoomFor(int needed) {
        if (pendingFrames >= frameCapacity) {
            return false;
        }
        if (needed > bufferCapacity) {
            return true;
        }
        if (!overflow.isEmpty()) {
            return false;
        }
        return fillLength + needed <= bufferCapacity;
    }

    /** Appends a framed payload to the fill buffer, or to the overflow queue if it cannot fit. */
    private void append(byte[] payload, int needed) {
        if (needed > bufferCapacity) {
            overflow.addLast(payload);
            return;
        }
        fillBuffer[fillLength] = (byte) (payload.length >>> 24);
        fillBuffer[fillLength + 1] = (byte) (payload.length >>> 16);
        fillBuffer[fillLength + 2] = (byte) (payload.length >>> 8);
        fillBuffer[fillLength + 3] = (byte) payload.length;
        System.arraycopy(payload, 0, fillBuffer, fillLength + 4, payload.length);
        fillLength += needed;
        fillFrames++;
    }

    /**
     * Backwards-compatible direct-send entry point used by handlers that answer a request
     * inline (e.g. the auth-token response). Delegates to the asynchronous queue.
     */
    @Override
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
        // Release everyone blocked on the outbound conditions so they observe running == false:
        // the writer waiting for work, and any producer waiting out its offer timeout.
        outboundLock.lock();
        try {
            notEmpty.signalAll();
            notFull.signalAll();
        } finally {
            outboundLock.unlock();
        }
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
