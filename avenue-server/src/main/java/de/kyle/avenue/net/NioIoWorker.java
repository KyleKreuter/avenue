package de.kyle.avenue.net;

import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.handler.client.ClientConnection;
import de.kyle.avenue.handler.packet.InboundPacketHandler;
import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One NIO I/O worker: owns a {@link Selector}, a disjoint subset of client connections and a
 * single-threaded event loop. The {@link NioServerTransport} acceptor assigns accepted channels to
 * workers round-robin, so each worker's connection set is touched only by its own thread (read-side
 * state needs no locking).
 * <p>
 * Cross-thread coordination uses a lock-free {@link #taskQueue}: any thread (e.g. a publisher's
 * worker fanning a frame out to a subscriber owned by <em>this</em> worker) enqueues a task and calls
 * {@link Selector#wakeup()}. The loop drains the task queue before each {@code select()}, so
 * {@code interestOps} is only ever mutated on this thread — the documented safe pattern that avoids
 * the JDK's cross-thread {@code interestOps}/{@code select} race.
 * <p>
 * The loop:
 * <ol>
 *   <li>drains the task queue (new-connection registrations, {@code OP_WRITE} enables, closes);</li>
 *   <li>{@code select(timeout)} where the timeout drives the periodic idle-timeout sweep;</li>
 *   <li>handles {@code OP_READ} (frame extraction + dispatch) and {@code OP_WRITE} (drain outbound);</li>
 *   <li>sweeps idle connections once per second.</li>
 * </ol>
 */
final class NioIoWorker implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(NioIoWorker.class);

    /** Select timeout; also the granularity of the idle-timeout sweep. */
    private static final long SELECT_TIMEOUT_MS = 1_000L;

    private final int id;
    private final Selector selector;
    private final AvenueConfig config;
    private final InboundPacketHandler inboundPacketHandler;
    private final TopicSubscriptionHandler topicSubscriptionHandler;
    private final long idleTimeoutMillis;

    /**
     * Cross-thread control task queue for the rare register/close operations. The per-message
     * {@code OP_WRITE} enable does NOT go here — it uses the allocation-free {@link #pendingWrites}
     * queue below, so the hot fan-out path never allocates a {@link Runnable}.
     */
    private final Queue<Runnable> taskQueue = new ConcurrentLinkedQueue<>();

    /**
     * Connections that need {@code OP_WRITE} enabled, drained once per loop iteration. A connection
     * enqueues itself here at most once (guarded by its own {@code writeEnableQueued} flag), so a
     * burst of fan-out frames produces a single entry instead of one {@link Runnable} per frame —
     * this is the fix for the {@code drainTasks}/{@code ConcurrentLinkedQueue.offer} hot spot the
     * naive per-frame-task design produced.
     */
    private final Queue<NioClientConnection> pendingWrites = new ConcurrentLinkedQueue<>();

    /**
     * Whether a cross-thread producer still has to issue a {@link Selector#wakeup()} to get this
     * loop moving — i.e. whether the loop is about to block, or already blocked, in
     * {@code select()}.
     * <p>
     * <b>Why this exists.</b> {@code wakeup()} is a real syscall (a write to the selector's self-pipe
     * / kqueue). Calling it unconditionally in {@link #enableWrite} meant <em>one syscall per frame
     * per foreign-owned subscriber</em>: an N-way fan-out whose subscribers live on other workers
     * paid N syscalls per published message. JFR at fan-out 16 put {@code enableWrite} at 31,8 % of
     * server CPU — the single hottest method, ahead of all real work.
     * <p>
     * <b>The protocol.</b> The loop sets this to {@code true} immediately before deciding to block
     * and back to {@code false} once {@code select()} returns. A producer only wakes the selector if
     * it wins the {@code true -> false} CAS, so a burst of enqueues produces at most one syscall.
     * No wakeup can be lost: after arming the flag the loop re-checks its queues and downgrades to a
     * non-blocking {@code selectNow()} when work arrived in the meantime, and a {@code wakeup()}
     * issued just before {@code select()} is latched by the selector and returns immediately.
     */
    private final AtomicBoolean wakeupNeeded = new AtomicBoolean();

    private volatile Thread thread;
    private volatile boolean running = true;
    private long lastIdleSweepNanos = System.nanoTime();

    NioIoWorker(
            int id,
            AvenueConfig config,
            InboundPacketHandler inboundPacketHandler,
            TopicSubscriptionHandler topicSubscriptionHandler
    ) throws IOException {
        this.id = id;
        this.selector = Selector.open();
        this.config = config;
        this.inboundPacketHandler = inboundPacketHandler;
        this.topicSubscriptionHandler = topicSubscriptionHandler;
        this.idleTimeoutMillis = config.getClientIdleTimeoutMillis();
    }

    void start() {
        this.thread = new Thread(this, "avenue-nio-worker-" + id);
        this.thread.start();
    }

    /** Whether the calling thread is this worker's own event-loop thread. */
    boolean isWorkerThread() {
        return Thread.currentThread() == this.thread;
    }

    /**
     * Sets {@code OP_WRITE} on the connection's key directly. Only safe to call on the worker thread
     * (the same-thread fast path and the {@link #pendingWrites} drain). Touching {@code interestOps}
     * off-thread would race the selector, which is exactly why foreign threads go through the queue.
     */
    void enableWriteOnWorker(NioClientConnection connection) {
        if (connection.isClosed()) {
            return;
        }
        // Cached key, never channel().keyFor(selector) — see NioClientConnection#key().
        SelectionKey key = connection.key();
        if (key == null || !key.isValid()) {
            return;
        }
        int ops = key.interestOps();
        if ((ops & SelectionKey.OP_WRITE) == 0) {
            // Only WRITE the interest set when it actually changes. The getter is a plain field
            // read; the setter goes through the selector's update lock and is comparatively
            // expensive, and on a busy connection OP_WRITE is usually already armed.
            key.interestOps(ops | SelectionKey.OP_WRITE);
        }
    }

    // ------------------------------------------------------------------
    // Cross-thread entry points (callable from any thread)
    // ------------------------------------------------------------------

    /**
     * Registers a freshly accepted connection with this worker. Called by the acceptor thread; the
     * actual {@code register()} runs on the worker thread via the task queue so the selection key is
     * created on the owning thread.
     */
    void register(NioClientConnection connection) {
        execute(() -> {
            try {
                int ops = SelectionKey.OP_READ;
                // Defensive: if an outbound frame was already enqueued before this registration ran
                // (a producer racing the acceptor), arm OP_WRITE immediately so it is not stranded.
                if (connection.hasPendingWrites()) {
                    ops |= SelectionKey.OP_WRITE;
                }
                SelectionKey key = connection.channel().register(selector, ops, connection);
                connection.setKey(key);
            } catch (ClosedChannelException e) {
                log.warn("Failed to register NIO connection (channel already closed)", e);
                connection.close(topicSubscriptionHandler::unsubscribeFromAllTopics);
            }
        });
    }

    /**
     * Asks this worker to enable {@code OP_WRITE} for {@code connection}. Safe from any thread.
     * <ul>
     *   <li>Same-thread fast path: when the caller already <em>is</em> this worker (the common case —
     *       inline publish fan-out runs on the publisher's worker, and a subscriber on the same worker
     *       needs no hop), {@code interestOps} is set directly with no queue and no wakeup.</li>
     *   <li>Cross-thread path: the connection enqueues itself onto {@link #pendingWrites} at most once
     *       (coalesced via its own flag) and wakes the selector. No per-frame {@link Runnable} is
     *       allocated.</li>
     * </ul>
     */
    void enableWrite(NioClientConnection connection) {
        if (isWorkerThread()) {
            enableWriteOnWorker(connection);
            return;
        }
        if (connection.markWriteEnableQueued()) {
            pendingWrites.add(connection);
            wakeupSelectorIfBlocked();
        }
    }

    /** Asks this worker to close {@code connection}. Safe from any thread. */
    void closeConnection(NioClientConnection connection) {
        execute(() -> connection.close(topicSubscriptionHandler::unsubscribeFromAllTopics));
    }

    /** Enqueues a task and wakes the selector so it runs promptly. */
    private void execute(Runnable task) {
        taskQueue.add(task);
        wakeupSelectorIfBlocked();
    }

    /**
     * Issues {@link Selector#wakeup()} only if the loop is (about to be) blocked in {@code select()},
     * collapsing a burst of cross-thread enqueues into at most one wakeup syscall. See
     * {@link #wakeupNeeded} for why this matters and why no wakeup can be lost.
     */
    private void wakeupSelectorIfBlocked() {
        if (wakeupNeeded.compareAndSet(true, false)) {
            selector.wakeup();
        }
    }

    // ------------------------------------------------------------------
    // Event loop
    // ------------------------------------------------------------------

    @Override
    public void run() {
        try {
            while (running) {
                drainTasks();
                drainPendingWrites();
                if (!running) {
                    break;
                }
                // Arm the wakeup flag BEFORE re-checking the queues: a producer that enqueues from
                // here on either wins the CAS and wakes us, or lost the race to the re-check below.
                wakeupNeeded.set(true);
                try {
                    if (pendingWrites.isEmpty() && taskQueue.isEmpty()) {
                        selector.select(SELECT_TIMEOUT_MS);
                    } else {
                        // Work arrived while we were arming: poll without blocking so it is handled
                        // this iteration instead of waiting out the select timeout.
                        selector.selectNow();
                    }
                } finally {
                    wakeupNeeded.set(false);
                }
                if (!selector.isOpen()) {
                    break;
                }
                Set<SelectionKey> selected = selector.selectedKeys();
                Iterator<SelectionKey> it = selected.iterator();
                while (it.hasNext()) {
                    SelectionKey key = it.next();
                    it.remove();
                    if (!key.isValid()) {
                        continue;
                    }
                    NioClientConnection connection = (NioClientConnection) key.attachment();
                    try {
                        if (key.isReadable()) {
                            handleRead(connection);
                        }
                        if (key.isValid() && key.isWritable()) {
                            handleWrite(connection, key);
                        }
                    } catch (CancelledKeyException ignored) {
                        // Key was cancelled mid-handling (e.g. a concurrent close); nothing to do.
                    }
                }
                sweepIdleConnections();
            }
        } catch (IOException e) {
            if (running) {
                log.error("NIO worker {} loop error", id, e);
            }
        } finally {
            closeAll();
        }
    }

    private void drainTasks() {
        Runnable task;
        while ((task = taskQueue.poll()) != null) {
            try {
                task.run();
            } catch (Exception e) {
                log.warn("NIO worker {} task failed", id, e);
            }
        }
    }

    /**
     * Drains the coalesced write-enable queue, clearing each connection's flag and setting
     * {@code OP_WRITE}. Runs on the worker thread, so {@code interestOps} is set safely.
     */
    private void drainPendingWrites() {
        NioClientConnection connection;
        while ((connection = pendingWrites.poll()) != null) {
            connection.clearWriteEnableQueued();
            enableWriteOnWorker(connection);
        }
    }

    private void handleRead(NioClientConnection connection) {
        try {
            boolean open = connection.readAndDispatch(frameBytes -> dispatchFrame(connection, frameBytes));
            if (!open) {
                connection.close(topicSubscriptionHandler::unsubscribeFromAllTopics);
            }
        } catch (NioClientConnection.FrameLengthException e) {
            // Negative/oversized length: a desynced or hostile peer; cannot resync -> close.
            log.warn("Closing NIO connection {} due to invalid frame length: {}",
                    connection.remoteAddress(), e.getMessage());
            connection.close(topicSubscriptionHandler::unsubscribeFromAllTopics);
        } catch (IOException e) {
            connection.close(topicSubscriptionHandler::unsubscribeFromAllTopics);
        }
    }

    /**
     * Dispatches one fully-read frame to the shared {@link InboundPacketHandler}, applying the same
     * {@code drop-unknown} policy the blocking transport applies: an {@link IllegalArgumentException}
     * (malformed/unauthorized/unknown packet) closes the connection when {@code drop-unknown} is on.
     */
    private void dispatchFrame(NioClientConnection connection, byte[] frameBytes) throws IOException {
        try {
            inboundPacketHandler.handleInboundPacket(frameBytes, config.getPacketSize(), connection);
        } catch (IllegalArgumentException e) {
            if (config.isDropUnknownPackets()) {
                log.warn("Unknown or malformed packet from {}, dropping client",
                        connection.remoteAddress(), e);
                connection.close(topicSubscriptionHandler::unsubscribeFromAllTopics);
            } else {
                log.warn("Unknown or malformed packet from {} but 'drop-unknown' is off; keeping client",
                        connection.remoteAddress(), e);
            }
        }
    }

    private void handleWrite(NioClientConnection connection, SelectionKey key) {
        try {
            boolean drained = connection.writeOutbound();
            if (drained && key.isValid()) {
                // Outbound empty: drop OP_WRITE so we stop spinning on a writable socket.
                key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            }
        } catch (IOException e) {
            connection.close(topicSubscriptionHandler::unsubscribeFromAllTopics);
        }
    }

    /**
     * Closes connections that have not sent a byte within the idle-timeout window. Same semantics as
     * the blocking transport's {@code setSoTimeout}-driven reaping; {@code idle-timeout=0} disables
     * it. Runs at most once per {@link #SELECT_TIMEOUT_MS} so it is cheap.
     */
    private void sweepIdleConnections() {
        if (idleTimeoutMillis <= 0) {
            return;
        }
        long now = System.nanoTime();
        if (now - lastIdleSweepNanos < SELECT_TIMEOUT_MS * 1_000_000L) {
            return;
        }
        lastIdleSweepNanos = now;
        long idleNanos = idleTimeoutMillis * 1_000_000L;
        for (SelectionKey key : selector.keys()) {
            Object attachment = key.attachment();
            if (!(attachment instanceof NioClientConnection connection)) {
                continue;
            }
            if (now - connection.lastReadNanos() >= idleNanos) {
                log.info("Idle timeout ({} ms) reached for {}, closing dead NIO connection",
                        idleTimeoutMillis, connection.remoteAddress());
                connection.close(topicSubscriptionHandler::unsubscribeFromAllTopics);
            }
        }
    }

    // ------------------------------------------------------------------
    // Shutdown
    // ------------------------------------------------------------------

    void stop() {
        running = false;
        selector.wakeup();
    }

    /** Closes every connection still registered with this worker, then the selector. */
    private void closeAll() {
        try {
            for (SelectionKey key : selector.keys()) {
                Object attachment = key.attachment();
                if (attachment instanceof NioClientConnection connection) {
                    connection.close(topicSubscriptionHandler::unsubscribeFromAllTopics);
                }
            }
            selector.close();
        } catch (IOException e) {
            log.warn("Error closing NIO worker {} selector", id, e);
        }
    }
}
