package de.kyle.avenue.cluster;

import de.kyle.avenue.metrics.ClusterMetrics;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Combined send + replay buffer for one cluster <em>target node</em>, the heart of Phase C's
 * at-least-once delivery.
 * <p>
 * <b>One instance per target {@code nodeId}.</b> The buffer is held by {@link ClusterManager}
 * (keyed by target node id), <em>not</em> by {@link PeerLink}, so it survives a link teardown:
 * when a dropped link is rebuilt the new link reuses the same buffer and can backfill everything
 * the peer has not yet acknowledged. This is what makes partition+heal recovery possible.
 * <p>
 * Because a node only ever sends messages with {@code originNodeId == self} over an outbound link
 * (single counter source), the {@code seq} numbers appended here are already strictly monotonic.
 * The buffer therefore is a simple bounded FIFO ring of {@code (seq, payload)} entries:
 * <ul>
 *   <li>{@link #append(long, byte[])} — adds the next entry. Called <b>only on the writer thread</b>
 *       (which may block), so backpressure never reaches the local delivery / client-reader path.
 *       When the ring is full (peer acks too slowly) the configured {@link BackpressurePolicy}
 *       decides: {@code BLOCK} waits up to {@code offerTimeoutMs} for the head to be acked away
 *       (and on timeout degrades to a gap), {@code DROP_GAP} drops immediately and records a gap.
 *       A gap is non-fatal: it only means the receiver will later resync past the lost range.</li>
 *   <li>{@link #ackUpTo(long)} — called on the link reader thread when a cumulative ACK arrives;
 *       discards every entry with {@code seq <= ackedSeq} (advances the ring head) and wakes a
 *       writer blocked in {@code append}.</li>
 *   <li>{@link #replayFrom(long)} — called right after a (re)connect to fetch everything with
 *       {@code seq > lastContiguousSeq} for backfill, or to learn that the requested resume point
 *       has already been evicted (a {@link ReplayResult#gap()}).</li>
 * </ul>
 * Memory is hard-bounded by {@code capacity}; the buffer is never unbounded.
 * <p>
 * Thread-safety: a single {@link ReentrantLock} guards all mutable state. The writer appends and
 * sends, a reader thread acks, and a reconnecting connector thread replays — all serialize on the
 * lock; contention is low because each of those is a single thread per link.
 */
public final class ReplayBuffer {

    /** Backpressure policy applied by {@link #append} when the ring is full. */
    public enum BackpressurePolicy {
        /** Wait up to the offer timeout for space; on timeout degrade to a gap. */
        BLOCK,
        /** Drop immediately and record a gap. */
        DROP_GAP;

        /** Parses a config value case-insensitively, falling back to {@link #BLOCK}. */
        public static BackpressurePolicy fromConfig(String raw) {
            if (raw == null) {
                return BLOCK;
            }
            try {
                return BackpressurePolicy.valueOf(raw.trim().toUpperCase(java.util.Locale.ROOT));
            } catch (IllegalArgumentException e) {
                return BLOCK;
            }
        }
    }

    /** Immutable ring entry: an origin sequence number and its pre-serialized envelope bytes. */
    public record Entry(long seq, byte[] payload) {
    }

    /**
     * Result of {@link #replayFrom(long)}. Either an ordered list of entries to re-send (possibly
     * empty), or a gap signal carrying the first sequence still available in the buffer when the
     * requested resume point has already been evicted.
     *
     * @param entries          entries with {@code seq > requested}, in order (empty on a gap)
     * @param gap              {@code true} if the requested resume point is no longer buffered
     * @param firstAvailableSeq when {@code gap}, the lowest {@code seq} still present (or
     *                          {@code nextSeqIfEmpty} if the buffer is empty)
     */
    public record ReplayResult(List<Entry> entries, boolean gap, long firstAvailableSeq) {
        static ReplayResult ofEntries(List<Entry> entries) {
            return new ReplayResult(entries, false, 0L);
        }

        static ReplayResult ofGap(long firstAvailableSeq) {
            return new ReplayResult(List.of(), true, firstAvailableSeq);
        }
    }

    private final int capacity;
    private final BackpressurePolicy policy;
    private final long offerTimeoutMs;
    private final ClusterMetrics metrics;

    private final ReentrantLock lock = new ReentrantLock();
    /** Signalled when the head advances (an ack freed space), waking a blocked appender. */
    private final Condition spaceAvailable = lock.newCondition();

    /** FIFO of buffered entries ordered by ascending seq. Head = oldest un-acked entry. */
    private final Deque<Entry> ring = new ArrayDeque<>();

    /** Highest seq ever appended (delivered or dropped). Used to keep the resume math correct. */
    private long highestSeq = 0L;
    /** Last seq cumulatively acked by the peer; entries with seq &lt;= this are evicted. */
    private long ackedSeq = 0L;
    /** Last ring size reported to the aggregate depth gauge, so we can report deltas. */
    private int lastReportedDepth = 0;

    public ReplayBuffer(int capacity, BackpressurePolicy policy, long offerTimeoutMs, ClusterMetrics metrics) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.capacity = capacity;
        this.policy = policy != null ? policy : BackpressurePolicy.BLOCK;
        this.offerTimeoutMs = offerTimeoutMs;
        this.metrics = metrics;
    }

    /**
     * Appends the next {@code (seq, payload)} entry, applying backpressure when the ring is full.
     * <b>Must be called on the writer thread only</b> — it may block (BLOCK policy), and the whole
     * point of Phase C is that this blocking never touches the local delivery or client-reader path.
     * <p>
     * Returns {@code true} if the entry was buffered (caller should send it), {@code false} if it
     * was dropped because of backpressure (a gap was recorded; caller must NOT send it). The link is
     * never killed by backpressure.
     *
     * @param seq     the strictly increasing origin sequence number
     * @param payload the pre-serialized envelope bytes
     * @return {@code true} if buffered and ready to send; {@code false} if dropped (gap)
     */
    public boolean append(long seq, byte[] payload) {
        lock.lock();
        try {
            // Track the high-water mark regardless of whether the entry is ultimately buffered, so
            // a later resume request can tell "you are caught up" from "there is a gap".
            if (seq > highestSeq) {
                highestSeq = seq;
            }

            if (isFullLocked()) {
                if (policy == BackpressurePolicy.DROP_GAP) {
                    recordGapLocked();
                    return false;
                }
                // BLOCK: wait for an ack to free the head, up to the offer timeout.
                long deadlineNanos = System.nanoTime() + offerTimeoutMs * 1_000_000L;
                metrics.incrementClusterSlowPeerStalls();
                while (isFullLocked()) {
                    long remaining = deadlineNanos - System.nanoTime();
                    if (remaining <= 0) {
                        // Timed out waiting for space: degrade to a gap for this one entry rather
                        // than killing the link or blocking forever.
                        recordGapLocked();
                        return false;
                    }
                    try {
                        spaceAvailable.awaitNanos(remaining);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        recordGapLocked();
                        return false;
                    }
                }
            }

            ring.addLast(new Entry(seq, payload));
            reportDepthLocked();
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Discards every buffered entry with {@code seq <= ackedSeq} (cumulative ACK), advancing the
     * ring head, and wakes a writer that may be blocked in {@link #append} waiting for space.
     * Idempotent and monotonic: a stale or out-of-order ack lower than the current high-water mark
     * is ignored. Safe to call from the reader thread.
     *
     * @param newAckedSeq the highest contiguous seq the peer has confirmed delivered
     */
    public void ackUpTo(long newAckedSeq) {
        lock.lock();
        try {
            if (newAckedSeq <= ackedSeq) {
                return;
            }
            ackedSeq = newAckedSeq;
            while (!ring.isEmpty() && ring.peekFirst().seq() <= ackedSeq) {
                ring.pollFirst();
            }
            reportDepthLocked();
            spaceAvailable.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns everything that must be re-sent so the peer reaches the current head again, given the
     * last contiguous seq the peer reports having delivered.
     * <ul>
     *   <li>If {@code lastContiguousSeq >= highestSeq} the peer is fully caught up → empty entries.</li>
     *   <li>If {@code (lastContiguousSeq + 1)} is still buffered (or the buffer is empty and the
     *       peer is caught up) → the ordered list of entries with {@code seq > lastContiguousSeq}.</li>
     *   <li>If {@code (lastContiguousSeq + 1)} has already been evicted from the ring head → a
     *       {@link ReplayResult#gap()} carrying the lowest seq still available, so the sender can
     *       tell the receiver to resync forward past the lost range.</li>
     * </ul>
     *
     * @param lastContiguousSeq the peer's last contiguously-delivered seq (0 on a fresh connect)
     * @return the replay result (entries to re-send, or a gap)
     */
    public ReplayResult replayFrom(long lastContiguousSeq) {
        lock.lock();
        try {
            // Peer already has everything we have produced: nothing to backfill.
            if (lastContiguousSeq >= highestSeq) {
                return ReplayResult.ofEntries(List.of());
            }

            long wantSeq = lastContiguousSeq + 1;

            if (ring.isEmpty()) {
                // Nothing buffered but the peer is behind highestSeq → those entries were evicted.
                // The first still-available seq is highestSeq + 1 (everything up to here is gone).
                return ReplayResult.ofGap(highestSeq + 1);
            }

            long firstAvailable = ring.peekFirst().seq();
            if (wantSeq < firstAvailable) {
                // The resume point sits before the ring head: the gap (wantSeq .. firstAvailable-1)
                // was dropped/evicted. Signal it so the receiver resyncs to firstAvailable - 1.
                return ReplayResult.ofGap(firstAvailable);
            }

            // The resume point is inside (or exactly at the head of) the ring: collect everything
            // strictly greater than lastContiguousSeq in order.
            List<Entry> out = new ArrayList<>();
            for (Entry e : ring) {
                if (e.seq() > lastContiguousSeq) {
                    out.add(e);
                }
            }
            return ReplayResult.ofEntries(out);
        } finally {
            lock.unlock();
        }
    }

    /** Current number of buffered (un-acked) entries. Visible for tests and the depth gauge. */
    public int size() {
        lock.lock();
        try {
            return ring.size();
        } finally {
            lock.unlock();
        }
    }

    /** Highest seq appended so far (for tests). */
    public long highestSeq() {
        lock.lock();
        try {
            return highestSeq;
        } finally {
            lock.unlock();
        }
    }

    // -------------------------------------------------------------------------
    // Internals (lock held by caller)
    // -------------------------------------------------------------------------

    /**
     * Releases this buffer's contribution to the aggregate depth gauge. Called by
     * {@link ClusterManager} when the buffer is expired and removed, so the cluster-wide gauge does
     * not leak the depth of a dropped target.
     */
    public void release() {
        lock.lock();
        try {
            if (lastReportedDepth != 0) {
                metrics.addReplayBufferDepth(-lastReportedDepth);
                lastReportedDepth = 0;
            }
        } finally {
            lock.unlock();
        }
    }

    private boolean isFullLocked() {
        return ring.size() >= capacity;
    }

    private void recordGapLocked() {
        metrics.incrementClusterGapEvents();
    }

    /** Reports the change in ring size to the aggregate depth gauge as a signed delta. */
    private void reportDepthLocked() {
        int now = ring.size();
        int delta = now - lastReportedDepth;
        if (delta != 0) {
            metrics.addReplayBufferDepth(delta);
            lastReportedDepth = now;
        }
    }
}

