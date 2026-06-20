package de.kyle.avenue.cluster;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-node monotonic publish sequencer.
 * <p>
 * Replaces the previous random {@code UUID} {@code messageId} with a compact, ordered identity:
 * {@code (originNodeId, originEpoch, seq)}. The {@code originNodeId} is the node's configured ID,
 * the {@code epoch} pins this process incarnation (so sequence numbers from a restarted node with
 * the same nodeId are never confused with the previous run), and {@code seq} is a strictly
 * increasing per-message counter starting at 1.
 * <p>
 * One instance lives per node, held by {@link ClusterManager}. Thread-safe: {@link #nextSeq()} is
 * an atomic increment shared by all publishing virtual threads.
 */
public final class OriginSequencer {

    private final AtomicLong seq = new AtomicLong(0);
    private final long epoch;

    /** Creates a sequencer whose epoch pins the current process incarnation. */
    public OriginSequencer() {
        this.epoch = System.currentTimeMillis();
    }

    /**
     * Returns the next sequence number. The first call returns {@code 1}; the sequence is
     * strictly increasing and never repeats within this epoch.
     */
    public long nextSeq() {
        return seq.incrementAndGet();
    }

    /** Returns this incarnation's epoch (milliseconds since the Unix epoch at construction). */
    public long epoch() {
        return epoch;
    }
}
