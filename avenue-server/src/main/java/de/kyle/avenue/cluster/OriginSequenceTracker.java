package de.kyle.avenue.cluster;

import java.util.BitSet;

/**
 * Per-origin (per {@code (originNodeId, originEpoch)}) acceptance tracker for cluster publish
 * sequence numbers, replacing the previous {@link MessageDeduplicator}.
 * <p>
 * Instead of a bounded LRU of random message IDs, this tracks the contiguous high-water mark of
 * delivered sequence numbers plus a small bounded reorder window for sequence numbers that arrive
 * out of order ahead of the contiguous frontier. The model:
 * <ul>
 *   <li>{@code contiguousSeq} – every seq {@code <= contiguousSeq} has been delivered.</li>
 *   <li>A bounded {@link BitSet} reorder window covers
 *       {@code (contiguousSeq, contiguousSeq + windowSize]} and records which of those future
 *       seqs already arrived, so the contiguous frontier can roll forward when the gap fills.</li>
 * </ul>
 * Acceptance rules in {@link #accept(long)}:
 * <ul>
 *   <li>{@code seq <= contiguousSeq} → duplicate, reject.</li>
 *   <li>{@code seq == contiguousSeq + 1} → deliver, advance the frontier and drag it forward over
 *       any contiguously-set window bits.</li>
 *   <li>{@code seq > contiguousSeq + 1} and within the window → deliver, mark it in the window
 *       (a forward gap is accepted; Phase A semantics remain best-effort/at-most-once, so missing
 *       earlier seqs are not recovered here).</li>
 *   <li>{@code seq} beyond the window → the window has been outrun; recenter the frontier to just
 *       below {@code seq} and deliver. This bounds memory and tolerates large jumps.</li>
 * </ul>
 * <p>
 * <b>Phase A note:</b> delivery semantics stay broadcast / at-most-once. This tracker is purely a
 * duplicate filter (a strict superset of the old behaviour for the single-hop full-mesh case),
 * and is the foundation that Phase C will extend into gap-aware at-least-once.
 * <p>
 * Thread-safety: all methods are {@code synchronized}; each tracker is touched only by the small
 * number of reader threads delivering from a single origin, so contention is negligible.
 */
public final class OriginSequenceTracker {

    /** Default reorder window size in sequence slots. */
    public static final int DEFAULT_WINDOW_SIZE = 4096;

    private final int windowSize;

    /** Highest seq such that every seq {@code <= contiguousSeq} has been delivered. */
    private long contiguousSeq;

    /**
     * Reorder window covering {@code (contiguousSeq, contiguousSeq + windowSize]}. Bit index
     * {@code i} maps to seq {@code contiguousSeq + 1 + i}. Bits shift as the frontier advances.
     */
    private final BitSet window;

    public OriginSequenceTracker() {
        this(DEFAULT_WINDOW_SIZE);
    }

    public OriginSequenceTracker(int windowSize) {
        if (windowSize <= 0) {
            throw new IllegalArgumentException("windowSize must be positive");
        }
        this.windowSize = windowSize;
        this.contiguousSeq = 0L;
        this.window = new BitSet(windowSize);
    }

    /**
     * Records a received sequence number and reports whether the message should be delivered.
     *
     * @param seq the origin sequence number (1-based, strictly increasing at the origin)
     * @return {@code true} if the message is new and should be delivered, {@code false} if it is a
     *         duplicate that must be dropped
     */
    public synchronized boolean accept(long seq) {
        if (seq <= contiguousSeq) {
            // Already delivered (or pre-frontier) — duplicate.
            return false;
        }

        long offsetLong = seq - contiguousSeq - 1; // 0-based index into the window
        if (offsetLong >= windowSize) {
            // Beyond the reorder window: recenter the frontier just below seq and deliver. This
            // bounds memory on large forward jumps; skipped seqs are treated as gone (at-most-once).
            window.clear();
            contiguousSeq = seq;
            return true;
        }

        int offset = (int) offsetLong;
        if (window.get(offset)) {
            // Out-of-order duplicate already recorded in the window.
            return false;
        }

        window.set(offset);
        if (offset == 0) {
            // Fills the immediate gap: advance the frontier across any further contiguously-set
            // window bits.
            advanceFrontier();
        }
        return true;
    }

    /**
     * Forcibly resets the tracker so that the next expected contiguous seq is {@code seq + 1}.
     * Used when a peer epoch changes or an explicit resync is needed. Clears the reorder window.
     *
     * @param seq the new contiguous high-water mark
     */
    public synchronized void resetTo(long seq) {
        this.contiguousSeq = seq;
        this.window.clear();
    }

    /** Returns the current contiguous high-water mark (test/visibility helper). */
    public synchronized long contiguousSeq() {
        return contiguousSeq;
    }

    /**
     * Rolls the contiguous frontier forward over leading set bits in the window, shifting the
     * window so bit 0 once again maps to {@code contiguousSeq + 1}.
     */
    private void advanceFrontier() {
        int advance = 0;
        while (advance < windowSize && window.get(advance)) {
            advance++;
        }
        if (advance == 0) {
            return;
        }
        contiguousSeq += advance;
        // Shift the window left by {@code advance} bits: bit i becomes bit i-advance.
        BitSet shifted = new BitSet(windowSize);
        for (int i = window.nextSetBit(advance); i >= 0; i = window.nextSetBit(i + 1)) {
            shifted.set(i - advance);
        }
        window.clear();
        window.or(shifted);
    }
}
