package de.kyle.avenue.cluster.membership;

/**
 * Lifecycle state of a remote cluster member in the SWIM membership model (Phase E).
 * <ul>
 *   <li>{@link #ALIVE} — the member responds to (direct or indirect) probes.</li>
 *   <li>{@link #SUSPECT} — a probe to the member failed; the member is suspected dead but may still
 *       refute the suspicion by gossiping a higher incarnation.</li>
 *   <li>{@link #DEAD} — the suspicion timed out without a refute; the member is declared dead. Sticky.</li>
 *   <li>{@link #LEFT} — the member announced a graceful leave; it is gone for good.</li>
 * </ul>
 */
public enum MemberState {
    ALIVE,
    SUSPECT,
    DEAD,
    LEFT
}
