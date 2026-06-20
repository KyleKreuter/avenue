package de.kyle.avenue.cluster.membership;

/**
 * Receives membership transition callbacks from the {@link MemberRegistry} (Phase E).
 * <p>
 * Callbacks fire on the thread that applied the state change (a SWIM probe / gossip thread). They
 * must therefore be cheap and non-blocking — the {@code ClusterManager} link supervisor only
 * submits work to its executor in response.
 */
public interface MemberRegistryListener {

    /** The member is (newly) ALIVE: either just discovered or refuted a prior SUSPECT/DEAD. */
    void onAlive(Member member);

    /** The member transitioned to SUSPECT (a probe failed). */
    void onSuspect(Member member);

    /** The member transitioned to DEAD (suspicion timed out without a refute). */
    void onDead(Member member);

    /** The member announced a graceful LEFT. */
    void onLeft(Member member);
}
