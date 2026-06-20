package de.kyle.avenue.cluster.membership;

import de.kyle.avenue.admin.dto.MemberRegistrySnapshot;
import de.kyle.avenue.admin.dto.MemberSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.LongUnaryOperator;

/**
 * Thread-safe registry of <em>remote</em> cluster members keyed by node id (Phase E SWIM membership).
 * The local node is never stored here.
 * <p>
 * Applies the SWIM incarnation merge rules so that gossiped facts converge regardless of arrival
 * order, and never let a stale fact overwrite a fresher one:
 * <ul>
 *   <li><b>ALIVE(j)</b> wins only if {@code j > current incarnation} OR the current state is not ALIVE
 *       (an equal-incarnation ALIVE refutes a SUSPECT, e.g. a member that just refuted on the same
 *       incarnation it was suspected at).</li>
 *   <li><b>SUSPECT(j)</b> applies if {@code j >= current incarnation} and the current state is not DEAD.</li>
 *   <li><b>DEAD(j)</b> is sticky: applies if {@code j >= current incarnation}; once DEAD only a strictly
 *       higher incarnation ALIVE (a genuine restart/refute) can revive it.</li>
 *   <li><b>LEFT</b> always applies (graceful, authoritative leave).</li>
 * </ul>
 * Own-node refute is handled by {@link SwimMembership}, not here: this registry only tracks remote
 * members, so it never sees a fact about the local node.
 * <p>
 * Each {@link #applyGossip} call is serialized per member via {@code computeIfAbsent} + a
 * {@code synchronized} block on the member instance, so concurrent gossip threads cannot interleave a
 * read-modify-write on the same member. Listener callbacks fire <em>after</em> the state mutation,
 * outside the per-member lock, to avoid re-entrancy surprises.
 */
public final class MemberRegistry {

    private static final Logger log = LoggerFactory.getLogger(MemberRegistry.class);

    private final ConcurrentHashMap<String, Member> members = new ConcurrentHashMap<>();
    private final CopyOnWriteArraySet<MemberRegistryListener> listeners = new CopyOnWriteArraySet<>();

    public void addListener(MemberRegistryListener listener) {
        listeners.add(listener);
    }

    /** Returns the tracked member or {@code null} if unknown. */
    public Member get(String nodeId) {
        return members.get(nodeId);
    }

    /** Snapshot of all tracked members. */
    public List<Member> snapshot() {
        return new ArrayList<>(members.values());
    }

    /**
     * Immutable, leak-free snapshot of all tracked members plus the aggregate state counts, for
     * admin introspection (Phase F). Each member is copied into a {@link MemberSnapshot} value while
     * holding its monitor, so the admin layer never sees a partially-updated member or holds a
     * reference to live mutable state. The counts are derived from the very same copies, so the
     * member list and the aggregate counts are always internally consistent.
     */
    public MemberRegistrySnapshot snapshotDto() {
        List<MemberSnapshot> snaps = new ArrayList<>(members.size());
        int alive = 0;
        int suspect = 0;
        int dead = 0;
        for (Member m : members.values()) {
            MemberState state;
            long incarnation;
            long changedAt;
            synchronized (m) {
                state = m.getState();
                incarnation = m.getIncarnation();
                changedAt = m.getStateChangedAtMillis();
            }
            snaps.add(new MemberSnapshot(
                    m.getNodeId(), state.name(), incarnation, m.getHost(), m.getClusterPort(), changedAt));
            switch (state) {
                case ALIVE -> alive++;
                case SUSPECT -> suspect++;
                case DEAD, LEFT -> dead++;
            }
        }
        return new MemberRegistrySnapshot(alive, suspect, dead, List.copyOf(snaps));
    }

    /** Number of members currently in {@link MemberState#ALIVE}. */
    public int aliveCount() {
        int count = 0;
        for (Member m : members.values()) {
            if (m.getState() == MemberState.ALIVE) {
                count++;
            }
        }
        return count;
    }

    /** Snapshot of all currently-ALIVE members. */
    public List<Member> aliveMembers() {
        List<Member> result = new ArrayList<>();
        for (Member m : members.values()) {
            if (m.getState() == MemberState.ALIVE) {
                result.add(m);
            }
        }
        return result;
    }

    /**
     * Applies a gossiped membership fact under the SWIM merge rules. Creates the member on first
     * sight (when the fact is ALIVE/SUSPECT/DEAD with contact info). Returns {@code true} if the fact
     * changed the member's effective state (and fired a listener callback).
     *
     * @param nowMillis the current wall clock (injectable for tests)
     */
    public boolean applyGossip(GossipUpdate update, long nowMillis) {
        MemberState incoming;
        try {
            incoming = MemberState.valueOf(update.state());
        } catch (IllegalArgumentException e) {
            log.warn("Ignoring gossip with unknown state '{}' for {}", update.state(), update.nodeId());
            return false;
        }
        return apply(update.nodeId(), update.host(), update.clusterPort(),
                update.incarnation(), incoming, nowMillis);
    }

    /**
     * Direct apply (used by join/leave handling and tests). Same merge rules as {@link #applyGossip}.
     */
    public boolean apply(String nodeId, String host, int clusterPort,
                         long incarnation, MemberState incoming, long nowMillis) {
        // computeIfAbsent both creates-on-first-sight AND tells us whether we created it, so a freshly
        // discovered member fires its initial-state listener exactly once.
        boolean[] created = {false};
        Member member = members.computeIfAbsent(nodeId, k -> {
            created[0] = true;
            return new Member(nodeId, host, clusterPort, incarnation, incoming, nowMillis);
        });

        boolean changed;
        synchronized (member) {
            if (created[0]) {
                changed = true; // first sight: the initial state is itself a transition to report
            } else {
                changed = mergeLocked(member, host, clusterPort, incarnation, incoming, nowMillis);
            }
        }

        if (changed) {
            fireListeners(member, member.getState());
        }
        return changed;
    }

    /**
     * Performs the locked read-modify-write of the merge rules. Returns {@code true} if the effective
     * state changed (so a listener should fire). Must be called while holding {@code member}'s monitor.
     */
    private boolean mergeLocked(Member member, String host, int clusterPort,
                                long incarnation, MemberState incoming, long nowMillis) {
        long currentInc = member.getIncarnation();
        MemberState currentState = member.getState();

        boolean accept = switch (incoming) {
            // ALIVE wins with a strictly higher incarnation always; at an EQUAL incarnation it only
            // refutes a SUSPECT (not a sticky DEAD/LEFT — those need a genuine restart, i.e. a strictly
            // higher incarnation, to be revived).
            case ALIVE -> incarnation > currentInc
                    || (incarnation == currentInc && currentState == MemberState.SUSPECT);
            case SUSPECT -> incarnation >= currentInc && currentState != MemberState.DEAD;
            case DEAD -> incarnation >= currentInc;
            case LEFT -> true;
        };
        if (!accept) {
            return false;
        }
        boolean stateChanged = currentState != incoming;
        boolean incChanged = incarnation > currentInc;
        if (!stateChanged && !incChanged) {
            return false;
        }
        if (incChanged) {
            member.setIncarnation(incarnation);
        }
        if (stateChanged) {
            member.setState(incoming);
            member.setStateChangedAtMillis(nowMillis);
        }
        return stateChanged;
    }

    /** Removes a member outright (DEAD/LEFT reaping). */
    public boolean remove(String nodeId) {
        return members.remove(nodeId) != null;
    }

    /**
     * Sweeps DEAD/LEFT members whose state has been terminal for longer than {@code deadTimeoutMs} and
     * removes them so a future genuine rejoin starts clean. Returns the removed node ids.
     */
    public List<String> reapDead(long deadTimeoutMs, long nowMillis) {
        List<String> removed = new ArrayList<>();
        for (Member m : members.values()) {
            MemberState s = m.getState();
            if ((s == MemberState.DEAD || s == MemberState.LEFT)
                    && nowMillis - m.getStateChangedAtMillis() >= deadTimeoutMs) {
                if (members.remove(m.getNodeId(), m)) {
                    removed.add(m.getNodeId());
                }
            }
        }
        return removed;
    }

    /**
     * Adjusts the incarnation of an existing member with the given function (used by tests / explicit
     * resets). No-op if the member is unknown.
     */
    public void updateIncarnation(String nodeId, LongUnaryOperator op) {
        Member m = members.get(nodeId);
        if (m != null) {
            synchronized (m) {
                m.setIncarnation(op.applyAsLong(m.getIncarnation()));
            }
        }
    }

    private void fireListeners(Member member, MemberState state) {
        for (MemberRegistryListener l : listeners) {
            try {
                switch (state) {
                    case ALIVE -> l.onAlive(member);
                    case SUSPECT -> l.onSuspect(member);
                    case DEAD -> l.onDead(member);
                    case LEFT -> l.onLeft(member);
                }
            } catch (Exception e) {
                log.warn("Membership listener threw on {} {}: {}", state, member.getNodeId(), e.getMessage());
            }
        }
    }
}
