package de.kyle.avenue.cluster.membership;

import de.kyle.avenue.cluster.ClusterEnvelopes;
import de.kyle.avenue.cluster.PeerLink;
import de.kyle.avenue.cluster.events.ClusterEvents;
import de.kyle.avenue.config.ClusterTuning;
import de.kyle.avenue.metrics.ClusterMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Core SWIM membership protocol (Phase E), running over the existing authenticated TCP
 * {@link PeerLink}s — no UDP. SWIM control messages are application-level packets with their own
 * timeouts, so failure detection never relies on TCP keepalive.
 * <p>
 * Responsibilities:
 * <ul>
 *   <li><b>Failure detection</b>: a probe loop pings one random ALIVE member per period; on timeout
 *       it asks {@code k} helpers to probe indirectly; if still silent the target is marked SUSPECT.
 *       A suspicion reaper promotes SUSPECT to DEAD after the suspicion timeout.</li>
 *   <li><b>Dissemination</b>: every SWIM packet piggybacks a bounded set of "hot" {@link GossipUpdate}s
 *       so membership facts spread epidemically. Each update is retired after it has been sent enough
 *       times (~{@code 3*log2(n+1)}).</li>
 *   <li><b>Refute</b>: a SUSPECT/DEAD gossiped about the <em>local</em> node bumps the local
 *       incarnation and re-gossips ALIVE, defeating false positives and restart ghosts.</li>
 *   <li><b>Join/Leave</b>: {@link #handleJoin}/{@link #handleJoinAck} bootstrap discovery; a graceful
 *       {@link #stop()} floods a {@code SwimLeave} so peers converge to LEFT almost immediately.</li>
 * </ul>
 * The {@link #onLinkClosed(String)} hook lets the transport's heartbeat watchdog be the first line of
 * failure detection: a broken TCP link immediately marks the peer SUSPECT.
 */
public final class SwimMembership implements SwimMessageSink {

    private static final Logger log = LoggerFactory.getLogger(SwimMembership.class);

    private final String localNodeId;
    private final String localHost;
    private final int localClusterPort;

    private final AtomicLong localIncarnation;
    private final MemberRegistry registry;
    private final ClusterTuning tuning;
    private final ClusterMetrics metrics;
    /** Resolves a peer's live {@link PeerLink} (or {@code null} if not currently connected). */
    private final Function<String, PeerLink> linkLookup;

    private final AtomicLong swimSeq = new AtomicLong(0);
    private final ConcurrentHashMap<Long, CompletableFuture<Void>> pendingProbes = new ConcurrentHashMap<>();

    /** Recently-changed membership facts awaiting dissemination, keyed by subject node id. */
    private final ConcurrentHashMap<String, HotGossip> hotGossip = new ConcurrentHashMap<>();

    private volatile boolean running = false;
    private Thread probeThread;
    private Thread reaperThread;

    /** A hot-list entry: the fact to gossip plus how many times it has been piggybacked. */
    private static final class HotGossip {
        final GossipUpdate update;
        int sendCount;

        HotGossip(GossipUpdate update) {
            this.update = update;
        }
    }

    public SwimMembership(
            String localNodeId,
            String localHost,
            int localClusterPort,
            long initialIncarnation,
            MemberRegistry registry,
            ClusterTuning tuning,
            ClusterMetrics metrics,
            Function<String, PeerLink> linkLookup
    ) {
        this.localNodeId = localNodeId;
        this.localHost = localHost;
        this.localClusterPort = localClusterPort;
        this.localIncarnation = new AtomicLong(initialIncarnation);
        this.registry = registry;
        this.tuning = tuning;
        this.metrics = metrics;
        this.linkLookup = linkLookup;
    }

    public long getLocalIncarnation() {
        return localIncarnation.get();
    }

    /** Bumps the local incarnation to at least {@code floor + 1} (used after a JoinAck). */
    public void bumpIncarnationAtLeast(long floor) {
        localIncarnation.updateAndGet(cur -> Math.max(cur, floor) + 1);
    }

    public MemberRegistry getRegistry() {
        return registry;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    public void start() {
        running = true;
        probeThread = Thread.ofVirtual().name("swim-probe-" + localNodeId).start(this::probeLoop);
        reaperThread = Thread.ofVirtual().name("swim-reaper-" + localNodeId).start(this::reaperLoop);
    }

    /**
     * Graceful leave: bump incarnation, flood a {@code SwimLeave} to all connected peers and
     * give it a brief moment to propagate before the caller tears the links down.
     */
    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        long inc = localIncarnation.incrementAndGet();
        de.kyle.avenue.proto.ClusterEnvelope leave = ClusterEnvelopes.swimLeave(localNodeId, inc);
        for (Member m : registry.aliveMembers()) {
            PeerLink link = linkLookup.apply(m.getNodeId());
            if (link != null) {
                link.enqueueControl(leave);
                metrics.incrementGossipMessagesSent();
            }
        }
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (probeThread != null) probeThread.interrupt();
        if (reaperThread != null) reaperThread.interrupt();
        for (CompletableFuture<Void> f : pendingProbes.values()) {
            f.complete(null);
        }
        pendingProbes.clear();
    }

    // -------------------------------------------------------------------------
    // Probe loop (direct + indirect failure detection)
    // -------------------------------------------------------------------------

    private void probeLoop() {
        try {
            while (running) {
                Thread.sleep(tuning.swimProbeIntervalMs());
                if (!running) {
                    break;
                }
                // Only probe ALIVE members we currently have a live link to. A member we know of but
                // have not yet connected to (e.g. just learned via a JoinAck) is the LinkSupervisor's
                // job to connect; suspecting it merely for the missing link would be a false positive.
                Member target = pickRandomLinkedAlive();
                if (target == null) {
                    continue;
                }
                probeOnce(target);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            if (running) {
                log.warn("SWIM probe loop error: {}", e.getMessage());
            }
        }
    }

    private void probeOnce(Member target) throws InterruptedException {
        String targetId = target.getNodeId();
        long seq = swimSeq.incrementAndGet();
        CompletableFuture<Void> future = new CompletableFuture<>();
        pendingProbes.put(seq, future);
        try {
            PeerLink link = linkLookup.apply(targetId);
            if (link == null) {
                // The link dropped between selection and probe: the onLinkClosed watchdog hook owns
                // that transition, so just skip this round rather than double-suspecting here.
                pendingProbes.remove(seq);
                return;
            }
            link.enqueueControl(ClusterEnvelopes.swimPing(seq, drainGossip()));
            metrics.incrementGossipMessagesSent();

            if (awaitProbe(future, tuning.swimProbeTimeoutMs())) {
                return; // direct ack
            }

            // Direct probe timed out: ask k helpers to probe the target indirectly.
            List<Member> helpers = pickRandomAlive(targetId, tuning.swimIndirectProbeCount());
            if (!helpers.isEmpty()) {
                for (Member helper : helpers) {
                    PeerLink hlink = linkLookup.apply(helper.getNodeId());
                    if (hlink != null) {
                        hlink.enqueueControl(ClusterEnvelopes.swimPingReq(targetId, seq, drainGossip()));
                        metrics.incrementIndirectProbesSent();
                        metrics.incrementGossipMessagesSent();
                    }
                }
                if (awaitProbe(future, tuning.swimProbeTimeoutMs())) {
                    return; // indirect ack
                }
            }
            // Still no ack: suspect.
            markSuspect(target);
        } finally {
            pendingProbes.remove(seq);
        }
    }

    private boolean awaitProbe(CompletableFuture<Void> future, long timeoutMs) throws InterruptedException {
        try {
            future.get(timeoutMs, TimeUnit.MILLISECONDS);
            return true;
        } catch (java.util.concurrent.TimeoutException e) {
            return false;
        } catch (java.util.concurrent.ExecutionException e) {
            return false;
        }
    }

    private void markSuspect(Member target) {
        if (target.getState() != MemberState.ALIVE) {
            return;
        }
        boolean changed = registry.apply(target.getNodeId(), target.getHost(), target.getClusterPort(),
                target.getIncarnation(), MemberState.SUSPECT, System.currentTimeMillis());
        if (changed) {
            metrics.incrementSuspectEvents();
            gossipNow(target.getNodeId(), MemberState.SUSPECT, target.getIncarnation(),
                    target.getHost(), target.getClusterPort());
            refreshGauges();
            ClusterEvents.suspect(target.getNodeId(), target.getIncarnation());
        }
    }

    // -------------------------------------------------------------------------
    // Suspicion + dead reaper
    // -------------------------------------------------------------------------

    private void reaperLoop() {
        long tick = Math.max(50L, Math.min(tuning.swimSuspicionTimeoutMs(), tuning.swimProbeIntervalMs()));
        try {
            while (running) {
                Thread.sleep(tick);
                if (!running) {
                    break;
                }
                long now = System.currentTimeMillis();
                // SUSPECT -> DEAD after the suspicion timeout.
                for (Member m : registry.snapshot()) {
                    if (m.getState() == MemberState.SUSPECT
                            && now - m.getStateChangedAtMillis() >= tuning.swimSuspicionTimeoutMs()) {
                        boolean changed = registry.apply(m.getNodeId(), m.getHost(), m.getClusterPort(),
                                m.getIncarnation(), MemberState.DEAD, now);
                        if (changed) {
                            metrics.incrementDeadEvents();
                            gossipNow(m.getNodeId(), MemberState.DEAD, m.getIncarnation(),
                                    m.getHost(), m.getClusterPort());
                            ClusterEvents.dead(m.getNodeId(), m.getIncarnation());
                        }
                    }
                }
                // Reap terminal members after the dead-member timeout.
                List<String> reaped = registry.reapDead(tuning.swimDeadMemberTimeoutMs(), now);
                for (String id : reaped) {
                    hotGossip.remove(id);
                    ClusterEvents.reaped(id);
                }
                refreshGauges();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // -------------------------------------------------------------------------
    // Watchdog integration
    // -------------------------------------------------------------------------

    /**
     * Called by the transport when a peer link closes unexpectedly: if the peer was ALIVE we mark it
     * SUSPECT at once, so there is no window where the watchdog has fired but SWIM still thinks the
     * node is alive. Indirect probing then confirms DEAD or the peer refutes via a higher incarnation.
     */
    public void onLinkClosed(String nodeId) {
        Member m = registry.get(nodeId);
        if (m != null && m.getState() == MemberState.ALIVE) {
            markSuspect(m);
        }
    }

    // -------------------------------------------------------------------------
    // SwimMessageSink — received packet handling
    // -------------------------------------------------------------------------

    @Override
    public void onSwimPing(String fromNodeId, long seqNo, List<GossipUpdate> piggyback) {
        metrics.incrementGossipMessagesReceived();
        mergeGossip(piggyback);
        // Reply with an ack carrying our hot gossip.
        PeerLink link = linkLookup.apply(fromNodeId);
        if (link != null) {
            link.enqueueControl(ClusterEnvelopes.swimAck(seqNo, drainGossip()));
            metrics.incrementGossipMessagesSent();
        }
    }

    @Override
    public void onSwimAck(String fromNodeId, long seqNo, List<GossipUpdate> piggyback) {
        metrics.incrementGossipMessagesReceived();
        mergeGossip(piggyback);
        completeProbe(seqNo);
    }

    @Override
    public void onSwimPingReq(String fromNodeId, String targetNodeId, long seqNo, List<GossipUpdate> piggyback) {
        metrics.incrementGossipMessagesReceived();
        mergeGossip(piggyback);
        // Helper role: ping the target on the requester's behalf, then ack back on success.
        PeerLink targetLink = linkLookup.apply(targetNodeId);
        if (targetLink == null) {
            return; // we have no link to the target either; we cannot help
        }
        long helperSeq = swimSeq.incrementAndGet();
        CompletableFuture<Void> future = new CompletableFuture<>();
        pendingProbes.put(helperSeq, future);
        targetLink.enqueueControl(ClusterEnvelopes.swimPing(helperSeq, drainGossip()));
        metrics.incrementGossipMessagesSent();
        // Wait for the target's ack on a short-lived virtual thread, then relay a ping-req-ack.
        Thread.ofVirtual().name("swim-pingreq-helper").start(() -> {
            try {
                boolean ok = awaitProbe(future, tuning.swimProbeTimeoutMs());
                if (ok) {
                    PeerLink back = linkLookup.apply(fromNodeId);
                    if (back != null) {
                        back.enqueueControl(ClusterEnvelopes.swimPingReqAck(targetNodeId, seqNo, drainGossip()));
                        metrics.incrementGossipMessagesSent();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                pendingProbes.remove(helperSeq);
            }
        });
    }

    @Override
    public void onSwimPingReqAck(String fromNodeId, String targetNodeId, long seqNo, List<GossipUpdate> piggyback) {
        metrics.incrementGossipMessagesReceived();
        mergeGossip(piggyback);
        // An indirect helper reached the target: complete the prober's original pending probe.
        completeProbe(seqNo);
    }

    @Override
    public void onSwimJoin(String fromNodeId, String host, int clusterPort, long incarnation) {
        metrics.incrementGossipMessagesReceived();
        handleJoin(fromNodeId, host, clusterPort, incarnation);
        // Answer with the full member snapshot so the joiner discovers everyone (and its own last
        // known incarnation, if we have a stale record for it).
        PeerLink link = linkLookup.apply(fromNodeId);
        if (link != null) {
            link.enqueueControl(ClusterEnvelopes.swimJoinAck(fullMemberSnapshot()));
            metrics.incrementGossipMessagesSent();
        }
    }

    @Override
    public void onSwimJoinAck(String fromNodeId, List<GossipUpdate> memberList) {
        metrics.incrementGossipMessagesReceived();
        // Extract our own last-known incarnation from the snapshot and bump past it (anti-ghost).
        for (GossipUpdate u : memberList) {
            if (u.nodeId().equals(localNodeId)) {
                bumpIncarnationAtLeast(u.incarnation());
            }
        }
        mergeGossip(memberList);
        refreshGauges();
    }

    @Override
    public void onSwimLeave(String fromNodeId, long incarnation) {
        metrics.incrementGossipMessagesReceived();
        Member m = registry.get(fromNodeId);
        String host = m != null ? m.getHost() : "";
        int port = m != null ? m.getClusterPort() : 0;
        boolean changed = registry.apply(fromNodeId, host, port, incarnation, MemberState.LEFT,
                System.currentTimeMillis());
        if (changed) {
            metrics.incrementLeaveEvents();
            gossipNow(fromNodeId, MemberState.LEFT, incarnation, host, port);
            refreshGauges();
            ClusterEvents.leave(fromNodeId, incarnation);
        }
    }

    /** Adds the joining member as ALIVE and gossips it. */
    private void handleJoin(String nodeId, String host, int clusterPort, long incarnation) {
        if (nodeId.equals(localNodeId)) {
            return;
        }
        boolean changed = registry.apply(nodeId, host, clusterPort, incarnation, MemberState.ALIVE,
                System.currentTimeMillis());
        if (changed) {
            metrics.incrementJoinEvents();
            gossipNow(nodeId, MemberState.ALIVE, incarnation, host, clusterPort);
            refreshGauges();
            ClusterEvents.join(nodeId, incarnation, host, clusterPort);
        }
    }

    // -------------------------------------------------------------------------
    // Gossip merge + dissemination
    // -------------------------------------------------------------------------

    /**
     * Merges a batch of received gossip facts. Own-node SUSPECT/DEAD facts trigger a refute (bump
     * incarnation + re-gossip ALIVE); everything else is applied via the registry merge rules and, if
     * it caused a transition, re-added to the hot list to keep the epidemic going.
     */
    private void mergeGossip(List<GossipUpdate> updates) {
        if (updates == null || updates.isEmpty()) {
            return;
        }
        long now = System.currentTimeMillis();
        for (GossipUpdate u : updates) {
            if (u.nodeId().equals(localNodeId)) {
                refuteIfNeeded(u);
                continue;
            }
            MemberState before = stateOf(u.nodeId());
            boolean changed = registry.applyGossip(u, now);
            if (changed) {
                MemberState after = stateOf(u.nodeId());
                accountTransition(before, after);
                // Re-disseminate the fact we just learned.
                addHot(new GossipUpdate(u.nodeId(), after.name(),
                        incarnationOf(u.nodeId(), u.incarnation()), u.host(), u.clusterPort()));
            }
        }
        refreshGauges();
    }

    /** If a peer gossips that WE are SUSPECT/DEAD, bump our incarnation and re-assert ALIVE. */
    private void refuteIfNeeded(GossipUpdate u) {
        MemberState s;
        try {
            s = MemberState.valueOf(u.state());
        } catch (IllegalArgumentException e) {
            return;
        }
        if ((s == MemberState.SUSPECT || s == MemberState.DEAD) && u.incarnation() >= localIncarnation.get()) {
            long bumped = localIncarnation.updateAndGet(cur -> Math.max(cur, u.incarnation()) + 1);
            metrics.incrementIncarnationConflicts();
            gossipNow(localNodeId, MemberState.ALIVE, bumped, localHost, localClusterPort);
            ClusterEvents.refute(s.name(), bumped);
        }
    }

    private void accountTransition(MemberState before, MemberState after) {
        if (before == after) {
            return;
        }
        switch (after) {
            case ALIVE -> {
                if (before == null) metrics.incrementJoinEvents();
            }
            case SUSPECT -> metrics.incrementSuspectEvents();
            case DEAD -> metrics.incrementDeadEvents();
            case LEFT -> metrics.incrementLeaveEvents();
        }
    }

    /** Records a fact in the hot list and immediately schedules it for the next outgoing packets. */
    private void gossipNow(String nodeId, MemberState state, long incarnation, String host, int port) {
        addHot(new GossipUpdate(nodeId, state.name(), incarnation, host, port));
    }

    private void addHot(GossipUpdate update) {
        hotGossip.put(update.nodeId(), new HotGossip(update));
    }

    /**
     * Selects up to {@code gossip-fanout} hot updates for the next packet, preferring the least-sent
     * ones, increments their send counts, and retires any that have been sent enough times
     * (~{@code 3*log2(n+1)}, where n is the current member count).
     */
    private List<GossipUpdate> drainGossip() {
        if (hotGossip.isEmpty()) {
            return List.of();
        }
        int n = registry.snapshot().size();
        int threshold = Math.max(2, (int) Math.ceil(3.0 * (Math.log(n + 1.0) / Math.log(2.0))));

        List<HotGossip> all = new ArrayList<>(hotGossip.values());
        all.sort((a, b) -> Integer.compare(a.sendCount, b.sendCount));

        int fanout = Math.max(1, tuning.swimGossipFanout());
        List<GossipUpdate> picked = new ArrayList<>(Math.min(fanout, all.size()));
        for (int i = 0; i < all.size() && picked.size() < fanout; i++) {
            HotGossip hg = all.get(i);
            picked.add(hg.update);
            hg.sendCount++;
            if (hg.sendCount >= threshold) {
                hotGossip.remove(hg.update.nodeId(), hg);
            }
        }
        return picked;
    }

    /** Full snapshot of all known members for a JoinAck (including terminal ones for anti-ghost). */
    private List<GossipUpdate> fullMemberSnapshot() {
        List<GossipUpdate> result = new ArrayList<>();
        for (Member m : registry.snapshot()) {
            result.add(new GossipUpdate(m.getNodeId(), m.getState().name(), m.getIncarnation(),
                    m.getHost(), m.getClusterPort()));
        }
        return result;
    }

    /**
     * Merges a member snapshot received over a seed join (called from ClusterManager's join flow).
     * Triggers onAlive callbacks (via the registry listener) so the link supervisor connects out to
     * the discovered members.
     */
    public void mergeMemberList(List<GossipUpdate> memberList) {
        for (GossipUpdate u : memberList) {
            if (u.nodeId().equals(localNodeId)) {
                bumpIncarnationAtLeast(u.incarnation());
            }
        }
        mergeGossip(memberList);
    }

    /** Announces this node as ALIVE to a specific peer's view via gossip (used after a join). */
    public void announceSelfAlive() {
        gossipNow(localNodeId, MemberState.ALIVE, localIncarnation.get(), localHost, localClusterPort);
    }

    /**
     * Registers a directly-connected peer as ALIVE (from a handshake / join) AND schedules its ALIVE
     * fact for gossip so the rest of the cluster discovers it. Unconditional gossip (independent of
     * whether the registry transition fired) is what lets a node learned only via a direct link still
     * propagate to indirect peers.
     */
    public void noteAliveAndGossip(String nodeId, String host, int clusterPort, long incarnation) {
        if (nodeId.equals(localNodeId)) {
            return;
        }
        boolean changed = registry.apply(nodeId, host, clusterPort, incarnation, MemberState.ALIVE,
                System.currentTimeMillis());
        if (changed) {
            metrics.incrementJoinEvents();
        }
        gossipNow(nodeId, MemberState.ALIVE, incarnation, host, clusterPort);
        refreshGauges();
    }

    /** True if the membership engine has a usable (caller-resolved) link to {@code nodeId}. */
    private boolean hasLink(String nodeId) {
        return linkLookup.apply(nodeId) != null;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void completeProbe(long seqNo) {
        CompletableFuture<Void> f = pendingProbes.remove(seqNo);
        if (f != null) {
            f.complete(null);
        }
    }

    /** A random ALIVE member that we currently hold a live link to (the probe target pool). */
    private Member pickRandomLinkedAlive() {
        List<Member> alive = registry.aliveMembers();
        alive.removeIf(m -> !hasLink(m.getNodeId()));
        if (alive.isEmpty()) {
            return null;
        }
        return alive.get(ThreadLocalRandom.current().nextInt(alive.size()));
    }

    /** Up to {@code k} random ALIVE helpers (excluding {@code exclude}) that we hold live links to. */
    private List<Member> pickRandomAlive(String exclude, int k) {
        List<Member> alive = registry.aliveMembers();
        alive.removeIf(m -> m.getNodeId().equals(exclude) || !hasLink(m.getNodeId()));
        Collections.shuffle(alive, ThreadLocalRandom.current());
        if (alive.size() > k) {
            return alive.subList(0, k);
        }
        return alive;
    }

    private MemberState stateOf(String nodeId) {
        Member m = registry.get(nodeId);
        return m == null ? null : m.getState();
    }

    private long incarnationOf(String nodeId, long fallback) {
        Member m = registry.get(nodeId);
        return m == null ? fallback : m.getIncarnation();
    }

    private void refreshGauges() {
        long alive = 0;
        long suspect = 0;
        long dead = 0;
        for (Member m : registry.snapshot()) {
            switch (m.getState()) {
                case ALIVE -> alive++;
                case SUSPECT -> suspect++;
                case DEAD, LEFT -> dead++;
            }
        }
        metrics.setMembersAlive(alive);
        metrics.setMembersSuspect(suspect);
        metrics.setMembersDead(dead);
    }
}
