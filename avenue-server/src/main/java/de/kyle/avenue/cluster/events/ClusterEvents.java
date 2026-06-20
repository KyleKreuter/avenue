package de.kyle.avenue.cluster.events;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Structured cluster event logging (Phase F).
 * <p>
 * All notable cluster lifecycle transitions are emitted through the single logger
 * {@code de.kyle.avenue.cluster.events} in a consistent, machine-parseable {@code key=value}
 * format, e.g.:
 * <pre>
 *   event=join nodeId=node-2 incarnation=7 host=10.0.0.2 port=7100
 *   event=suspect nodeId=node-3 incarnation=4
 *   event=peer-connected nodeId=node-2 host=10.0.0.2 port=7100
 * </pre>
 * This is purely a <em>structuring</em> of the most important existing {@code log.info/warn/error}
 * calls in {@link de.kyle.avenue.cluster.ClusterManager}, {@link de.kyle.avenue.cluster.PeerLink}
 * and {@link de.kyle.avenue.cluster.membership.SwimMembership}; it changes no behaviour. The class
 * intentionally has no per-event state and only does cheap string building, so it is safe to call
 * from the cluster hot path.
 * <p>
 * Severity mapping follows the operational importance of the event:
 * <ul>
 *   <li><b>INFO</b> — normal lifecycle: {@code join}, {@code leave}, {@code peer-connected},
 *       {@code peer-disconnected}, {@code backfill-completed}, {@code reaped}.</li>
 *   <li><b>WARN</b> — degraded but recoverable: {@code suspect}, {@code reconnect},
 *       {@code heartbeat-timeout}, {@code queue-full}, {@code handshake-auth-failure},
 *       {@code refute}, {@code gap}.</li>
 *   <li><b>ERROR</b> — durable problems: {@code dead}, {@code partition}, {@code data-loss}.</li>
 * </ul>
 */
public final class ClusterEvents {

    /** The dedicated structured-event logger; configure it independently in {@code logback.xml}. */
    private static final Logger log = LoggerFactory.getLogger("de.kyle.avenue.cluster.events");

    private ClusterEvents() {
    }

    // ------------------------------------------------------------------
    // INFO — normal lifecycle
    // ------------------------------------------------------------------

    /** A remote member joined / became ALIVE. */
    public static void join(String nodeId, long incarnation, String host, int port) {
        log.info("event=join nodeId={} incarnation={} host={} port={}", nodeId, incarnation, host, port);
    }

    /** A remote member announced a graceful leave (LEFT). */
    public static void leave(String nodeId, long incarnation) {
        log.info("event=leave nodeId={} incarnation={}", nodeId, incarnation);
    }

    /** A peer link was established (handshake complete, link active). */
    public static void peerConnected(String nodeId, String host, int port) {
        log.info("event=peer-connected nodeId={} host={} port={}", nodeId, host, port);
    }

    /** A peer link was removed from the active set (crash / partition / drop / watchdog). */
    public static void peerDisconnected(String nodeId) {
        log.info("event=peer-disconnected nodeId={}", nodeId);
    }

    /** Reconnect backfill finished: {@code count} buffered messages were re-sent to a recovered peer. */
    public static void backfillCompleted(String nodeId, long count, long fromLinkSeq) {
        log.info("event=backfill-completed nodeId={} messages={} fromLinkSeq={}", nodeId, count, fromLinkSeq);
    }

    /** A terminal (DEAD/LEFT) member was reaped from the registry. */
    public static void reaped(String nodeId) {
        log.info("event=reaped nodeId={}", nodeId);
    }

    // ------------------------------------------------------------------
    // WARN — degraded but recoverable
    // ------------------------------------------------------------------

    /** A member transitioned to SUSPECT (probe / indirect probe failed, or link dropped). */
    public static void suspect(String nodeId, long incarnation) {
        log.warn("event=suspect nodeId={} incarnation={}", nodeId, incarnation);
    }

    /** An outbound link is being (re)attempted to a peer. */
    public static void reconnect(String nodeId, String host, int port, long backoffMs) {
        log.warn("event=reconnect nodeId={} host={} port={} backoffMs={}", nodeId, host, port, backoffMs);
    }

    /** The transport heartbeat watchdog fired: no heartbeat within the timeout window. */
    public static void heartbeatTimeout(String nodeId) {
        log.warn("event=heartbeat-timeout nodeId={}", nodeId);
    }

    /** A control / publish queue towards a peer was full and an item was dropped. */
    public static void queueFull(String nodeId, String what) {
        log.warn("event=queue-full nodeId={} dropped={}", nodeId, what);
    }

    /** A peer handshake was rejected because the HMAC proof did not verify (wrong cluster secret). */
    public static void handshakeAuthFailure(String remote) {
        log.warn("event=handshake-auth-failure remote={}", remote);
    }

    /** This node refuted a SUSPECT/DEAD gossip about itself by bumping its incarnation. */
    public static void refute(String state, long newIncarnation) {
        log.warn("event=refute refutedState={} newIncarnation={}", state, newIncarnation);
    }

    /** A replay gap was detected/declared on a link (requested range no longer available). */
    public static void gap(String nodeId, long firstAvailableSeq) {
        log.warn("event=gap nodeId={} firstAvailableSeq={}", nodeId, firstAvailableSeq);
    }

    // ------------------------------------------------------------------
    // ERROR — durable problems
    // ------------------------------------------------------------------

    /** A member was declared DEAD (suspicion timed out without a refute). */
    public static void dead(String nodeId, long incarnation) {
        log.error("event=dead nodeId={} incarnation={}", nodeId, incarnation);
    }

    /** Accepted, unrecoverable data loss on a link: the receiver resynced its frontier forward. */
    public static void dataLoss(String nodeId, long resyncContiguousSeq) {
        log.error("event=data-loss nodeId={} resyncContiguousLinkSeq={}", nodeId, resyncContiguousSeq);
    }
}
