package de.kyle.avenue.admin.dto;

/**
 * Immutable, read-only view of a single SWIM member for admin introspection (Phase F).
 * <p>
 * A plain value record copied out of the live {@link de.kyle.avenue.cluster.membership.Member} under
 * the registry's per-member synchronization, so no mutable internal state leaks to the admin HTTP
 * layer.
 *
 * @param nodeId               the member's node id
 * @param state                its current SWIM state (ALIVE / SUSPECT / DEAD / LEFT)
 * @param incarnation          its current incarnation number
 * @param host                 advertised host
 * @param clusterPort          advertised cluster port
 * @param stateChangedAtMillis wall-clock time of the last state transition
 */
public record MemberSnapshot(
        String nodeId,
        String state,
        long incarnation,
        String host,
        int clusterPort,
        long stateChangedAtMillis
) {
}
