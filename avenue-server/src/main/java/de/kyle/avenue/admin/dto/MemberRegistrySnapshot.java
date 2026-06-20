package de.kyle.avenue.admin.dto;

import java.util.List;

/**
 * Immutable, read-only membership snapshot for admin introspection (Phase F): the per-member
 * {@link MemberSnapshot} list plus the aggregate ALIVE/SUSPECT/DEAD counts, all computed in one
 * consistent pass so {@code /cluster/members} and {@code /health} agree.
 *
 * @param alivecount   number of ALIVE remote members
 * @param suspectCount number of SUSPECT remote members
 * @param deadCount    number of DEAD or LEFT remote members
 * @param members      unmodifiable list of all tracked remote members
 */
public record MemberRegistrySnapshot(
        int alivecount,
        int suspectCount,
        int deadCount,
        List<MemberSnapshot> members
) {
}
