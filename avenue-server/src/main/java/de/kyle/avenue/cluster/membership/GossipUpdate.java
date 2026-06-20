package de.kyle.avenue.cluster.membership;

import de.kyle.avenue.proto.MemberStateProto;

/**
 * A single membership fact piggybacked on SWIM control packets (Phase E gossip dissemination).
 * <p>
 * It is a plain DTO, not a framed wire message: many of these ride inside one SWIM packet as a
 * {@code repeated GossipUpdate} on the wire. The domain {@code state} stays a String
 * ({@code ALIVE/SUSPECT/DEAD/LEFT}) so the registry merge rules can keep parsing it via
 * {@link MemberState#valueOf(String)}; the protobuf boundary converts it to/from the
 * {@link MemberStateProto} enum in {@link #toProto()} / {@link #fromProto}.
 *
 * @param nodeId      the subject member's node id
 * @param state       the gossiped {@link MemberState} name ({@code ALIVE/SUSPECT/DEAD/LEFT})
 * @param incarnation the subject member's incarnation at the time of the gossip
 * @param host        the subject member's advertised host (for dynamic discovery + outbound connect)
 * @param clusterPort the subject member's advertised cluster port
 */
public record GossipUpdate(String nodeId, String state, long incarnation, String host, int clusterPort) {

    /**
     * Converts this update to its protobuf form, mapping the domain {@code state} String to the
     * {@link MemberStateProto} enum. An unparseable state defaults to {@code ALIVE}, mirroring the
     * proto3 default so a corrupt fact never crashes the gossip path.
     */
    public de.kyle.avenue.proto.GossipUpdate toProto() {
        return de.kyle.avenue.proto.GossipUpdate.newBuilder()
                .setNodeId(nodeId)
                .setState(toProtoState(state))
                .setIncarnation(incarnation)
                .setHost(host)
                .setClusterPort(clusterPort)
                .build();
    }

    /** Parses one update from its protobuf form, mapping the {@link MemberStateProto} enum to a String. */
    public static GossipUpdate fromProto(de.kyle.avenue.proto.GossipUpdate p) {
        return new GossipUpdate(
                p.getNodeId(),
                fromProtoState(p.getState()),
                p.getIncarnation(),
                p.getHost(),
                p.getClusterPort());
    }

    /** Maps the domain state String to the protobuf enum; unknown/garbage maps to {@code ALIVE}. */
    private static MemberStateProto toProtoState(String state) {
        if (state == null) {
            return MemberStateProto.ALIVE;
        }
        return switch (state) {
            case "SUSPECT" -> MemberStateProto.SUSPECT;
            case "DEAD" -> MemberStateProto.DEAD;
            case "LEFT" -> MemberStateProto.LEFT;
            default -> MemberStateProto.ALIVE;
        };
    }

    /** Maps the protobuf enum back to the domain state String; {@code UNRECOGNIZED} maps to {@code ALIVE}. */
    private static String fromProtoState(MemberStateProto state) {
        return switch (state) {
            case SUSPECT -> "SUSPECT";
            case DEAD -> "DEAD";
            case LEFT -> "LEFT";
            default -> "ALIVE";
        };
    }
}
