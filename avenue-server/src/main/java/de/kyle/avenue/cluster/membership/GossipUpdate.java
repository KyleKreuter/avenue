package de.kyle.avenue.cluster.membership;

import org.json.JSONObject;

/**
 * A single membership fact piggybacked on SWIM control packets (Phase E gossip dissemination).
 * <p>
 * Serialized as a compact JSON object inside a {@code "piggyback"} array, e.g.
 * <pre>{"nodeId":"node-3","state":"ALIVE","incarnation":1234,"host":"127.0.0.1","clusterPort":9003}</pre>
 * It is a plain DTO, not a framed {@link de.kyle.avenue.packet.Packet}: many of these ride inside one
 * SWIM packet's body.
 *
 * @param nodeId      the subject member's node id
 * @param state       the gossiped {@link MemberState} name ({@code ALIVE/SUSPECT/DEAD/LEFT})
 * @param incarnation the subject member's incarnation at the time of the gossip
 * @param host        the subject member's advertised host (for dynamic discovery + outbound connect)
 * @param clusterPort the subject member's advertised cluster port
 */
public record GossipUpdate(String nodeId, String state, long incarnation, String host, int clusterPort) {

    /** Serializes this update into its compact JSON form. */
    public JSONObject toJson() {
        JSONObject o = new JSONObject();
        o.put("nodeId", nodeId);
        o.put("state", state);
        o.put("incarnation", incarnation);
        o.put("host", host);
        o.put("clusterPort", clusterPort);
        return o;
    }

    /** Parses one update from its compact JSON form. */
    public static GossipUpdate fromJson(JSONObject o) {
        return new GossipUpdate(
                o.optString("nodeId", ""),
                o.optString("state", "ALIVE"),
                o.optLong("incarnation", 0L),
                o.optString("host", ""),
                o.optInt("clusterPort", 0));
    }
}
