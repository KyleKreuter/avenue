package de.kyle.avenue.packet.cluster;

import de.kyle.avenue.packet.Packet;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

/**
 * SWIM join request (Phase E). A node sends this over a freshly-established seed link to announce
 * itself to the cluster: its node id, advertised contact info and current process incarnation. The
 * seed answers with a {@link SwimJoinAckPacket} carrying the full known member list.
 */
public class SwimJoinPacket implements Packet {

    private final String nodeId;
    private final String host;
    private final int clusterPort;
    private final long incarnation;

    public SwimJoinPacket(String nodeId, String host, int clusterPort, long incarnation) {
        this.nodeId = nodeId;
        this.host = host;
        this.clusterPort = clusterPort;
        this.incarnation = incarnation;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getHost() {
        return host;
    }

    public int getClusterPort() {
        return clusterPort;
    }

    public long getIncarnation() {
        return incarnation;
    }

    @Override
    public byte[] getHeader() {
        JSONObject obj = new JSONObject();
        obj.put("name", getClass().getSimpleName());
        return obj.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getBody() {
        JSONObject obj = new JSONObject();
        obj.put("nodeId", nodeId);
        obj.put("host", host);
        obj.put("clusterPort", clusterPort);
        obj.put("incarnation", incarnation);
        return obj.toString().getBytes(StandardCharsets.UTF_8);
    }

    public static SwimJoinPacket fromJson(JSONObject envelope) {
        JSONObject body = envelope.getJSONObject("body");
        return new SwimJoinPacket(
                body.optString("nodeId", ""),
                body.optString("host", ""),
                body.optInt("clusterPort", 0),
                body.optLong("incarnation", 0L));
    }
}
