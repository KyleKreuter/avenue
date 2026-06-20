package de.kyle.avenue.packet.cluster;

import de.kyle.avenue.packet.Packet;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

/**
 * Periodic liveness signal exchanged between cluster peers.
 * <p>
 * Each node sends this packet every {@code cluster.heartbeat-interval} milliseconds on every
 * established peer link. The receiver uses it to reset a watchdog timer. If no heartbeat
 * arrives within {@code cluster.heartbeat-interval * 3} milliseconds the link is closed
 * and a reconnect is scheduled.
 */
public class ClusterHeartbeatPacket implements Packet {

    private final String nodeId;

    public ClusterHeartbeatPacket(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getNodeId() {
        return nodeId;
    }

    @Override
    public byte[] getHeader() {
        JSONObject obj = new JSONObject();
        obj.put("name", getClass().getSimpleName());
        obj.put("nodeId", nodeId);
        return obj.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getBody() {
        return "{}".getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Parses a received JSON envelope into a {@link ClusterHeartbeatPacket}.
     */
    public static ClusterHeartbeatPacket fromJson(JSONObject envelope) {
        String nid = envelope.getJSONObject("header").optString("nodeId", "unknown");
        return new ClusterHeartbeatPacket(nid);
    }
}
