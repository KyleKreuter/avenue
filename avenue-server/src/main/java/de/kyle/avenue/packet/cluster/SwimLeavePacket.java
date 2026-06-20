package de.kyle.avenue.packet.cluster;

import de.kyle.avenue.packet.Packet;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

/**
 * SWIM graceful-leave announcement (Phase E). A node sends this to its peers from {@code stop()} so
 * the cluster marks it LEFT immediately (converging far faster than the suspicion timeout). The
 * {@code incarnation} is the leaving node's bumped incarnation, ensuring the LEFT fact wins over any
 * in-flight ALIVE gossip.
 */
public class SwimLeavePacket implements Packet {

    private final String nodeId;
    private final long incarnation;

    public SwimLeavePacket(String nodeId, long incarnation) {
        this.nodeId = nodeId;
        this.incarnation = incarnation;
    }

    public String getNodeId() {
        return nodeId;
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
        obj.put("incarnation", incarnation);
        return obj.toString().getBytes(StandardCharsets.UTF_8);
    }

    public static SwimLeavePacket fromJson(JSONObject envelope) {
        JSONObject body = envelope.getJSONObject("body");
        return new SwimLeavePacket(
                body.optString("nodeId", ""),
                body.optLong("incarnation", 0L));
    }
}
