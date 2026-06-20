package de.kyle.avenue.packet.cluster;

import de.kyle.avenue.packet.Packet;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

/**
 * Single interest delta (Phase D): tells a peer that this node started (ADD) or stopped (REMOVE)
 * being interested in one topic, i.e. it gained its first / lost its last local subscriber for it.
 * <p>
 * Deltas are best-effort over the normal control path: a lost delta is self-healed by the periodic
 * full-state {@link ClusterInterestSyncPacket} (anti-entropy). The {@code interestVersion} is a
 * per-node monotonic counter (separate from the publish sequence) used by the receiver's
 * {@link de.kyle.avenue.cluster.InterestRoutingTable} to discard stale / out-of-order updates.
 * <p>
 * Wire shape: {@code header={name, nodeId, interestVersion}}, {@code body={op, topic}} with
 * {@code op} one of {@code "ADD"} / {@code "REMOVE"}.
 */
public class ClusterInterestPacket implements Packet {

    private final String nodeId;
    private final long interestVersion;
    private final String op;
    private final String topic;

    public ClusterInterestPacket(String nodeId, long interestVersion, String op, String topic) {
        this.nodeId = nodeId;
        this.interestVersion = interestVersion;
        this.op = op;
        this.topic = topic;
    }

    public String getNodeId() {
        return nodeId;
    }

    public long getInterestVersion() {
        return interestVersion;
    }

    public String getOp() {
        return op;
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public byte[] getHeader() {
        JSONObject obj = new JSONObject();
        obj.put("name", getClass().getSimpleName());
        obj.put("nodeId", nodeId);
        obj.put("interestVersion", interestVersion);
        return obj.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getBody() {
        JSONObject obj = new JSONObject();
        obj.put("op", op);
        obj.put("topic", topic);
        return obj.toString().getBytes(StandardCharsets.UTF_8);
    }

    /** Parses a received JSON envelope into a {@link ClusterInterestPacket}. */
    public static ClusterInterestPacket fromJson(JSONObject envelope) {
        JSONObject header = envelope.getJSONObject("header");
        JSONObject body = envelope.getJSONObject("body");
        return new ClusterInterestPacket(
                header.optString("nodeId", "unknown"),
                header.getLong("interestVersion"),
                body.getString("op"),
                body.getString("topic"));
    }
}
