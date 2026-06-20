package de.kyle.avenue.packet.cluster;

import de.kyle.avenue.cluster.membership.GossipUpdate;
import de.kyle.avenue.packet.Packet;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * SWIM indirect-probe request (Phase E). When a direct {@link SwimPingPacket} to a target times out,
 * the prober asks {@code k} other alive members to probe the target on its behalf. The helper pings
 * {@code targetNodeId} and, on success, returns a {@link SwimPingReqAckPacket} echoing {@code seqNo}.
 */
public class SwimPingReqPacket implements Packet {

    private final String targetNodeId;
    private final long seqNo;
    private final List<GossipUpdate> piggyback;

    public SwimPingReqPacket(String targetNodeId, long seqNo, List<GossipUpdate> piggyback) {
        this.targetNodeId = targetNodeId;
        this.seqNo = seqNo;
        this.piggyback = piggyback != null ? piggyback : List.of();
    }

    public String getTargetNodeId() {
        return targetNodeId;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public List<GossipUpdate> getPiggyback() {
        return piggyback;
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
        obj.put("targetNodeId", targetNodeId);
        obj.put("seqNo", seqNo);
        obj.put("piggyback", SwimGossipCodec.toJsonArray(piggyback));
        return obj.toString().getBytes(StandardCharsets.UTF_8);
    }

    public static SwimPingReqPacket fromJson(JSONObject envelope) {
        JSONObject body = envelope.getJSONObject("body");
        return new SwimPingReqPacket(
                body.optString("targetNodeId", ""),
                body.optLong("seqNo", 0L),
                SwimGossipCodec.fromJsonArray(body.optJSONArray("piggyback")));
    }
}
