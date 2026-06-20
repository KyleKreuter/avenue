package de.kyle.avenue.packet.cluster;

import de.kyle.avenue.cluster.membership.GossipUpdate;
import de.kyle.avenue.packet.Packet;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * SWIM indirect-probe acknowledgement (Phase E). A helper that received a {@link SwimPingReqPacket}
 * and successfully reached {@code targetNodeId} sends this back to the original prober, echoing the
 * probe {@code seqNo} so the prober can complete its pending probe as an indirect success.
 */
public class SwimPingReqAckPacket implements Packet {

    private final String targetNodeId;
    private final long seqNo;
    private final List<GossipUpdate> piggyback;

    public SwimPingReqAckPacket(String targetNodeId, long seqNo, List<GossipUpdate> piggyback) {
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

    public static SwimPingReqAckPacket fromJson(JSONObject envelope) {
        JSONObject body = envelope.getJSONObject("body");
        return new SwimPingReqAckPacket(
                body.optString("targetNodeId", ""),
                body.optLong("seqNo", 0L),
                SwimGossipCodec.fromJsonArray(body.optJSONArray("piggyback")));
    }
}
