package de.kyle.avenue.packet.cluster;

import de.kyle.avenue.cluster.membership.GossipUpdate;
import de.kyle.avenue.packet.Packet;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * SWIM probe acknowledgement (Phase E). Sent in reply to a {@link SwimPingPacket}, echoing its
 * {@code seqNo} so the prober can complete the matching pending probe. Carries gossip piggyback.
 */
public class SwimAckPacket implements Packet {

    private final long seqNo;
    private final List<GossipUpdate> piggyback;

    public SwimAckPacket(long seqNo, List<GossipUpdate> piggyback) {
        this.seqNo = seqNo;
        this.piggyback = piggyback != null ? piggyback : List.of();
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
        obj.put("seqNo", seqNo);
        obj.put("piggyback", SwimGossipCodec.toJsonArray(piggyback));
        return obj.toString().getBytes(StandardCharsets.UTF_8);
    }

    public static SwimAckPacket fromJson(JSONObject envelope) {
        JSONObject body = envelope.getJSONObject("body");
        return new SwimAckPacket(
                body.optLong("seqNo", 0L),
                SwimGossipCodec.fromJsonArray(body.optJSONArray("piggyback")));
    }
}
