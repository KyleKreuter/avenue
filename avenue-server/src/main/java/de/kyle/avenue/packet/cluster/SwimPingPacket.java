package de.kyle.avenue.packet.cluster;

import de.kyle.avenue.cluster.membership.GossipUpdate;
import de.kyle.avenue.packet.Packet;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * SWIM direct probe (Phase E). A node sends this over a peer's authenticated link to confirm the
 * peer is alive. The receiver answers with a {@link SwimAckPacket} carrying the same {@code seqNo}.
 * Membership gossip rides along in {@code piggyback}.
 * <p>
 * Wire shape: {@code header={name}}, {@code body={seqNo, piggyback:[GossipUpdate...]}}.
 */
public class SwimPingPacket implements Packet {

    private final long seqNo;
    private final List<GossipUpdate> piggyback;

    public SwimPingPacket(long seqNo, List<GossipUpdate> piggyback) {
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

    public static SwimPingPacket fromJson(JSONObject envelope) {
        JSONObject body = envelope.getJSONObject("body");
        return new SwimPingPacket(
                body.optLong("seqNo", 0L),
                SwimGossipCodec.fromJsonArray(body.optJSONArray("piggyback")));
    }
}
