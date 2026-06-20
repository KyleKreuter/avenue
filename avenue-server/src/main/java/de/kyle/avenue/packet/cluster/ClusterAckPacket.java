package de.kyle.avenue.packet.cluster;

import de.kyle.avenue.packet.Packet;
import org.json.JSONArray;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Cumulative acknowledgment sent periodically from the message-receiving side of a cluster link
 * back to the sender, driving Phase C's at-least-once delivery.
 * <p>
 * Each {@link Ack} reports, per origin {@code (originNodeId, originEpoch)}, the highest
 * <em>contiguous</em> sequence number the receiver has delivered. The sender uses it to evict
 * everything up to and including {@code ackedSeq} from that origin's {@link de.kyle.avenue.cluster.ReplayBuffer}.
 * Being cumulative, a single ACK subsumes all earlier ones, so a lost ACK is harmless: the next one
 * carries the latest high-water mark.
 * <p>
 * In the single-hop full-mesh topology a given outbound link carries exactly one origin (the local
 * node), but the list is kept generic so the same packet works once multi-origin links exist.
 */
public class ClusterAckPacket implements Packet {

    /**
     * One per-origin cumulative acknowledgment.
     *
     * @param originNodeId the origin whose sequence space this ack refers to
     * @param originEpoch  the origin's process incarnation epoch
     * @param ackedSeq     the highest contiguous seq the receiver has delivered for this origin
     */
    public record Ack(String originNodeId, long originEpoch, long ackedSeq) {
    }

    private final String ackerNodeId;
    private final List<Ack> acks;

    public ClusterAckPacket(String ackerNodeId, List<Ack> acks) {
        this.ackerNodeId = ackerNodeId;
        this.acks = acks != null ? List.copyOf(acks) : List.of();
    }

    public String getAckerNodeId() {
        return ackerNodeId;
    }

    public List<Ack> getAcks() {
        return acks;
    }

    @Override
    public byte[] getHeader() {
        JSONObject obj = new JSONObject();
        obj.put("name", getClass().getSimpleName());
        obj.put("ackerNodeId", ackerNodeId);
        return obj.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getBody() {
        JSONArray arr = new JSONArray();
        for (Ack ack : acks) {
            JSONObject a = new JSONObject();
            a.put("originNodeId", ack.originNodeId());
            a.put("originEpoch", ack.originEpoch());
            a.put("ackedSeq", ack.ackedSeq());
            arr.put(a);
        }
        JSONObject obj = new JSONObject();
        obj.put("acks", arr);
        return obj.toString().getBytes(StandardCharsets.UTF_8);
    }

    /** Parses a received JSON envelope into a {@link ClusterAckPacket}. */
    public static ClusterAckPacket fromJson(JSONObject envelope) {
        JSONObject header = envelope.getJSONObject("header");
        JSONObject body = envelope.getJSONObject("body");
        String ackerNodeId = header.optString("ackerNodeId", "unknown");
        List<Ack> acks = new ArrayList<>();
        JSONArray arr = body.optJSONArray("acks");
        if (arr != null) {
            for (int i = 0; i < arr.length(); i++) {
                JSONObject a = arr.getJSONObject(i);
                acks.add(new Ack(
                        a.getString("originNodeId"),
                        a.getLong("originEpoch"),
                        a.getLong("ackedSeq")));
            }
        }
        return new ClusterAckPacket(ackerNodeId, acks);
    }
}
