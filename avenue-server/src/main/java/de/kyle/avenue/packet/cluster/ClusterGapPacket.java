package de.kyle.avenue.packet.cluster;

import de.kyle.avenue.packet.Packet;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

/**
 * Sent by the message-<em>sending</em> side in response to a {@link ClusterResumePacket} (or while
 * backfilling) when the receiver's requested resume point has already been evicted from the
 * {@link de.kyle.avenue.cluster.ReplayBuffer} — i.e. messages were lost (buffer overflow or a long
 * partition) and cannot be replayed.
 * <p>
 * It carries the {@code firstAvailableSeq}: the lowest sequence number the sender can still deliver
 * for the origin. The receiver responds by resetting its
 * {@link de.kyle.avenue.cluster.OriginSequenceTracker} to {@code firstAvailableSeq - 1}, explicitly
 * accepting the data loss in the skipped range, then resumes lossless delivery from there. A gap
 * event is recorded in the metrics on both sides.
 */
public class ClusterGapPacket implements Packet {

    private final String senderNodeId;
    private final String originNodeId;
    private final long originEpoch;
    private final long firstAvailableSeq;

    public ClusterGapPacket(String senderNodeId, String originNodeId, long originEpoch, long firstAvailableSeq) {
        this.senderNodeId = senderNodeId;
        this.originNodeId = originNodeId;
        this.originEpoch = originEpoch;
        this.firstAvailableSeq = firstAvailableSeq;
    }

    public String getSenderNodeId() {
        return senderNodeId;
    }

    public String getOriginNodeId() {
        return originNodeId;
    }

    public long getOriginEpoch() {
        return originEpoch;
    }

    public long getFirstAvailableSeq() {
        return firstAvailableSeq;
    }

    @Override
    public byte[] getHeader() {
        JSONObject obj = new JSONObject();
        obj.put("name", getClass().getSimpleName());
        obj.put("senderNodeId", senderNodeId);
        return obj.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getBody() {
        JSONObject obj = new JSONObject();
        obj.put("originNodeId", originNodeId);
        obj.put("originEpoch", originEpoch);
        obj.put("firstAvailableSeq", firstAvailableSeq);
        return obj.toString().getBytes(StandardCharsets.UTF_8);
    }

    /** Parses a received JSON envelope into a {@link ClusterGapPacket}. */
    public static ClusterGapPacket fromJson(JSONObject envelope) {
        JSONObject header = envelope.getJSONObject("header");
        JSONObject body = envelope.getJSONObject("body");
        return new ClusterGapPacket(
                header.optString("senderNodeId", "unknown"),
                body.getString("originNodeId"),
                body.getLong("originEpoch"),
                body.getLong("firstAvailableSeq"));
    }
}
