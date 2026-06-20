package de.kyle.avenue.packet.cluster;

import de.kyle.avenue.packet.Packet;
import org.json.JSONArray;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Sent by the message-<em>receiving</em> side immediately after a cluster link comes up, telling the
 * sender where to resume so it can backfill anything missed while the link was down.
 * <p>
 * Each {@link Resume} reports, per origin {@code (originNodeId, originEpoch)}, the last contiguous
 * sequence number the receiver has delivered. The sender feeds each entry into the matching
 * {@link de.kyle.avenue.cluster.ReplayBuffer#replayFrom(long)}: if the range is still buffered it
 * re-sends every entry beyond {@code lastContiguousSeq} (Backfill); if it has been evicted it answers
 * with a {@link ClusterGapPacket}. On a first connect with no prior state the list is empty (or each
 * {@code lastContiguousSeq} is {@code 0}).
 */
public class ClusterResumePacket implements Packet {

    /**
     * One per-origin resume point.
     *
     * @param originNodeId      the origin whose sequence space to resume
     * @param originEpoch       the origin's process incarnation epoch
     * @param lastContiguousSeq the last contiguous seq the receiver has delivered ({@code 0} = none)
     */
    public record Resume(String originNodeId, long originEpoch, long lastContiguousSeq) {
    }

    private final String nodeId;
    private final List<Resume> resumes;

    public ClusterResumePacket(String nodeId, List<Resume> resumes) {
        this.nodeId = nodeId;
        this.resumes = resumes != null ? List.copyOf(resumes) : List.of();
    }

    public String getNodeId() {
        return nodeId;
    }

    public List<Resume> getResumes() {
        return resumes;
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
        JSONArray arr = new JSONArray();
        for (Resume r : resumes) {
            JSONObject o = new JSONObject();
            o.put("originNodeId", r.originNodeId());
            o.put("originEpoch", r.originEpoch());
            o.put("lastContiguousSeq", r.lastContiguousSeq());
            arr.put(o);
        }
        JSONObject obj = new JSONObject();
        obj.put("resume", arr);
        return obj.toString().getBytes(StandardCharsets.UTF_8);
    }

    /** Parses a received JSON envelope into a {@link ClusterResumePacket}. */
    public static ClusterResumePacket fromJson(JSONObject envelope) {
        JSONObject header = envelope.getJSONObject("header");
        JSONObject body = envelope.getJSONObject("body");
        String nodeId = header.optString("nodeId", "unknown");
        List<Resume> resumes = new ArrayList<>();
        JSONArray arr = body.optJSONArray("resume");
        if (arr != null) {
            for (int i = 0; i < arr.length(); i++) {
                JSONObject o = arr.getJSONObject(i);
                resumes.add(new Resume(
                        o.getString("originNodeId"),
                        o.getLong("originEpoch"),
                        o.getLong("lastContiguousSeq")));
            }
        }
        return new ClusterResumePacket(nodeId, resumes);
    }
}
