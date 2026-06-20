package de.kyle.avenue.packet.cluster;

import de.kyle.avenue.packet.Packet;
import org.json.JSONArray;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Full interest-state snapshot (Phase D anti-entropy): carries the complete set of topics this node
 * is currently interested in, so a peer can {@link de.kyle.avenue.cluster.InterestRoutingTable#replaceFullState
 * replace} its view of this node's interest in one shot.
 * <p>
 * Sent in two situations:
 * <ul>
 *   <li>Immediately when a new peer link comes up (initial sync), so the new peer routes correctly
 *       from the very first publish — analogous to the Phase C resume request.</li>
 *   <li>Periodically every {@code cluster.interest.sync-interval-ms} to every peer, healing any
 *       interest delta that was lost in transit (deltas are best-effort).</li>
 * </ul>
 * The {@code interestVersion} is the same per-node monotonic counter the {@link ClusterInterestPacket}
 * deltas use; the receiver only applies the snapshot if its version is newer than what it last saw,
 * so a stale sync can never regress fresher delta state.
 * <p>
 * Wire shape: {@code header={name, nodeId, interestVersion}}, {@code body={topics:[...]}}.
 */
public class ClusterInterestSyncPacket implements Packet {

    private final String nodeId;
    private final long interestVersion;
    private final Set<String> topics;

    public ClusterInterestSyncPacket(String nodeId, long interestVersion, Set<String> topics) {
        this.nodeId = nodeId;
        this.interestVersion = interestVersion;
        this.topics = topics != null ? new LinkedHashSet<>(topics) : Set.of();
    }

    public String getNodeId() {
        return nodeId;
    }

    public long getInterestVersion() {
        return interestVersion;
    }

    public Set<String> getTopics() {
        return topics;
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
        JSONArray arr = new JSONArray();
        for (String t : topics) {
            arr.put(t);
        }
        JSONObject obj = new JSONObject();
        obj.put("topics", arr);
        return obj.toString().getBytes(StandardCharsets.UTF_8);
    }

    /** Parses a received JSON envelope into a {@link ClusterInterestSyncPacket}. */
    public static ClusterInterestSyncPacket fromJson(JSONObject envelope) {
        JSONObject header = envelope.getJSONObject("header");
        JSONObject body = envelope.getJSONObject("body");
        Set<String> topics = new LinkedHashSet<>();
        JSONArray arr = body.optJSONArray("topics");
        if (arr != null) {
            for (int i = 0; i < arr.length(); i++) {
                topics.add(arr.getString(i));
            }
        }
        return new ClusterInterestSyncPacket(
                header.optString("nodeId", "unknown"),
                header.getLong("interestVersion"),
                topics);
    }
}
