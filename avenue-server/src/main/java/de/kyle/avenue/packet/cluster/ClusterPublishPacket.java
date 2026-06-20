package de.kyle.avenue.packet.cluster;

import de.kyle.avenue.packet.Packet;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

/**
 * Carries a published message across a cluster link.
 * <p>
 * Fields:
 * <ul>
 *   <li>{@code topic}        – normalized topic key</li>
 *   <li>{@code source}       – original client-supplied source identifier</li>
 *   <li>{@code originNodeId} – ID of the node that first received the local publish</li>
 *   <li>{@code originEpoch}  – the origin's process incarnation epoch (ms); together with
 *       {@code originNodeId} it scopes the sequence space, so a restarted origin never collides
 *       with its previous run</li>
 *   <li>{@code seq}          – strictly increasing per-origin sequence number; the
 *       {@code (originNodeId, originEpoch, seq)} triple replaces the old random {@code messageId}
 *       and is used for ordered deduplication</li>
 *   <li>{@code data}         – payload (body)</li>
 * </ul>
 * Receiving nodes deliver the message locally and never re-forward it (single-hop rule in a
 * full-mesh topology).
 */
public class ClusterPublishPacket implements Packet {

    private final String topic;
    private final String source;
    private final String originNodeId;
    private final long originEpoch;
    private final long seq;
    private final String data;

    public ClusterPublishPacket(
            String topic,
            String source,
            String originNodeId,
            long originEpoch,
            long seq,
            String data
    ) {
        this.topic = topic;
        this.source = source;
        this.originNodeId = originNodeId;
        this.originEpoch = originEpoch;
        this.seq = seq;
        this.data = data;
    }

    public String getTopic() {
        return topic;
    }

    public String getSource() {
        return source;
    }

    public String getOriginNodeId() {
        return originNodeId;
    }

    public long getOriginEpoch() {
        return originEpoch;
    }

    public long getSeq() {
        return seq;
    }

    public String getData() {
        return data;
    }

    @Override
    public byte[] getHeader() {
        JSONObject obj = new JSONObject();
        obj.put("name", getClass().getSimpleName());
        obj.put("topic", topic);
        obj.put("source", source);
        obj.put("originNodeId", originNodeId);
        obj.put("originEpoch", originEpoch);
        obj.put("seq", seq);
        return obj.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getBody() {
        JSONObject obj = new JSONObject();
        obj.put("data", data);
        return obj.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Parses a received JSON envelope into a {@link ClusterPublishPacket}.
     *
     * @param envelope the full {@code {"header":{...},"body":{...}}} JSON object
     * @return parsed packet
     */
    public static ClusterPublishPacket fromJson(JSONObject envelope) {
        JSONObject header = envelope.getJSONObject("header");
        JSONObject body = envelope.getJSONObject("body");
        return new ClusterPublishPacket(
                header.getString("topic"),
                header.getString("source"),
                header.getString("originNodeId"),
                header.getLong("originEpoch"),
                header.getLong("seq"),
                body.getString("data")
        );
    }
}
