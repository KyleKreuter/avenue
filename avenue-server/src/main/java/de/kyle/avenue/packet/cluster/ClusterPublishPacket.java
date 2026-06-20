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
 *       and is the <b>deduplication</b> identity (a node never delivers the same origin seq twice)</li>
 *   <li>{@code linkSeq}      – Phase D: the strictly-monotonic <b>per-link reliability sequence</b>
 *       assigned by the sender for the specific target link this frame travels on. With
 *       interest-based routing a given target only receives the publishes of the topics it is
 *       interested in, so the {@code origin seq} stream it sees is sparse (full of holes). The
 *       at-least-once layer (replay buffer / cumulative ACK / resume / gap) therefore keys on this
 *       dense, gap-free {@code linkSeq} instead of the origin seq. Identity/dedup still uses the
 *       {@code (originNodeId, originEpoch, seq)} triple — the two sequence spaces are independent.</li>
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
    private final long linkSeq;
    private final String data;

    public ClusterPublishPacket(
            String topic,
            String source,
            String originNodeId,
            long originEpoch,
            long seq,
            String data
    ) {
        this(topic, source, originNodeId, originEpoch, seq, 0L, data);
    }

    public ClusterPublishPacket(
            String topic,
            String source,
            String originNodeId,
            long originEpoch,
            long seq,
            long linkSeq,
            String data
    ) {
        this.topic = topic;
        this.source = source;
        this.originNodeId = originNodeId;
        this.originEpoch = originEpoch;
        this.seq = seq;
        this.linkSeq = linkSeq;
        this.data = data;
    }

    /**
     * Returns a copy of this packet carrying the given per-link reliability sequence. Used by the
     * sender to stamp the same logical publish with a target-specific {@code linkSeq} just before it
     * is serialized for that one target link.
     */
    public ClusterPublishPacket withLinkSeq(long newLinkSeq) {
        return new ClusterPublishPacket(topic, source, originNodeId, originEpoch, seq, newLinkSeq, data);
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

    /** Per-link reliability sequence (Phase D). {@code 0} on a packet that predates link-seq stamping. */
    public long getLinkSeq() {
        return linkSeq;
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
        obj.put("linkSeq", linkSeq);
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
                header.optLong("linkSeq", 0L),
                body.getString("data")
        );
    }
}
