package de.kyle.avenue.cluster;

import de.kyle.avenue.proto.ClusterEnvelope;

/**
 * Sum type for the per-peer outbound writer queue in {@link PeerLink}.
 * <p>
 * Two shapes flow through the same queue so ordering between data and control traffic is
 * preserved:
 * <ul>
 *   <li>{@link PreSerializedFrame} – fully-serialized {@link ClusterEnvelope} payload bytes (no
 *       length prefix). Publish fan-out uses this so a publish frame is serialized to protobuf bytes
 *       exactly once in {@link ClusterManager#forward} (per target, because each target carries its
 *       own {@code linkSeq}) and the immutable byte array is then shared across the writer / replay
 *       buffer without re-serialization.</li>
 *   <li>{@link ControlFrame} – a control {@link ClusterEnvelope} (heartbeat / ack / resume / gap /
 *       interest / SWIM) that the writer serializes lazily right before it is written. Control
 *       traffic is low-rate, so the per-frame serialization cost is irrelevant and keeping the
 *       envelope avoids pre-serializing on the sender threads.</li>
 * </ul>
 */
public sealed interface OutboundItem permits OutboundItem.PreSerializedFrame, OutboundItem.ControlFrame {

    /**
     * A ready-to-write cluster envelope as immutable protobuf bytes (without the 4-byte length
     * prefix, which {@link de.kyle.avenue.serialization.PacketFraming#writeFrame} adds). Bytes are
     * never mutated after construction, so a single instance is safe to share across threads and
     * peer links.
     *
     * @param payload the serialized {@link ClusterEnvelope} payload bytes
     */
    record PreSerializedFrame(byte[] payload) implements OutboundItem {
    }

    /**
     * A control envelope serialized lazily by the writer.
     *
     * @param envelope the control envelope to serialize and send
     */
    record ControlFrame(ClusterEnvelope envelope) implements OutboundItem {
    }
}
