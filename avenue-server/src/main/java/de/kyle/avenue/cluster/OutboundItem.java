package de.kyle.avenue.cluster;

import de.kyle.avenue.packet.Packet;

/**
 * Sum type for the per-peer outbound writer queue in {@link PeerLink}.
 * <p>
 * Two shapes flow through the same queue so ordering between data and control traffic is
 * preserved:
 * <ul>
 *   <li>{@link PreSerializedFrame} – fully-serialized envelope bytes (no length prefix). Publish
 *       fan-out uses this so the {@link ClusterPublishPacket} is serialized exactly once in
 *       {@link ClusterManager#forward} and the immutable byte array is shared across every target
 *       peer link (O(1) instead of O(N) serialization per publish).</li>
 *   <li>{@link ControlFrame} – a control {@link Packet} (currently only the heartbeat) that the
 *       writer serializes lazily right before it is written. Control traffic is low-rate, so the
 *       per-frame serialization cost is irrelevant and keeping the {@code Packet} avoids
 *       pre-serializing on the heartbeat-sender thread.</li>
 * </ul>
 */
public sealed interface OutboundItem permits OutboundItem.PreSerializedFrame, OutboundItem.ControlFrame {

    /**
     * A ready-to-write cluster envelope as immutable bytes (without the 4-byte length prefix,
     * which {@link de.kyle.avenue.serialization.PacketFraming#writeFrame} adds). Bytes are never
     * mutated after construction, so a single instance is safe to share across threads and peer
     * links.
     *
     * @param payload the serialized {@code {"header":{...},"body":{...}}} envelope bytes
     */
    record PreSerializedFrame(byte[] payload) implements OutboundItem {
    }

    /**
     * A control packet (e.g. heartbeat) serialized lazily by the writer.
     *
     * @param packet the control packet to serialize and send
     */
    record ControlFrame(Packet packet) implements OutboundItem {
    }
}
