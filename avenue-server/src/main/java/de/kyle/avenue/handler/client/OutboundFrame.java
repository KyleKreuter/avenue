package de.kyle.avenue.handler.client;

import de.kyle.avenue.proto.ClientEnvelope;

/**
 * Sum type carried by the per-client outbound queue ({@link ClientConnectionHandler}).
 * <p>
 * Two shapes flow through the same queue so the FIFO ordering between fan-out and request/response
 * traffic is preserved, while serialization happens at most once per logical message:
 * <ul>
 *   <li>{@link PreSerialized} — fully-serialized {@link ClientEnvelope} payload bytes (no length
 *       prefix). The publish fan-out uses this so a {@code PublishOutbound} envelope is serialized
 *       to protobuf bytes <em>exactly once</em> in
 *       {@link de.kyle.avenue.handler.subscription.TopicSubscriptionHandler#deliverPacketToSubscribers}
 *       and the immutable byte array is shared across every subscriber's writer — O(1) instead of
 *       O(N) serialization for an N-way fan-out. This is the client-plane analogue of
 *       {@link de.kyle.avenue.cluster.OutboundItem.PreSerializedFrame} on the cluster plane.</li>
 *   <li>{@link Envelope} — a typed {@link ClientEnvelope} the writer serializes lazily right before
 *       it is written. The low-rate request/response answers (auth-token response, subscribe-ack)
 *       use this via {@link ClientConnectionHandler#enqueue(ClientEnvelope)}: per-frame
 *       serialization cost is irrelevant there and keeping the envelope avoids pre-serializing on
 *       the handler thread.</li>
 * </ul>
 * The bare-payload bytes of a {@link PreSerialized} are never mutated after construction, so a
 * single instance is safe to share across all subscriber connections and their writer threads.
 */
public sealed interface OutboundFrame permits OutboundFrame.PreSerialized, OutboundFrame.Envelope {

    /**
     * A ready-to-write client envelope as immutable protobuf bytes (without the 4-byte length
     * prefix, which {@link de.kyle.avenue.serialization.PacketFraming} adds).
     *
     * @param payload the serialized {@link ClientEnvelope} payload bytes
     */
    record PreSerialized(byte[] payload) implements OutboundFrame {
    }

    /**
     * A typed envelope the writer serializes lazily right before writing it.
     *
     * @param envelope the envelope to serialize and send
     */
    record Envelope(ClientEnvelope envelope) implements OutboundFrame {
    }
}
