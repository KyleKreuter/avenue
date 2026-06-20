package de.kyle.avenue.handler.client;

import de.kyle.avenue.proto.ClientEnvelope;

/**
 * Transport abstraction for a single client connection.
 * <p>
 * This interface decouples the protocol / delivery / dispatch layers
 * ({@link de.kyle.avenue.handler.subscription.TopicSubscriptionHandler},
 * {@link de.kyle.avenue.handler.packet.InboundPacketHandler} and the per-case
 * {@link de.kyle.avenue.handler.packet.PacketHandler}s) from the concrete socket transport. Today
 * the only implementation is the blocking {@link ClientConnectionHandler} (a thread-per-connection
 * reader plus a dedicated writer thread); a future NIO event-loop transport can implement the same
 * contract without touching any caller.
 * <p>
 * It declares <em>exactly</em> the methods callers invoke on a connection, and nothing more:
 * <ul>
 *   <li>{@link #send(ClientEnvelope)} — used by the auth-token and subscribe-ack handlers to answer
 *       a request inline (low-rate request/response).</li>
 *   <li>{@link #enqueue(ClientEnvelope)} — the asynchronous, never-socket-blocking enqueue primitive
 *       {@link #send(ClientEnvelope)} delegates to.</li>
 *   <li>{@link #enqueuePreSerialized(byte[])} — the encode-once publish fan-out path: the same
 *       immutable payload bytes are shared across every subscriber.</li>
 * </ul>
 * <p>
 * Identity is intentionally object identity (no {@code equals}/{@code hashCode} override): the
 * subscriber sets in {@link de.kyle.avenue.handler.subscription.TopicSubscriptionHandler} are
 * identity sets keyed on the live connection instance, so subscribe / deliver / unsubscribe all
 * agree on the same physical connection.
 */
public interface ClientConnection {

    /**
     * Enqueues a typed envelope for asynchronous delivery. Never blocks the caller on the socket;
     * the underlying transport serializes and writes it on its own writer path. Used for the
     * low-rate request/response answers (auth-token response, subscribe-ack).
     *
     * @param envelope the envelope to serialize and send
     */
    void enqueue(ClientEnvelope envelope);

    /**
     * Direct-send entry point used by handlers that answer a request inline. Semantically identical
     * to {@link #enqueue(ClientEnvelope)} (asynchronous, non-blocking on the socket).
     *
     * @param envelope the envelope to serialize and send
     */
    void send(ClientEnvelope envelope);

    /**
     * Enqueues an already-serialized client frame for asynchronous delivery (encode-once fan-out).
     * The supplied bytes are the bare protobuf payload (no length prefix) of a {@link ClientEnvelope}
     * and are written verbatim by the transport. The publish fan-out serializes the
     * {@code PublishOutbound} envelope exactly once and hands the same immutable {@code byte[]} to
     * every subscriber, so the bytes must never be mutated after being passed in.
     *
     * @param frame the bare protobuf payload bytes to deliver verbatim
     */
    void enqueuePreSerialized(byte[] frame);
}
