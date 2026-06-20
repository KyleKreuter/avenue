package de.kyle.avenue.serialization;

import com.google.protobuf.InvalidProtocolBufferException;
import de.kyle.avenue.proto.ClientEnvelope;
import de.kyle.avenue.proto.ClusterEnvelope;

/**
 * Protobuf wire (de)serialization for the Avenue protocol envelopes.
 * <p>
 * It turns a top-level {@link ClientEnvelope} / {@link ClusterEnvelope} into its bare payload bytes
 * and back. It deals exclusively with the bare payload — the 4-byte length prefix that frames a
 * message on the stream is still added/removed by {@link PacketFraming}.
 * <p>
 * The {@code maxSize} overloads enforce the configured oversized-payload guard so the framing layer
 * and the codec agree on the maximum allowed message size.
 */
public final class WireCodec {

    private WireCodec() {
    }

    // --- Client plane ---------------------------------------------------------------------------

    /**
     * Serializes a client envelope into its bare protobuf payload bytes (no length prefix).
     *
     * @param envelope the envelope to encode
     * @return the encoded payload bytes
     * @throws IllegalArgumentException if {@code envelope} is null
     */
    public static byte[] encodeClient(ClientEnvelope envelope) {
        if (envelope == null) {
            throw new IllegalArgumentException("ClientEnvelope cannot be null");
        }
        return envelope.toByteArray();
    }

    /**
     * Serializes a client envelope, rejecting payloads larger than {@code maxSize}.
     *
     * @param envelope the envelope to encode
     * @param maxSize  the maximum allowed payload size in bytes
     * @return the encoded payload bytes
     * @throws IllegalArgumentException if {@code envelope} is null or the payload exceeds {@code maxSize}
     */
    public static byte[] encodeClient(ClientEnvelope envelope, int maxSize) {
        byte[] payload = encodeClient(envelope);
        checkSize(payload.length, maxSize);
        return payload;
    }

    /**
     * Parses a client envelope from its bare protobuf payload bytes (length prefix already consumed).
     *
     * @param payload the payload bytes to decode
     * @return the parsed envelope
     * @throws IllegalArgumentException if {@code payload} is null or not a valid client envelope
     */
    public static ClientEnvelope decodeClient(byte[] payload) {
        if (payload == null) {
            throw new IllegalArgumentException("Payload cannot be null");
        }
        try {
            return ClientEnvelope.parseFrom(payload);
        } catch (InvalidProtocolBufferException e) {
            // Mirror the JSON path: a malformed payload is a desynced or hostile peer; surface it as
            // an IllegalArgumentException so the caller drops the connection.
            throw new IllegalArgumentException("Malformed ClientEnvelope payload", e);
        }
    }

    /**
     * Parses a client envelope, rejecting payloads larger than {@code maxSize}.
     *
     * @param payload the payload bytes to decode
     * @param maxSize the maximum allowed payload size in bytes
     * @return the parsed envelope
     * @throws IllegalArgumentException if {@code payload} is null, too large, or malformed
     */
    public static ClientEnvelope decodeClient(byte[] payload, int maxSize) {
        if (payload == null) {
            throw new IllegalArgumentException("Payload cannot be null");
        }
        checkSize(payload.length, maxSize);
        return decodeClient(payload);
    }

    // --- Cluster plane --------------------------------------------------------------------------

    /**
     * Serializes a cluster envelope into its bare protobuf payload bytes (no length prefix).
     *
     * @param envelope the envelope to encode
     * @return the encoded payload bytes
     * @throws IllegalArgumentException if {@code envelope} is null
     */
    public static byte[] encodeCluster(ClusterEnvelope envelope) {
        if (envelope == null) {
            throw new IllegalArgumentException("ClusterEnvelope cannot be null");
        }
        return envelope.toByteArray();
    }

    /**
     * Serializes a cluster envelope, rejecting payloads larger than {@code maxSize}.
     *
     * @param envelope the envelope to encode
     * @param maxSize  the maximum allowed payload size in bytes
     * @return the encoded payload bytes
     * @throws IllegalArgumentException if {@code envelope} is null or the payload exceeds {@code maxSize}
     */
    public static byte[] encodeCluster(ClusterEnvelope envelope, int maxSize) {
        byte[] payload = encodeCluster(envelope);
        checkSize(payload.length, maxSize);
        return payload;
    }

    /**
     * Parses a cluster envelope from its bare protobuf payload bytes (length prefix already consumed).
     *
     * @param payload the payload bytes to decode
     * @return the parsed envelope
     * @throws IllegalArgumentException if {@code payload} is null or not a valid cluster envelope
     */
    public static ClusterEnvelope decodeCluster(byte[] payload) {
        if (payload == null) {
            throw new IllegalArgumentException("Payload cannot be null");
        }
        try {
            return ClusterEnvelope.parseFrom(payload);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Malformed ClusterEnvelope payload", e);
        }
    }

    /**
     * Parses a cluster envelope, rejecting payloads larger than {@code maxSize}.
     *
     * @param payload the payload bytes to decode
     * @param maxSize the maximum allowed payload size in bytes
     * @return the parsed envelope
     * @throws IllegalArgumentException if {@code payload} is null, too large, or malformed
     */
    public static ClusterEnvelope decodeCluster(byte[] payload, int maxSize) {
        if (payload == null) {
            throw new IllegalArgumentException("Payload cannot be null");
        }
        checkSize(payload.length, maxSize);
        return decodeCluster(payload);
    }

    // --- Helpers --------------------------------------------------------------------------------

    private static void checkSize(int length, int maxSize) {
        if (length > maxSize) {
            throw new IllegalArgumentException(
                    String.format("Payload is too large (%d bytes), exceeding maximum size of %d bytes",
                            length, maxSize));
        }
    }
}
