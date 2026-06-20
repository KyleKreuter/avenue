package de.kyle.avenue.serialization;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

/**
 * Length-prefix framing for the Avenue wire protocol.
 * <p>
 * This is the single, clearly-named place where the 4-byte big-endian length prefix
 * lives. Each message on the wire is laid out as:
 * <pre>
 *   [4 bytes: big-endian int32 payload length][payload bytes (Protobuf envelope)]
 * </pre>
 * The payload is a serialized {@code ClientEnvelope} / {@code ClusterEnvelope} Protobuf message
 * produced by {@link WireCodec} (see {@code docs/protocol.md}).
 * The {@code serialize()}/{@code deserialize()} methods deal exclusively with the bare
 * payload; framing is added/removed here so that multiple messages can be read from a
 * single connection in a loop.
 */
public final class PacketFraming {

    private PacketFraming() {
    }

    /**
     * Writes a single framed message: the 4-byte length prefix followed by the payload.
     *
     * @param out     the target stream
     * @param payload the bare payload bytes (already serialized, no prefix)
     * @throws IOException if writing fails
     */
    public static void writeFrame(DataOutputStream out, byte[] payload) throws IOException {
        out.writeInt(payload.length);
        out.write(payload);
        out.flush();
    }

    /**
     * Reads a single framed message: the 4-byte length prefix followed by exactly that
     * many payload bytes.
     *
     * @param in            the source stream
     * @param maxPacketSize the configured maximum allowed payload size
     * @return the payload bytes of the next message
     * @throws EOFException             if the stream ends before a full frame is read
     * @throws IOException              if reading fails
     * @throws IllegalArgumentException if the announced length is negative or exceeds the maximum
     */
    public static byte[] readFrame(DataInputStream in, int maxPacketSize) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > maxPacketSize) {
            // Drop the connection: a negative or oversized length means a desynced or
            // hostile peer. We cannot safely resync the stream, so the caller must close it.
            throw new IllegalArgumentException(
                    String.format("Invalid frame length %d (max allowed %d)", length, maxPacketSize));
        }
        byte[] payload = new byte[length];
        in.readFully(payload);
        return payload;
    }
}
