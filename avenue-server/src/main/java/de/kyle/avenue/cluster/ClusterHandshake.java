package de.kyle.avenue.cluster;

import de.kyle.avenue.packet.cluster.ClusterHelloPacket;
import de.kyle.avenue.serialization.PacketFraming;
import org.json.JSONObject;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * Performs the two-sided cluster link handshake.
 * <p>
 * Protocol:
 * <pre>
 *   Initiator ---[ClusterHelloPacket]---> Acceptor
 *   Initiator <--[ClusterHelloPacket]--- Acceptor
 * </pre>
 * Both sides authenticate each other via a shared {@code cluster.secret} carried in the
 * hello. The connection is closed immediately on mismatch.
 */
final class ClusterHandshake {

    private ClusterHandshake() {
    }

    /**
     * Initiator-side handshake: send our hello, then read the peer's hello.
     *
     * @return the remote node's {@code nodeId}
     * @throws IOException       on I/O failure
     * @throws SecurityException if the peer secret does not match
     */
    static String initiatorHandshake(Socket socket, String localNodeId, String secret, int maxPacketSize)
            throws IOException {
        DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
        DataInputStream din = new DataInputStream(socket.getInputStream());

        sendHello(dout, localNodeId, secret);
        return readAndVerifyHello(din, secret, maxPacketSize);
    }

    /**
     * Acceptor-side handshake: read the peer's hello, then send ours.
     *
     * @return the remote node's {@code nodeId}
     * @throws IOException       on I/O failure
     * @throws SecurityException if the peer secret does not match
     */
    static String acceptorHandshake(Socket socket, String localNodeId, String secret, int maxPacketSize)
            throws IOException {
        DataInputStream din = new DataInputStream(socket.getInputStream());
        DataOutputStream dout = new DataOutputStream(socket.getOutputStream());

        String remoteNodeId = readAndVerifyHello(din, secret, maxPacketSize);
        sendHello(dout, localNodeId, secret);
        return remoteNodeId;
    }

    private static void sendHello(DataOutputStream dout, String nodeId, String secret) throws IOException {
        ClusterHelloPacket hello = new ClusterHelloPacket(nodeId, secret);
        JSONObject envelope = new JSONObject();
        envelope.put("header", new JSONObject(new String(hello.getHeader(), StandardCharsets.UTF_8)));
        envelope.put("body", new JSONObject(new String(hello.getBody(), StandardCharsets.UTF_8)));
        PacketFraming.writeFrame(dout, envelope.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static String readAndVerifyHello(DataInputStream din, String expectedSecret, int maxPacketSize)
            throws IOException {
        byte[] frame = PacketFraming.readFrame(din, maxPacketSize);
        JSONObject envelope = new JSONObject(new String(frame, StandardCharsets.UTF_8));
        ClusterHelloPacket hello = ClusterHelloPacket.fromJson(envelope);
        if (!MessageDigest.isEqual(
                expectedSecret.getBytes(StandardCharsets.UTF_8),
                hello.getSecret().getBytes(StandardCharsets.UTF_8))) {
            throw new SecurityException("Cluster peer presented wrong secret");
        }
        return hello.getNodeId();
    }
}
