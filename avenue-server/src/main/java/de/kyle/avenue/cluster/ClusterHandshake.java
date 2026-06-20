package de.kyle.avenue.cluster;

import de.kyle.avenue.metrics.ClusterMetrics;
import de.kyle.avenue.packet.Packet;
import de.kyle.avenue.packet.cluster.ClusterAuthChallengePacket;
import de.kyle.avenue.packet.cluster.ClusterAuthHelloPacket;
import de.kyle.avenue.packet.cluster.ClusterAuthResponsePacket;
import de.kyle.avenue.serialization.PacketFraming;
import org.json.JSONObject;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;

/**
 * Performs the two-sided cluster link handshake using an HMAC challenge-response protocol.
 * <p>
 * The shared {@code cluster.secret} is used purely as an HMAC key and <strong>never</strong>
 * appears on the wire. Each side proves possession of the secret by HMAC-ing a canonical transcript
 * of both node IDs and both nonces (role-tagged to defeat reflection attacks):
 * <pre>
 *   Initiator --[ClusterAuthHelloPacket: nodeId_I, host, port, incarnation, nonce_I]--> Acceptor
 *   Initiator <--[ClusterAuthChallengePacket: nodeId_A, host, port, incarnation, nonce_A, proof_A]-- Acceptor
 *   Initiator --[ClusterAuthResponsePacket: nodeId_I, proof_I]--> Acceptor
 * </pre>
 * The initiator verifies {@code proof_A} (ROLE=ACCEPTOR) before answering; the acceptor verifies
 * {@code proof_I} (ROLE=INITIATOR) before accepting the link. A proof mismatch raises a
 * {@link SecurityException} and increments {@link ClusterMetrics#incrementHandshakeAuthFailures()}.
 * <p>
 * Throughout the handshake the socket read timeout is pinned to {@code handshakeTimeoutMs} so a
 * silent or stalled peer cannot block a virtual thread indefinitely. The timeout is reset to
 * {@code 0} (infinite) on success so {@link PeerLink#readerLoop()}, which manages its own
 * per-read {@code setSoTimeout}, is unaffected.
 */
final class ClusterHandshake {

    /** Default socket read timeout enforced for the duration of the handshake. */
    static final int DEFAULT_HANDSHAKE_TIMEOUT_MS = 5_000;

    private ClusterHandshake() {
    }

    /**
     * Immutable result of a successful handshake. Beyond {@code remoteNodeId} (used today for the
     * peer registry and tie-break) it carries the peer's advertised contact information and process
     * incarnation, which the SWIM membership layer (Phase E) will consume. Callers may ignore the
     * advertised fields for now.
     *
     * @param remoteNodeId      the authenticated remote node ID
     * @param remoteHost        the host the remote node advertised it can be reached on
     * @param remoteClusterPort the cluster port the remote node advertised
     * @param remoteIncarnation the remote node's process incarnation (epoch ms)
     */
    record AuthResult(
            String remoteNodeId,
            String remoteHost,
            int remoteClusterPort,
            long remoteIncarnation
    ) {
    }

    /**
     * Initiator-side handshake: send hello, read+verify challenge, send response.
     *
     * @param socket             the connected (already TLS-wrapped if enabled) socket
     * @param localNodeId        this node's ID
     * @param localHost          this node's advertised host
     * @param localClusterPort   this node's advertised cluster port
     * @param localIncarnation   this node's process incarnation (epoch ms)
     * @param authenticator      HMAC helper keyed by the shared secret
     * @param maxPacketSize      maximum allowed frame size
     * @param handshakeTimeoutMs socket read timeout enforced during the handshake
     * @param metrics            cluster metrics; the auth-failure counter is bumped on mismatch
     * @return the authenticated {@link AuthResult}
     * @throws IOException       on I/O failure or handshake timeout
     * @throws SecurityException if the acceptor's proof does not verify
     */
    static AuthResult initiatorHandshake(
            Socket socket,
            String localNodeId,
            String localHost,
            int localClusterPort,
            long localIncarnation,
            HmacAuthenticator authenticator,
            int maxPacketSize,
            int handshakeTimeoutMs,
            ClusterMetrics metrics
    ) throws IOException {
        DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
        DataInputStream din = new DataInputStream(socket.getInputStream());

        int previousSoTimeout = socket.getSoTimeout();
        socket.setSoTimeout(handshakeTimeoutMs);
        try {
            // 1) Send our hello with a fresh nonce.
            String initiatorNonce = authenticator.newNonce();
            sendFrame(dout, new ClusterAuthHelloPacket(
                    localNodeId, localHost, localClusterPort, localIncarnation,
                    initiatorNonce, HmacAuthenticator.PROTOCOL));

            // 2) Read the acceptor's challenge and verify its proof (ROLE=ACCEPTOR).
            ClusterAuthChallengePacket challenge =
                    ClusterAuthChallengePacket.fromJson(readEnvelope(din, maxPacketSize));
            String acceptorNodeId = challenge.getNodeId();
            String acceptorNonce = challenge.getNonce();

            String expectedAcceptorProof = authenticator.computeProof(
                    localNodeId, acceptorNodeId, initiatorNonce, acceptorNonce,
                    HmacAuthenticator.ROLE_ACCEPTOR);
            if (!authenticator.proofsMatch(expectedAcceptorProof, challenge.getProof())) {
                metrics.incrementHandshakeAuthFailures();
                throw new SecurityException("Cluster peer presented an invalid acceptor proof");
            }

            // 3) Send our own proof (ROLE=INITIATOR) to complete mutual auth.
            String initiatorProof = authenticator.computeProof(
                    localNodeId, acceptorNodeId, initiatorNonce, acceptorNonce,
                    HmacAuthenticator.ROLE_INITIATOR);
            sendFrame(dout, new ClusterAuthResponsePacket(localNodeId, initiatorProof));

            return new AuthResult(
                    acceptorNodeId,
                    challenge.getHost(),
                    challenge.getClusterPort(),
                    challenge.getIncarnation());
        } catch (SocketTimeoutException e) {
            throw new IOException("Handshake timed out waiting for cluster peer", e);
        } finally {
            // Restore so PeerLink's own per-read timeout management governs the live link.
            restoreSoTimeout(socket, previousSoTimeout);
        }
    }

    /**
     * Acceptor-side handshake: read hello, send challenge, read+verify response.
     *
     * @param socket             the accepted (already TLS-wrapped if enabled) socket
     * @param localNodeId        this node's ID
     * @param localHost          this node's advertised host
     * @param localClusterPort   this node's advertised cluster port
     * @param localIncarnation   this node's process incarnation (epoch ms)
     * @param authenticator      HMAC helper keyed by the shared secret
     * @param maxPacketSize      maximum allowed frame size
     * @param handshakeTimeoutMs socket read timeout enforced during the handshake
     * @param metrics            cluster metrics; the auth-failure counter is bumped on mismatch
     * @return the authenticated {@link AuthResult}
     * @throws IOException       on I/O failure or handshake timeout
     * @throws SecurityException if the initiator's proof does not verify
     */
    static AuthResult acceptorHandshake(
            Socket socket,
            String localNodeId,
            String localHost,
            int localClusterPort,
            long localIncarnation,
            HmacAuthenticator authenticator,
            int maxPacketSize,
            int handshakeTimeoutMs,
            ClusterMetrics metrics
    ) throws IOException {
        DataInputStream din = new DataInputStream(socket.getInputStream());
        DataOutputStream dout = new DataOutputStream(socket.getOutputStream());

        int previousSoTimeout = socket.getSoTimeout();
        socket.setSoTimeout(handshakeTimeoutMs);
        try {
            // 1) Read the initiator's hello.
            ClusterAuthHelloPacket hello =
                    ClusterAuthHelloPacket.fromJson(readEnvelope(din, maxPacketSize));
            String initiatorNodeId = hello.getNodeId();
            String initiatorNonce = hello.getNonce();

            // 2) Build our challenge: fresh nonce + our proof (ROLE=ACCEPTOR).
            String acceptorNonce = authenticator.newNonce();
            String acceptorProof = authenticator.computeProof(
                    initiatorNodeId, localNodeId, initiatorNonce, acceptorNonce,
                    HmacAuthenticator.ROLE_ACCEPTOR);
            sendFrame(dout, new ClusterAuthChallengePacket(
                    localNodeId, localHost, localClusterPort, localIncarnation,
                    acceptorNonce, acceptorProof));

            // 3) Read the initiator's response and verify its proof (ROLE=INITIATOR).
            ClusterAuthResponsePacket response =
                    ClusterAuthResponsePacket.fromJson(readEnvelope(din, maxPacketSize));
            String expectedInitiatorProof = authenticator.computeProof(
                    initiatorNodeId, localNodeId, initiatorNonce, acceptorNonce,
                    HmacAuthenticator.ROLE_INITIATOR);
            if (!authenticator.proofsMatch(expectedInitiatorProof, response.getProof())) {
                metrics.incrementHandshakeAuthFailures();
                throw new SecurityException("Cluster peer presented an invalid initiator proof");
            }

            return new AuthResult(
                    initiatorNodeId,
                    hello.getHost(),
                    hello.getClusterPort(),
                    hello.getIncarnation());
        } catch (SocketTimeoutException e) {
            throw new IOException("Handshake timed out waiting for cluster peer", e);
        } finally {
            restoreSoTimeout(socket, previousSoTimeout);
        }
    }

    // -------------------------------------------------------------------------
    // Wire helpers
    // -------------------------------------------------------------------------

    private static void sendFrame(DataOutputStream dout, Packet packet) throws IOException {
        JSONObject envelope = new JSONObject();
        envelope.put("header", new JSONObject(new String(packet.getHeader(), StandardCharsets.UTF_8)));
        envelope.put("body", new JSONObject(new String(packet.getBody(), StandardCharsets.UTF_8)));
        PacketFraming.writeFrame(dout, envelope.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static JSONObject readEnvelope(DataInputStream din, int maxPacketSize) throws IOException {
        byte[] frame = PacketFraming.readFrame(din, maxPacketSize);
        return new JSONObject(new String(frame, StandardCharsets.UTF_8));
    }

    private static void restoreSoTimeout(Socket socket, int soTimeoutMs) {
        try {
            socket.setSoTimeout(soTimeoutMs);
        } catch (IOException ignored) {
            // The socket is about to be wrapped by a PeerLink (which resets the timeout per read)
            // or closed; a failure to restore here is harmless.
        }
    }
}
