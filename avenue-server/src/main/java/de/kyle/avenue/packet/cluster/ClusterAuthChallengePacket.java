package de.kyle.avenue.packet.cluster;

import de.kyle.avenue.packet.Packet;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

/**
 * Second message of the HMAC challenge-response cluster handshake, sent by the <em>acceptor</em>
 * back to the <em>initiator</em> in reply to a {@link ClusterAuthHelloPacket}.
 * <p>
 * It advertises the acceptor's identity/contact information, carries the acceptor's own fresh
 * {@code nonce}, and includes the acceptor's {@code proof}: an HMAC over the joined transcript of
 * both node IDs and both nonces tagged with the {@code ACCEPTOR} role. By presenting a valid proof
 * the acceptor demonstrates possession of the shared secret without ever transmitting it.
 * <p>
 * Wire shape:
 * <pre>
 *   header = {name, nodeId, host, clusterPort, incarnation}
 *   body   = {nonce, proof}
 * </pre>
 */
public class ClusterAuthChallengePacket implements Packet {

    private final String nodeId;
    private final String host;
    private final int clusterPort;
    private final long incarnation;
    private final String nonce;
    private final String proof;

    public ClusterAuthChallengePacket(
            String nodeId,
            String host,
            int clusterPort,
            long incarnation,
            String nonce,
            String proof
    ) {
        this.nodeId = nodeId;
        this.host = host;
        this.clusterPort = clusterPort;
        this.incarnation = incarnation;
        this.nonce = nonce;
        this.proof = proof;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getHost() {
        return host;
    }

    public int getClusterPort() {
        return clusterPort;
    }

    public long getIncarnation() {
        return incarnation;
    }

    public String getNonce() {
        return nonce;
    }

    public String getProof() {
        return proof;
    }

    @Override
    public byte[] getHeader() {
        JSONObject obj = new JSONObject();
        obj.put("name", getClass().getSimpleName());
        obj.put("nodeId", nodeId);
        obj.put("host", host);
        obj.put("clusterPort", clusterPort);
        obj.put("incarnation", incarnation);
        return obj.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getBody() {
        JSONObject obj = new JSONObject();
        obj.put("nonce", nonce);
        obj.put("proof", proof);
        return obj.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Parses a received JSON envelope into a {@link ClusterAuthChallengePacket}.
     *
     * @param envelope the full {@code {"header":{...},"body":{...}}} JSON object
     * @return parsed packet
     */
    public static ClusterAuthChallengePacket fromJson(JSONObject envelope) {
        JSONObject header = envelope.getJSONObject("header");
        JSONObject body = envelope.getJSONObject("body");
        return new ClusterAuthChallengePacket(
                header.getString("nodeId"),
                header.getString("host"),
                header.getInt("clusterPort"),
                header.getLong("incarnation"),
                body.getString("nonce"),
                body.getString("proof")
        );
    }
}
