package de.kyle.avenue.packet.cluster;

import de.kyle.avenue.packet.Packet;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

/**
 * Third and final message of the HMAC challenge-response cluster handshake, sent by the
 * <em>initiator</em> to the <em>acceptor</em> after the initiator has verified the acceptor's
 * challenge.
 * <p>
 * It carries the initiator's {@code proof}: an HMAC over the joined transcript of both node IDs and
 * both nonces tagged with the {@code INITIATOR} role. The acceptor verifies it to complete mutual
 * authentication. No identity fields are repeated here beyond {@code nodeId}, since the acceptor
 * already learned the initiator's advertised data from the preceding {@link ClusterAuthHelloPacket}.
 * <p>
 * Wire shape:
 * <pre>
 *   header = {name, nodeId}
 *   body   = {proof}
 * </pre>
 */
public class ClusterAuthResponsePacket implements Packet {

    private final String nodeId;
    private final String proof;

    public ClusterAuthResponsePacket(String nodeId, String proof) {
        this.nodeId = nodeId;
        this.proof = proof;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getProof() {
        return proof;
    }

    @Override
    public byte[] getHeader() {
        JSONObject obj = new JSONObject();
        obj.put("name", getClass().getSimpleName());
        obj.put("nodeId", nodeId);
        return obj.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getBody() {
        JSONObject obj = new JSONObject();
        obj.put("proof", proof);
        return obj.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Parses a received JSON envelope into a {@link ClusterAuthResponsePacket}.
     *
     * @param envelope the full {@code {"header":{...},"body":{...}}} JSON object
     * @return parsed packet
     */
    public static ClusterAuthResponsePacket fromJson(JSONObject envelope) {
        JSONObject header = envelope.getJSONObject("header");
        JSONObject body = envelope.getJSONObject("body");
        return new ClusterAuthResponsePacket(
                header.getString("nodeId"),
                body.getString("proof")
        );
    }
}
