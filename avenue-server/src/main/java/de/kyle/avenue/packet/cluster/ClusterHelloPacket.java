package de.kyle.avenue.packet.cluster;

import de.kyle.avenue.packet.Packet;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

/**
 * Cluster peer handshake packet.
 * <p>
 * Sent by both sides of a cluster link immediately after TCP connection is established.
 * Carries the sender's {@code nodeId} and the {@code secret} for mutual authentication.
 * The connection is closed if the secret does not match the locally configured
 * {@code cluster.secret}.
 */
public class ClusterHelloPacket implements Packet {

    private final String nodeId;
    private final String secret;

    public ClusterHelloPacket(String nodeId, String secret) {
        this.nodeId = nodeId;
        this.secret = secret;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getSecret() {
        return secret;
    }

    @Override
    public byte[] getHeader() {
        JSONObject obj = new JSONObject();
        obj.put("name", getClass().getSimpleName());
        obj.put("nodeId", nodeId);
        obj.put("secret", secret);
        return obj.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getBody() {
        return "{}".getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Parses a received JSON envelope into a {@link ClusterHelloPacket}.
     *
     * @param envelope the full {@code {"header":{...},"body":{...}}} JSON object
     * @return parsed packet
     */
    public static ClusterHelloPacket fromJson(JSONObject envelope) {
        JSONObject header = envelope.getJSONObject("header");
        return new ClusterHelloPacket(header.getString("nodeId"), header.getString("secret"));
    }
}
