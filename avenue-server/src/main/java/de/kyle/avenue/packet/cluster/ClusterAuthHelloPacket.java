package de.kyle.avenue.packet.cluster;

import de.kyle.avenue.packet.Packet;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

/**
 * First message of the HMAC challenge-response cluster handshake, sent by the <em>initiator</em>
 * (the side that opened the TCP connection) to the <em>acceptor</em>.
 * <p>
 * It advertises the initiator's identity and contact information and carries a fresh random
 * {@code nonce}. The shared {@code cluster.secret} is deliberately <strong>not</strong> present:
 * the secret is only ever used as an HMAC key and never travels on the wire.
 * <p>
 * Wire shape:
 * <pre>
 *   header = {name, nodeId, host, clusterPort, incarnation}
 *   body   = {nonce, protocol}
 * </pre>
 * The {@code host}/{@code clusterPort}/{@code incarnation} fields advertise where and as which
 * process incarnation this node can be reached; they are carried now so the future SWIM membership
 * layer (Phase E) can consume them without a protocol bump.
 */
public class ClusterAuthHelloPacket implements Packet {

    private final String nodeId;
    private final String host;
    private final int clusterPort;
    private final long incarnation;
    private final String nonce;
    private final String protocol;

    public ClusterAuthHelloPacket(
            String nodeId,
            String host,
            int clusterPort,
            long incarnation,
            String nonce,
            String protocol
    ) {
        this.nodeId = nodeId;
        this.host = host;
        this.clusterPort = clusterPort;
        this.incarnation = incarnation;
        this.nonce = nonce;
        this.protocol = protocol;
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

    public String getProtocol() {
        return protocol;
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
        obj.put("protocol", protocol);
        return obj.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Parses a received JSON envelope into a {@link ClusterAuthHelloPacket}.
     *
     * @param envelope the full {@code {"header":{...},"body":{...}}} JSON object
     * @return parsed packet
     */
    public static ClusterAuthHelloPacket fromJson(JSONObject envelope) {
        JSONObject header = envelope.getJSONObject("header");
        JSONObject body = envelope.getJSONObject("body");
        return new ClusterAuthHelloPacket(
                header.getString("nodeId"),
                header.getString("host"),
                header.getInt("clusterPort"),
                header.getLong("incarnation"),
                body.getString("nonce"),
                body.getString("protocol")
        );
    }
}
