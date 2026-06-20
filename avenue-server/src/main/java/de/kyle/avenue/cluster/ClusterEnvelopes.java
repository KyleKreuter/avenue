package de.kyle.avenue.cluster;

import com.google.protobuf.ByteString;
import de.kyle.avenue.cluster.membership.GossipUpdate;
import de.kyle.avenue.proto.ClusterAck;
import de.kyle.avenue.proto.ClusterAuthChallenge;
import de.kyle.avenue.proto.ClusterAuthHello;
import de.kyle.avenue.proto.ClusterAuthResponse;
import de.kyle.avenue.proto.ClusterEnvelope;
import de.kyle.avenue.proto.ClusterGap;
import de.kyle.avenue.proto.ClusterInterest;
import de.kyle.avenue.proto.ClusterInterestSync;
import de.kyle.avenue.proto.ClusterPublish;
import de.kyle.avenue.proto.ClusterResume;
import de.kyle.avenue.proto.Heartbeat;
import de.kyle.avenue.proto.SwimAck;
import de.kyle.avenue.proto.SwimJoin;
import de.kyle.avenue.proto.SwimJoinAck;
import de.kyle.avenue.proto.SwimLeave;
import de.kyle.avenue.proto.SwimPing;
import de.kyle.avenue.proto.SwimPingReq;
import de.kyle.avenue.proto.SwimPingReqAck;

import java.util.List;

/**
 * Factory for the {@link ClusterEnvelope}s exchanged on the cluster control plane (Step 2 of the
 * JSON-&gt;protobuf migration). Centralizing the {@code oneof}-field wiring keeps the call sites in
 * {@link PeerLink}, {@link ClusterManager}, {@link ClusterHandshake} and the SWIM engine free of the
 * repeated builder boilerplate and is the single place that maps the domain
 * {@link GossipUpdate} list to the {@code repeated GossipUpdate} on the wire.
 */
public final class ClusterEnvelopes {

    private ClusterEnvelopes() {
    }

    // --- Publish + at-least-once control --------------------------------------------------------

    /**
     * Builds a {@link ClusterPublish} cross-node forward. The payload {@code data} is an opaque
     * {@link ByteString} set verbatim on the {@code bytes} field — the same immutable instance the
     * publish handler parsed from the local client — so the cross-node path never transcodes it
     * to/from a Java {@code String}.
     */
    public static ClusterEnvelope publish(String topic, String source, String originNodeId,
                                   long originEpoch, long seq, long linkSeq, ByteString data) {
        return ClusterEnvelope.newBuilder()
                .setClusterPublish(ClusterPublish.newBuilder()
                        .setTopic(topic)
                        .setSource(source)
                        .setOriginNodeId(originNodeId)
                        .setOriginEpoch(originEpoch)
                        .setSeq(seq)
                        .setLinkSeq(linkSeq)
                        .setData(data))
                .build();
    }

    public static ClusterEnvelope heartbeat(String nodeId) {
        return ClusterEnvelope.newBuilder()
                .setHeartbeat(Heartbeat.newBuilder().setNodeId(nodeId))
                .build();
    }

    /** Single-origin cumulative ack carrying the contiguous link high-water mark. */
    public static ClusterEnvelope ack(String ackerNodeId, String originNodeId, long originEpoch, long ackedSeq) {
        return ClusterEnvelope.newBuilder()
                .setClusterAck(ClusterAck.newBuilder()
                        .setAckerNodeId(ackerNodeId)
                        .addAcks(ClusterAck.AckEntry.newBuilder()
                                .setOriginNodeId(originNodeId)
                                .setOriginEpoch(originEpoch)
                                .setAckedSeq(ackedSeq)))
                .build();
    }

    /** Single-origin resume request carrying our last contiguous link seq. */
    public static ClusterEnvelope resume(String nodeId, String originNodeId, long originEpoch, long lastContiguousSeq) {
        return ClusterEnvelope.newBuilder()
                .setClusterResume(ClusterResume.newBuilder()
                        .setNodeId(nodeId)
                        .addResume(ClusterResume.ResumeEntry.newBuilder()
                                .setOriginNodeId(originNodeId)
                                .setOriginEpoch(originEpoch)
                                .setLastContiguousSeq(lastContiguousSeq)))
                .build();
    }

    public static ClusterEnvelope gap(String senderNodeId, String originNodeId, long originEpoch, long firstAvailableSeq) {
        return ClusterEnvelope.newBuilder()
                .setClusterGap(ClusterGap.newBuilder()
                        .setSenderNodeId(senderNodeId)
                        .setOriginNodeId(originNodeId)
                        .setOriginEpoch(originEpoch)
                        .setFirstAvailableSeq(firstAvailableSeq))
                .build();
    }

    // --- Interest routing -----------------------------------------------------------------------

    public static ClusterEnvelope interest(String nodeId, long interestVersion, String op, String topic) {
        return ClusterEnvelope.newBuilder()
                .setClusterInterest(ClusterInterest.newBuilder()
                        .setNodeId(nodeId)
                        .setInterestVersion(interestVersion)
                        .setOp(op)
                        .setTopic(topic))
                .build();
    }

    public static ClusterEnvelope interestSync(String nodeId, long interestVersion, Iterable<String> topics) {
        return ClusterEnvelope.newBuilder()
                .setClusterInterestSync(ClusterInterestSync.newBuilder()
                        .setNodeId(nodeId)
                        .setInterestVersion(interestVersion)
                        .addAllTopics(topics))
                .build();
    }

    // --- Auth handshake (nonce/proof are raw bytes on the wire) ---------------------------------

    public static ClusterEnvelope authHello(String nodeId, String host, int clusterPort, long incarnation,
                                     ByteString nonce, String protocol) {
        return ClusterEnvelope.newBuilder()
                .setClusterAuthHello(ClusterAuthHello.newBuilder()
                        .setNodeId(nodeId)
                        .setHost(host)
                        .setClusterPort(clusterPort)
                        .setIncarnation(incarnation)
                        .setNonce(nonce)
                        .setProtocol(protocol))
                .build();
    }

    public static ClusterEnvelope authChallenge(String nodeId, String host, int clusterPort, long incarnation,
                                         ByteString nonce, ByteString proof) {
        return ClusterEnvelope.newBuilder()
                .setClusterAuthChallenge(ClusterAuthChallenge.newBuilder()
                        .setNodeId(nodeId)
                        .setHost(host)
                        .setClusterPort(clusterPort)
                        .setIncarnation(incarnation)
                        .setNonce(nonce)
                        .setProof(proof))
                .build();
    }

    public static ClusterEnvelope authResponse(String nodeId, ByteString proof) {
        return ClusterEnvelope.newBuilder()
                .setClusterAuthResponse(ClusterAuthResponse.newBuilder()
                        .setNodeId(nodeId)
                        .setProof(proof))
                .build();
    }

    // --- SWIM membership ------------------------------------------------------------------------

    public static ClusterEnvelope swimPing(long seqNo, List<GossipUpdate> piggyback) {
        SwimPing.Builder b = SwimPing.newBuilder().setSeqNo(seqNo);
        for (GossipUpdate u : piggyback) {
            b.addPiggyback(u.toProto());
        }
        return ClusterEnvelope.newBuilder().setSwimPing(b).build();
    }

    public static ClusterEnvelope swimAck(long seqNo, List<GossipUpdate> piggyback) {
        SwimAck.Builder b = SwimAck.newBuilder().setSeqNo(seqNo);
        for (GossipUpdate u : piggyback) {
            b.addPiggyback(u.toProto());
        }
        return ClusterEnvelope.newBuilder().setSwimAck(b).build();
    }

    public static ClusterEnvelope swimPingReq(String targetNodeId, long seqNo, List<GossipUpdate> piggyback) {
        SwimPingReq.Builder b = SwimPingReq.newBuilder().setTargetNodeId(targetNodeId).setSeqNo(seqNo);
        for (GossipUpdate u : piggyback) {
            b.addPiggyback(u.toProto());
        }
        return ClusterEnvelope.newBuilder().setSwimPingReq(b).build();
    }

    public static ClusterEnvelope swimPingReqAck(String targetNodeId, long seqNo, List<GossipUpdate> piggyback) {
        SwimPingReqAck.Builder b = SwimPingReqAck.newBuilder().setTargetNodeId(targetNodeId).setSeqNo(seqNo);
        for (GossipUpdate u : piggyback) {
            b.addPiggyback(u.toProto());
        }
        return ClusterEnvelope.newBuilder().setSwimPingReqAck(b).build();
    }

    public static ClusterEnvelope swimJoin(String nodeId, String host, int clusterPort, long incarnation) {
        return ClusterEnvelope.newBuilder()
                .setSwimJoin(SwimJoin.newBuilder()
                        .setNodeId(nodeId)
                        .setHost(host)
                        .setClusterPort(clusterPort)
                        .setIncarnation(incarnation))
                .build();
    }

    public static ClusterEnvelope swimJoinAck(List<GossipUpdate> members) {
        SwimJoinAck.Builder b = SwimJoinAck.newBuilder();
        for (GossipUpdate u : members) {
            b.addMembers(u.toProto());
        }
        return ClusterEnvelope.newBuilder().setSwimJoinAck(b).build();
    }

    public static ClusterEnvelope swimLeave(String nodeId, long incarnation) {
        return ClusterEnvelope.newBuilder()
                .setSwimLeave(SwimLeave.newBuilder()
                        .setNodeId(nodeId)
                        .setIncarnation(incarnation))
                .build();
    }

    /** Maps a {@code repeated GossipUpdate} from the wire back to the domain list. */
    public static List<GossipUpdate> fromProtoList(List<de.kyle.avenue.proto.GossipUpdate> protos) {
        List<GossipUpdate> out = new java.util.ArrayList<>(protos.size());
        for (de.kyle.avenue.proto.GossipUpdate p : protos) {
            out.add(GossipUpdate.fromProto(p));
        }
        return out;
    }
}
