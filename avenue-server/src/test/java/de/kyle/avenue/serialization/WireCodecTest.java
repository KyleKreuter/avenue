package de.kyle.avenue.serialization;

import de.kyle.avenue.proto.ClientEnvelope;
import de.kyle.avenue.proto.ClusterEnvelope;
import de.kyle.avenue.proto.ClusterPublish;
import de.kyle.avenue.proto.GossipUpdate;
import de.kyle.avenue.proto.MemberStateProto;
import de.kyle.avenue.proto.PublishInbound;
import de.kyle.avenue.proto.SwimJoinAck;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trip and rejection tests for {@link WireCodec}. Confirms that encode -&gt; decode preserves a
 * representative client message, a scalar-heavy cluster message and a cluster message with a repeated
 * nested field, and that malformed bytes are rejected as {@link IllegalArgumentException}.
 */
class WireCodecTest {

    @Test
    void clientPublishInboundRoundTrip() {
        ClientEnvelope original = ClientEnvelope.newBuilder()
                .setPublishInbound(PublishInbound.newBuilder()
                        .setTopic("orders")
                        .setSource("client-7")
                        .setToken("tok-abc")
                        .setData("hello world")
                        .build())
                .build();

        byte[] payload = WireCodec.encodeClient(original);
        ClientEnvelope decoded = WireCodec.decodeClient(payload);

        assertEquals(ClientEnvelope.MsgCase.PUBLISH_INBOUND, decoded.getMsgCase());
        assertEquals("orders", decoded.getPublishInbound().getTopic());
        assertEquals("client-7", decoded.getPublishInbound().getSource());
        assertEquals("tok-abc", decoded.getPublishInbound().getToken());
        assertEquals("hello world", decoded.getPublishInbound().getData());
        assertEquals(original, decoded);
    }

    @Test
    void clusterPublishRoundTrip() {
        ClusterEnvelope original = ClusterEnvelope.newBuilder()
                .setClusterPublish(ClusterPublish.newBuilder()
                        .setTopic("orders")
                        .setSource("client-7")
                        .setOriginNodeId("node-1")
                        .setOriginEpoch(1700000000000L)
                        .setSeq(42L)
                        .setLinkSeq(7L)
                        .setData("payload")
                        .build())
                .build();

        byte[] payload = WireCodec.encodeCluster(original);
        ClusterEnvelope decoded = WireCodec.decodeCluster(payload);

        assertEquals(ClusterEnvelope.MsgCase.CLUSTER_PUBLISH, decoded.getMsgCase());
        ClusterPublish p = decoded.getClusterPublish();
        assertEquals("node-1", p.getOriginNodeId());
        assertEquals(1700000000000L, p.getOriginEpoch());
        assertEquals(42L, p.getSeq());
        assertEquals(7L, p.getLinkSeq());
        assertEquals(original, decoded);
    }

    @Test
    void clusterSwimJoinAckWithRepeatedMembersRoundTrip() {
        ClusterEnvelope original = ClusterEnvelope.newBuilder()
                .setSwimJoinAck(SwimJoinAck.newBuilder()
                        .addMembers(GossipUpdate.newBuilder()
                                .setNodeId("node-1")
                                .setState(MemberStateProto.ALIVE)
                                .setIncarnation(3L)
                                .setHost("127.0.0.1")
                                .setClusterPort(9001)
                                .build())
                        .addMembers(GossipUpdate.newBuilder()
                                .setNodeId("node-2")
                                .setState(MemberStateProto.SUSPECT)
                                .setIncarnation(5L)
                                .setHost("127.0.0.1")
                                .setClusterPort(9002)
                                .build())
                        .build())
                .build();

        byte[] payload = WireCodec.encodeCluster(original);
        ClusterEnvelope decoded = WireCodec.decodeCluster(payload);

        assertEquals(ClusterEnvelope.MsgCase.SWIM_JOIN_ACK, decoded.getMsgCase());
        assertEquals(2, decoded.getSwimJoinAck().getMembersCount());
        assertEquals("node-2", decoded.getSwimJoinAck().getMembers(1).getNodeId());
        assertEquals(MemberStateProto.SUSPECT, decoded.getSwimJoinAck().getMembers(1).getState());
        assertEquals(original, decoded);
    }

    @Test
    void encodeDecodeAreSymmetricAtByteLevel() {
        ClientEnvelope original = ClientEnvelope.newBuilder()
                .setSubscribe(de.kyle.avenue.proto.Subscribe.newBuilder()
                        .setTopic("orders")
                        .setToken("tok-xyz")
                        .build())
                .build();

        byte[] payload = WireCodec.encodeClient(original);
        ClientEnvelope decoded = WireCodec.decodeClient(payload);
        // Re-encoding the decoded envelope must produce identical bytes.
        assertArrayEquals(payload, WireCodec.encodeClient(decoded));
    }

    @Test
    void decodeRejectsGarbageBytes() {
        // 0xFF starts an invalid field tag / truncated varint -> protobuf parse failure.
        byte[] garbage = new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
        assertThrows(IllegalArgumentException.class, () -> WireCodec.decodeCluster(garbage));
        assertThrows(IllegalArgumentException.class, () -> WireCodec.decodeClient(garbage));
    }

    @Test
    void decodeRejectsNull() {
        assertThrows(IllegalArgumentException.class, () -> WireCodec.decodeClient(null));
        assertThrows(IllegalArgumentException.class, () -> WireCodec.decodeCluster(null));
    }

    @Test
    void maxSizeGuardRejectsOversizedPayload() {
        ClusterEnvelope envelope = ClusterEnvelope.newBuilder()
                .setClusterPublish(ClusterPublish.newBuilder().setData("x".repeat(64)).build())
                .build();
        byte[] payload = WireCodec.encodeCluster(envelope);
        assertTrue(payload.length > 1, "payload should be non-trivial");
        assertThrows(IllegalArgumentException.class, () -> WireCodec.encodeCluster(envelope, 1));
        assertThrows(IllegalArgumentException.class, () -> WireCodec.decodeCluster(payload, 1));
    }
}
