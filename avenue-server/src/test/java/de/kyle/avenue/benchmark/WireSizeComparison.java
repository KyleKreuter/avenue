package de.kyle.avenue.benchmark;

import com.google.protobuf.ByteString;
import de.kyle.avenue.proto.ClientEnvelope;
import de.kyle.avenue.proto.ClusterEnvelope;
import de.kyle.avenue.proto.ClusterPublish;
import de.kyle.avenue.proto.PublishOutbound;
import de.kyle.avenue.serialization.WireCodec;

import java.nio.charset.StandardCharsets;

/**
 * Standalone wire-size comparison: Protobuf envelope vs. the legacy JSON {@code {header,body}}
 * representation, for two representative messages (a client {@code PublishOutbound} delivered to a
 * subscriber and a cross-node {@code ClusterPublish}).
 * <p>
 * Like the throughput benchmarks it is a plain {@code main} (NOT a {@code *Test}), so the Surefire
 * include pattern ({@code **}{@code /*Test.java}) never runs it during {@code mvn test} and it never
 * lands in the production jar (it lives under {@code src/test/java}).
 * <p>
 * The Protobuf side is measured exactly, through {@link WireCodec} (the bare envelope payload, i.e.
 * what {@code PacketFraming} length-prefixes onto the stream). The JSON side is reconstructed by hand
 * because the old serializers were deleted in the cutover: it rebuilds the exact {@code {header,body}}
 * JSON string the legacy protocol put on the wire (same field names, same {@code name}-based dispatch
 * header) and measures its UTF-8 byte length. Both sides exclude the 4-byte length prefix, which is
 * identical for both encodings, so the comparison is apples-to-apples on the payload.
 * <p>
 * How to run (from the repository root):
 * <pre>
 *   export JAVA_HOME=/path/to/jdk-21
 *   /opt/homebrew/bin/mvn -q -pl avenue-server test-compile
 *   /opt/homebrew/bin/mvn -q -pl avenue-server exec:java \
 *       -Dexec.classpathScope=test \
 *       -Dexec.mainClass=de.kyle.avenue.benchmark.WireSizeComparison
 * </pre>
 */
public final class WireSizeComparison {

    private WireSizeComparison() {
    }

    public static void main(String[] args) {
        System.out.println("Wire-size comparison: legacy JSON {header,body} vs. Protobuf envelope");
        System.out.println("(payload bytes only; the identical 4-byte length prefix is excluded)");
        System.out.println("==================================================================");

        comparePublishOutbound();
        System.out.println();
        compareClusterPublish();
    }

    /**
     * A typical fan-out delivery to a subscriber: topic + source header, a small JSON-ish data body.
     * Representative of the single-node and client-facing hot path.
     */
    private static void comparePublishOutbound() {
        String topic = "orders.eu.created";
        String source = "service-checkout-7";
        String data = "{\"orderId\":\"a1b2c3\",\"amount\":4299,\"currency\":\"EUR\"}";

        // Protobuf: exact bytes the codec puts on the wire (the bare ClientEnvelope payload).
        ClientEnvelope envelope = ClientEnvelope.newBuilder()
                .setPublishOutbound(PublishOutbound.newBuilder()
                        .setTopic(topic).setSource(source).setData(ByteString.copyFromUtf8(data)).build())
                .build();
        int protobufBytes = WireCodec.encodeClient(envelope).length;

        // Legacy JSON: the old PublishMessageOutboundPacket serialized as {header:{name,topic,source},
        // body:{data}}. Field order/spacing mirrors the old org.json output (compact, no spaces).
        String legacyJson = "{"
                + "\"header\":{"
                + "\"name\":\"publish-message\","
                + "\"topic\":" + jsonString(topic) + ","
                + "\"source\":" + jsonString(source)
                + "},"
                + "\"body\":{"
                + "\"data\":" + jsonString(data)
                + "}"
                + "}";
        int jsonBytes = legacyJson.getBytes(StandardCharsets.UTF_8).length;

        report("PublishOutbound (client fan-out)", jsonBytes, protobufBytes);
    }

    /**
     * A typical cross-node forward: the full cluster reliability header (origin identity, epoch, two
     * sequence spaces) plus the same payload. Representative of the cluster hot path.
     */
    private static void compareClusterPublish() {
        String topic = "orders.eu.created";
        String source = "service-checkout-7";
        String originNodeId = "node-eu-west-1";
        long originEpoch = 1718900000000L;
        long seq = 1048576L;
        long linkSeq = 524288L;
        String data = "{\"orderId\":\"a1b2c3\",\"amount\":4299,\"currency\":\"EUR\"}";

        ClusterEnvelope envelope = ClusterEnvelope.newBuilder()
                .setClusterPublish(ClusterPublish.newBuilder()
                        .setTopic(topic).setSource(source).setOriginNodeId(originNodeId)
                        .setOriginEpoch(originEpoch).setSeq(seq).setLinkSeq(linkSeq)
                        .setData(ByteString.copyFromUtf8(data)).build())
                .build();
        int protobufBytes = WireCodec.encodeCluster(envelope).length;

        // Legacy JSON: the old ClusterPublishPacket as {header:{name,topic,source,originNodeId,
        // originEpoch,seq,linkSeq}, body:{data}}.
        String legacyJson = "{"
                + "\"header\":{"
                + "\"name\":\"cluster-publish\","
                + "\"topic\":" + jsonString(topic) + ","
                + "\"source\":" + jsonString(source) + ","
                + "\"originNodeId\":" + jsonString(originNodeId) + ","
                + "\"originEpoch\":" + originEpoch + ","
                + "\"seq\":" + seq + ","
                + "\"linkSeq\":" + linkSeq
                + "},"
                + "\"body\":{"
                + "\"data\":" + jsonString(data)
                + "}"
                + "}";
        int jsonBytes = legacyJson.getBytes(StandardCharsets.UTF_8).length;

        report("ClusterPublish (cross-node forward)", jsonBytes, protobufBytes);
    }

    private static void report(String label, int jsonBytes, int protobufBytes) {
        double savedPercent = 100.0 * (jsonBytes - protobufBytes) / jsonBytes;
        System.out.printf("%s%n", label);
        System.out.printf("  legacy JSON : %d bytes%n", jsonBytes);
        System.out.printf("  Protobuf    : %d bytes%n", protobufBytes);
        System.out.printf("  saving      : %d bytes (%.1f%% smaller)%n",
                jsonBytes - protobufBytes, savedPercent);
    }

    /** Minimal JSON string escaper so the reconstructed legacy bytes are accurate for our inputs. */
    private static String jsonString(String value) {
        StringBuilder sb = new StringBuilder(value.length() + 2);
        sb.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> sb.append(c);
            }
        }
        sb.append('"');
        return sb.toString();
    }
}
