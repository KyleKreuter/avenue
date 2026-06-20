package de.kyle.avenue.serialization;

import com.google.protobuf.ByteString;
import de.kyle.avenue.proto.ClientEnvelope;
import de.kyle.avenue.proto.PublishOutbound;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Wire-safety guardrail for {@link OutboundEncoder}.
 * <p>
 * The publish hot path replaced the {@code ClientEnvelopes.publishOutbound(...).toByteArray()} builder
 * round-trip with {@link OutboundEncoder#encodePublishOutbound} writing the wire bytes directly via a
 * {@link com.google.protobuf.CodedOutputStream}. This test pins that the direct encoder is byte-for-byte
 * identical to the generated builder/message serialization across a broad input matrix — empty and
 * non-empty {@code topic}/{@code source}/{@code data}, ASCII, multi-byte UTF-8, special characters and
 * large payloads — and that the produced bytes round-trip back into the correct {@link ClientEnvelope}.
 * If the wire layout ever diverges (a wrong field number, a missed proto3 default-omission, a varint
 * length bug) this test fails before any malformed frame can reach a client.
 */
class OutboundEncoderTest {

    /** Wire equality: directEncode == builder.toByteArray() for a representative case. */
    @Test
    void directEncodeMatchesBuilderForRepresentativeCase() {
        assertWireIdentical("orders", "client-7", ByteString.copyFromUtf8("hello world"));
    }

    /** Wire equality across a broad matrix of edge-case inputs. */
    @Test
    void directEncodeMatchesBuilderAcrossInputMatrix() {
        List<String> topics = textInputs();
        List<String> sources = textInputs();
        List<ByteString> datas = dataInputs();

        // Full cross-product would be huge; sample every text against every data plus a few text pairs.
        for (ByteString data : datas) {
            for (String topic : topics) {
                for (String source : sources) {
                    assertWireIdentical(topic, source, data);
                }
            }
        }
    }

    /** Round-trip: decoding the directly-encoded bytes yields the correct PublishOutbound envelope. */
    @Test
    void directEncodeRoundTripsThroughDecoder() {
        String topic = "Bestellungen-Übersicht";
        String source = "klient-π";
        ByteString data = ByteString.copyFromUtf8("Nutzlast mit Ümläüten und 𝄞 emoji 🎵");

        byte[] payload = OutboundEncoder.encodePublishOutbound(topic, source, data);
        ClientEnvelope decoded = WireCodec.decodeClient(payload);

        assertEquals(ClientEnvelope.MsgCase.PUBLISH_OUTBOUND, decoded.getMsgCase());
        PublishOutbound out = decoded.getPublishOutbound();
        assertEquals(topic, out.getTopic());
        assertEquals(source, out.getSource());
        assertEquals(data, out.getData());
    }

    /** All-empty inputs must encode to the same (non-trivial: just the empty nested message) bytes. */
    @Test
    void directEncodeMatchesBuilderForAllEmpty() {
        assertWireIdentical("", "", ByteString.EMPTY);
    }

    /** A large multi-kilobyte payload must still encode wire-identically. */
    @Test
    void directEncodeMatchesBuilderForLargePayload() {
        byte[] big = new byte[64 * 1024];
        for (int i = 0; i < big.length; i++) {
            big[i] = (byte) (i & 0xFF);
        }
        assertWireIdentical("metrics.firehose", "load-generator", ByteString.copyFrom(big));
    }

    // --- Helpers --------------------------------------------------------------------------------

    private static void assertWireIdentical(String topic, String source, ByteString data) {
        byte[] expected = ClientEnvelopes.publishOutbound(topic, source, data).toByteArray();
        byte[] actual = OutboundEncoder.encodePublishOutbound(topic, source, data);
        assertArrayEquals(expected, actual,
                "direct encode must be wire-identical to builder for topic=[" + topic
                        + "] source=[" + source + "] dataLen=" + data.size());
    }

    private static List<String> textInputs() {
        List<String> inputs = new ArrayList<>();
        inputs.add("");                              // empty -> proto3 omits the field entirely
        inputs.add("orders");                        // plain ASCII
        inputs.add("a/b.c-d_e:f");                   // ASCII special characters
        inputs.add("Bestellungen");                  // ASCII-ish word
        inputs.add("Ümläüt-Töpic-ß");                // multi-byte UTF-8 (2-byte)
        inputs.add("πράξη-Δ");                        // Greek (2-byte UTF-8)
        inputs.add("订单-主题");                        // CJK (3-byte UTF-8)
        inputs.add("emoji-🎵-𝄞");                      // 4-byte UTF-8 (surrogate pairs)
        inputs.add("x".repeat(300));                 // forces a 2-byte varint length prefix
        return inputs;
    }

    private static List<ByteString> dataInputs() {
        List<ByteString> inputs = new ArrayList<>();
        inputs.add(ByteString.EMPTY);                                   // empty -> proto3 omits data
        inputs.add(ByteString.copyFromUtf8("x"));                       // single byte
        inputs.add(ByteString.copyFromUtf8("hello world"));             // small ASCII
        inputs.add(ByteString.copyFromUtf8("Nutzlast mit Ümläüten 🎵")); // multi-byte UTF-8 payload
        inputs.add(ByteString.copyFrom(new byte[]{0x00, (byte) 0xFF, 0x10, (byte) 0x80, 0x7F})); // raw non-UTF-8 bytes
        inputs.add(ByteString.copyFrom("y".repeat(200).getBytes(StandardCharsets.UTF_8))); // 2-byte varint length
        return inputs;
    }
}
