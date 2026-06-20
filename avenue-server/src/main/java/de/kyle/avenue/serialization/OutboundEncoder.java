package de.kyle.avenue.serialization;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import de.kyle.avenue.proto.ClientEnvelope;
import de.kyle.avenue.proto.PublishOutbound;

import java.io.IOException;

/**
 * Allocation-lean direct encoder for the single hottest outbound frame: the
 * {@link ClientEnvelope} carrying a {@link PublishOutbound} that is fanned out to every local
 * subscriber on each publish.
 * <p>
 * On the publish hot path the only thing the server needs from the protobuf layer is the bare wire
 * bytes of {@code ClientEnvelope{ publish_outbound = PublishOutbound{topic, source, data} }}. The
 * generated builder path ({@link ClientEnvelopes#publishOutbound}) allocates a
 * {@code PublishOutbound$Builder}, a {@code PublishOutbound} message, a {@code ClientEnvelope$Builder}
 * and a {@code ClientEnvelope} message — all immediately thrown away after {@code toByteArray()} — plus
 * the final {@code byte[]}. Under load these short-lived builder/message graphs dominated server
 * allocation (JFR: ~27% of allocation in the {@code *$Builder}/message objects) and drove the young-GC
 * rate.
 * <p>
 * {@link #encodePublishOutbound} writes the same wire bytes directly with a {@link CodedOutputStream}
 * over a single, exactly-sized {@code byte[]} — <b>one</b> allocation per publish, no builder, no
 * intermediate message object. The layout is byte-for-byte identical to what the generated code emits
 * (same field numbers, same varint lengths, {@code writeString} for {@code topic}/{@code source},
 * {@code writeBytes} for {@code data}, and the same proto3 "skip empty scalar" rule), which the
 * {@code OutboundEncoderTest} guardrail asserts against the builder output across a broad input matrix.
 * <p>
 * The returned bytes are the <em>bare</em> protobuf payload (no 4-byte length prefix): the framing
 * prefix is still added by {@link PacketFraming#writeFrameNoFlush}, exactly as for the builder-produced
 * {@code toByteArray()} bytes the encode-once fan-out used before. The result is immutable and is meant
 * to be shared verbatim across all subscribers of a fan-out.
 */
public final class OutboundEncoder {

    /**
     * Wire tag for the nested {@code publish_outbound} field (field 6, wire type 2 = length-delimited).
     * Equivalent to {@code WireFormat.makeTag(PUBLISH_OUTBOUND_FIELD_NUMBER, WIRETYPE_LENGTH_DELIMITED)}.
     */
    private static final int PUBLISH_OUTBOUND_TAG =
            (ClientEnvelope.PUBLISH_OUTBOUND_FIELD_NUMBER << 3) | 2;

    private OutboundEncoder() {
    }

    /**
     * Directly encodes {@code ClientEnvelope{ publish_outbound = PublishOutbound{topic, source, data} }}
     * into its bare protobuf payload bytes, without allocating any builder or message object.
     * <p>
     * Wire-identical to {@code ClientEnvelopes.publishOutbound(topic, source, data).toByteArray()}: the
     * nested {@link PublishOutbound} is written as a length-delimited sub-message under field
     * {@link ClientEnvelope#PUBLISH_OUTBOUND_FIELD_NUMBER}, and inside it {@code topic}/{@code source}
     * are {@code writeString}-encoded and {@code data} is {@code writeBytes}-encoded, each only when
     * non-empty (the proto3 default-value omission the generated {@code writeTo} applies).
     *
     * @param topic  the (already normalized for lookup, but raw on the wire) topic the message was
     *               published on
     * @param source the publisher's logical source name
     * @param data   the opaque message payload (shared, immutable {@link ByteString})
     * @return the bare protobuf payload bytes of the envelope (no length prefix); one fresh
     *         {@code byte[]} per call, safe to share immutably across subscribers
     */
    public static byte[] encodePublishOutbound(String topic, String source, ByteString data) {
        // 1) Inner PublishOutbound size: only non-empty scalars are emitted (proto3 rule), each as
        //    tag + length + bytes. computeStringSize / computeBytesSize already include the tag and the
        //    varint length prefix, matching the generated getSerializedSize() exactly.
        int innerSize = 0;
        boolean hasTopic = !topic.isEmpty();
        boolean hasSource = !source.isEmpty();
        boolean hasData = !data.isEmpty();
        if (hasTopic) {
            innerSize += CodedOutputStream.computeStringSize(PublishOutbound.TOPIC_FIELD_NUMBER, topic);
        }
        if (hasSource) {
            innerSize += CodedOutputStream.computeStringSize(PublishOutbound.SOURCE_FIELD_NUMBER, source);
        }
        if (hasData) {
            innerSize += CodedOutputStream.computeBytesSize(PublishOutbound.DATA_FIELD_NUMBER, data);
        }

        // 2) Outer envelope size: tag for field 6 + varint(innerSize) + innerSize. This mirrors
        //    CodedOutputStream.writeMessage(6, publishOutbound), i.e. computeMessageSize(6, msg).
        int outerSize = CodedOutputStream.computeTagSize(ClientEnvelope.PUBLISH_OUTBOUND_FIELD_NUMBER)
                + CodedOutputStream.computeUInt32SizeNoTag(innerSize)
                + innerSize;

        // 3) Single exactly-sized allocation: the only object created per publish.
        byte[] out = new byte[outerSize];
        CodedOutputStream cos = CodedOutputStream.newInstance(out);
        try {
            // Outer: tag + length-delimited nested message (== writeMessage(6, ...) expanded).
            cos.writeUInt32NoTag(PUBLISH_OUTBOUND_TAG);
            cos.writeUInt32NoTag(innerSize);
            // Inner fields, written in field-number order exactly as the generated writeTo does.
            if (hasTopic) {
                cos.writeString(PublishOutbound.TOPIC_FIELD_NUMBER, topic);
            }
            if (hasSource) {
                cos.writeString(PublishOutbound.SOURCE_FIELD_NUMBER, source);
            }
            if (hasData) {
                cos.writeBytes(PublishOutbound.DATA_FIELD_NUMBER, data);
            }
            cos.checkNoSpaceLeft();
        } catch (IOException e) {
            // Writing into a fixed in-memory array cannot do real I/O; a failure here means the
            // pre-computed size disagreed with what was written, which is a hard programming error.
            throw new IllegalStateException("Direct outbound encode failed (size mismatch?)", e);
        }
        return out;
    }
}
