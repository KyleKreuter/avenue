package de.kyle.avenue.serialization;

import de.kyle.avenue.proto.AuthTokenResponse;
import de.kyle.avenue.proto.ClientEnvelope;
import de.kyle.avenue.proto.PublishOutbound;
import de.kyle.avenue.proto.SubscribeAck;

/**
 * Small factory for the server-to-client {@link ClientEnvelope} messages.
 * <p>
 * Centralizes envelope construction so the publish/subscribe/auth handlers and the cluster
 * delivery path ({@link de.kyle.avenue.cluster.PeerLink}) all build identical wire messages. This
 * is the client-plane analogue of {@code ClusterEnvelopes} on the cluster plane.
 */
public final class ClientEnvelopes {

    private ClientEnvelopes() {
    }

    /**
     * Builds a {@link PublishOutbound} envelope for fan-out to a local subscriber.
     *
     * @param topic  the (already normalized) topic the message was published on
     * @param source the publisher's logical source name
     * @param data   the message payload
     * @return a client envelope with the {@code publish_outbound} case set
     */
    public static ClientEnvelope publishOutbound(String topic, String source, String data) {
        return ClientEnvelope.newBuilder()
                .setPublishOutbound(PublishOutbound.newBuilder()
                        .setTopic(topic)
                        .setSource(source)
                        .setData(data)
                        .build())
                .build();
    }

    /**
     * Builds an {@link AuthTokenResponse} envelope carrying the issued token.
     *
     * @param token the token to return to the client
     * @return a client envelope with the {@code auth_response} case set
     */
    public static ClientEnvelope authResponse(String token) {
        return ClientEnvelope.newBuilder()
                .setAuthResponse(AuthTokenResponse.newBuilder().setToken(token).build())
                .build();
    }

    /**
     * Builds a {@link SubscribeAck} envelope for an already-registered subscription.
     *
     * @param topic the normalized topic the subscription was stored under
     * @return a client envelope with the {@code subscribe_ack} case set
     */
    public static ClientEnvelope subscribeAck(String topic) {
        return ClientEnvelope.newBuilder()
                .setSubscribeAck(SubscribeAck.newBuilder().setTopic(topic).build())
                .build();
    }
}
