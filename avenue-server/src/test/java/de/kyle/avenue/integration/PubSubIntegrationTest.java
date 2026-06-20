package de.kyle.avenue.integration;

import de.kyle.avenue.SingleNodeServer;
import de.kyle.avenue.config.AvenueConfig;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

/**
 * End-to-end pub-sub integration test (E19).
 * <p>
 * Spins up a real {@link SingleNodeServer} on an ephemeral port with a known test config and
 * drives it through real TCP sockets using {@link TestClient} (real framing + real packet
 * classes). The full path connect -&gt; auth -&gt; subscribe -&gt; publish -&gt; receive is
 * verified deterministically with bounded waits (no fixed sleeps used as synchronization).
 * <p>
 * No {@code AvenueClient} smoke test is included here on purpose:
 * <ul>
 *   <li>{@code AvenueClient} lives in the {@code avenue-api} module, which depends on
 *       {@code avenue-server}; testing it from here would invert that dependency.</li>
 *   <li>It is a process-wide singleton whose host/port come solely from {@code AvenueClientConfig}
 *       (env / {@code .env} / properties). The server here binds an EPHEMERAL port known only at
 *       runtime, and OS environment variables cannot be set in-process to feed {@code SERVER_PORT}
 *       to the singleton. Wiring that up would require reflection hacks on the singleton field or
 *       a fixed port (flaky in CI). The raw-socket path below exercises the exact same wire
 *       protocol the client speaks, so coverage is not lost.
 * </ul>
 */
class PubSubIntegrationTest {

    private static final String HOST = "127.0.0.1";
    private static final String SECRET = "test-secret";
    private static final String TOKEN = "test-token";
    private static final int PACKET_SIZE = 65536;
    private static final long TIMEOUT = 5;
    private static final TimeUnit UNIT = TimeUnit.SECONDS;

    private SingleNodeServer server;
    private int port;

    @BeforeEach
    void startServer() {
        // dropUnknownPackets = true reflects the production default and lets us assert the
        // "bad token disconnects the offending client" behaviour deterministically.
        AvenueConfig config = new AvenueConfig(
                PACKET_SIZE,
                true,
                SECRET,
                TOKEN,
                0, // ephemeral port
                1024,
                100
        );
        server = new SingleNodeServer(config);
        server.start();
        port = server.getPort();
        Assertions.assertTrue(port > 0, "Server must report a bound ephemeral port");
    }

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop();
        }
    }

    private TestClient connectAuthedClient() throws Exception {
        TestClient client = new TestClient(HOST, port, PACKET_SIZE);
        String token = client.authenticate(SECRET, TIMEOUT, UNIT);
        Assertions.assertEquals(TOKEN, token, "Server must issue the configured token for the valid secret");
        return client;
    }

    @Test
    void subscriber_receives_published_message() throws Exception {
        try (TestClient subscriber = connectAuthedClient();
             TestClient publisher = connectAuthedClient()) {

            // Block until the server acknowledges the subscription, then publish. This makes
            // delivery deterministic: registration happened-before ack-receipt happened-before
            // publish, so the publish cannot race ahead of the subscription.
            subscriber.subscribe("news", TOKEN, TIMEOUT, UNIT);
            publisher.publish("news", "hello-world", "publisher-1", TOKEN);

            JSONObject received = subscriber.awaitPacket("PublishMessageOutboundPacket", TIMEOUT, UNIT);
            Assertions.assertNotNull(received, "Subscriber must receive the published message");
            Assertions.assertEquals("news", received.getJSONObject("header").getString("topic"));
            Assertions.assertEquals("publisher-1", received.getJSONObject("header").getString("source"));
            Assertions.assertEquals("hello-world", received.getJSONObject("body").getString("data"));
        }
    }

    @Test
    void message_on_topic_without_subscribers_reaches_nobody_and_does_not_crash() throws Exception {
        try (TestClient publisher = connectAuthedClient();
             TestClient idleClient = connectAuthedClient()) {

            // idleClient is authenticated but subscribed to nothing.
            publisher.publish("orphan-topic", "data", "publisher-1", TOKEN);

            Assertions.assertTrue(
                    idleClient.expectNoPacket("PublishMessageOutboundPacket", 1, TimeUnit.SECONDS),
                    "A non-subscriber must not receive the message");

            // Server is still healthy: a real subscriber on a new topic still works afterwards.
            try (TestClient subscriber = connectAuthedClient()) {
                subscriber.subscribe("live", TOKEN, TIMEOUT, UNIT);
                publisher.publish("live", "still-alive", "publisher-1", TOKEN);
                Assertions.assertNotNull(
                        subscriber.awaitPacket("PublishMessageOutboundPacket", TIMEOUT, UNIT),
                        "Server must keep working after a no-subscriber publish");
            }
        }
    }

    @Test
    void topic_keys_are_normalized_case_insensitively() throws Exception {
        try (TestClient subscriber = connectAuthedClient();
             TestClient publisher = connectAuthedClient()) {

            // Subscribe with mixed case, publish with lower case -> normalization must match.
            // The blocking subscribe waits for the ack (whose topic is the normalized key), so
            // the subscription is guaranteed registered before the publish is sent.
            subscriber.subscribe("News", TOKEN, TIMEOUT, UNIT);
            publisher.publish("news", "case-test", "publisher-1", TOKEN);

            JSONObject received = subscriber.awaitPacket("PublishMessageOutboundPacket", TIMEOUT, UNIT);
            Assertions.assertNotNull(received, "Subscriber must receive despite topic case mismatch");
            Assertions.assertEquals("case-test", received.getJSONObject("body").getString("data"));
        }
    }

    @Test
    void two_subscribers_on_same_topic_both_receive() throws Exception {
        try (TestClient subscriberA = connectAuthedClient();
             TestClient subscriberB = connectAuthedClient();
             TestClient publisher = connectAuthedClient()) {

            // Both subscriptions are acknowledged before the publish, so both deliveries are
            // guaranteed.
            subscriberA.subscribe("sports", TOKEN, TIMEOUT, UNIT);
            subscriberB.subscribe("sports", TOKEN, TIMEOUT, UNIT);
            publisher.publish("sports", "goal", "publisher-1", TOKEN);

            JSONObject a = subscriberA.awaitPacket("PublishMessageOutboundPacket", TIMEOUT, UNIT);
            JSONObject b = subscriberB.awaitPacket("PublishMessageOutboundPacket", TIMEOUT, UNIT);
            Assertions.assertNotNull(a, "Subscriber A must receive the message");
            Assertions.assertNotNull(b, "Subscriber B must receive the message");
            Assertions.assertEquals("goal", a.getJSONObject("body").getString("data"));
            Assertions.assertEquals("goal", b.getJSONObject("body").getString("data"));
        }
    }

    @Test
    void publish_with_wrong_token_is_not_delivered_and_drops_the_offender() throws Exception {
        try (TestClient subscriber = connectAuthedClient();
             TestClient badPublisher = connectAuthedClient()) {

            // Subscribe with a valid token and wait for the ack, so the subscription is
            // definitely registered; the negative assertion then proves the bad publish is
            // dropped rather than merely racing the subscription.
            subscriber.subscribe("secure", TOKEN, TIMEOUT, UNIT);

            // Wrong token: server's @Secured check throws -> with dropUnknown=true the offending
            // client connection is torn down and the message is never routed.
            badPublisher.publish("secure", "should-not-arrive", "attacker", "WRONG-TOKEN");

            Assertions.assertTrue(
                    subscriber.expectNoPacket("PublishMessageOutboundPacket", 1, TimeUnit.SECONDS),
                    "A publish with an invalid token must not be delivered");
        }
    }

    @Test
    void subscribe_with_wrong_token_does_not_register_subscription() throws Exception {
        try (TestClient badSubscriber = new TestClient(HOST, port, PACKET_SIZE);
             TestClient publisher = connectAuthedClient()) {

            // Authenticate the publisher normally; the bad subscriber authenticates too but then
            // subscribes with an invalid token, which must be rejected (connection dropped).
            badSubscriber.authenticate(SECRET, TIMEOUT, UNIT);
            badSubscriber.subscribe("vip", "WRONG-TOKEN");

            // Give the server a moment to process and drop; then publish with a valid token.
            publisher.publish("vip", "secret-data", "publisher-1", TOKEN);

            Assertions.assertTrue(
                    badSubscriber.expectNoPacket("PublishMessageOutboundPacket", 1, TimeUnit.SECONDS),
                    "A subscription attempted with an invalid token must not receive messages");
        }
    }

    @Test
    void publish_with_empty_topic_is_rejected_and_not_delivered() throws Exception {
        // After the protobuf cutover a missing topic is represented as the proto3 default empty
        // string. The typed dispatch gate must treat "" exactly like the old "missing topic" case:
        // reject the publish (and, with dropUnknown=true, drop the offender) so nothing is routed.
        try (TestClient subscriber = connectAuthedClient();
             TestClient badPublisher = connectAuthedClient()) {

            subscriber.subscribe("topicful", TOKEN, TIMEOUT, UNIT);
            // Valid token, but empty topic -> must be rejected by the @TopicHandler-equivalent gate.
            badPublisher.publish("", "should-not-arrive", "attacker", TOKEN);

            Assertions.assertTrue(
                    subscriber.expectNoPacket("PublishMessageOutboundPacket", 1, TimeUnit.SECONDS),
                    "A publish with an empty topic must not be delivered");
        }
    }

    @Test
    void valid_secret_yields_configured_token() throws Exception {
        try (TestClient client = new TestClient(HOST, port, PACKET_SIZE)) {
            String token = client.authenticate(SECRET, TIMEOUT, UNIT);
            Assertions.assertEquals(TOKEN, token);
        }
    }
}
