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
 * End-to-end integration test for the hand-rolled NIO selector transport ({@code server.io-mode=nio},
 * Event-Loop step B). It is the correctness proof that the NIO transport delivers the exact same
 * client protocol semantics as the blocking transport: it boots a real {@link SingleNodeServer} in
 * NIO mode on an ephemeral port and drives it with real {@link TestClient}s over real TCP sockets
 * (real length-prefix framing + real {@code ClientEnvelope} protobuf), exercising the full path
 * connect -&gt; auth -&gt; subscribe (with {@code SubscribeAck}) -&gt; publish -&gt; cross-client
 * receive.
 * <p>
 * It covers, deterministically (latches/awaits, no sleeps as synchronization):
 * <ul>
 *   <li>basic cross-client delivery;</li>
 *   <li>fan-out to two subscribers (which generally land on different I/O workers, exercising the
 *       cross-thread {@code enqueuePreSerialized} + {@code OP_WRITE} wakeup path);</li>
 *   <li>a wrong-token publish that must be rejected and dropped;</li>
 *   <li>5000 messages over a single connection, exercising frame extraction across partial reads and
 *       multiple frames per {@code select()}.</li>
 * </ul>
 */
class NioIntegrationTest {

    private static final String HOST = "127.0.0.1";
    private static final String SECRET = "nio-secret";
    private static final String TOKEN = "nio-token";
    private static final int PACKET_SIZE = 65536;
    private static final long TIMEOUT = 10;
    private static final TimeUnit UNIT = TimeUnit.SECONDS;

    private SingleNodeServer server;
    private int port;

    @BeforeEach
    void startServer() {
        // Direct-value (test) config defaults to the blocking transport; the copy constructor flips
        // it to the NIO selector transport (auto worker count). dropUnknownPackets=true so the
        // wrong-token publisher is deterministically dropped.
        AvenueConfig base = new AvenueConfig(
                PACKET_SIZE,
                true,
                SECRET,
                TOKEN,
                0,         // ephemeral port
                1_000_000, // large outbound queue: the 5000-message burst exercises FRAMING across
                           // partial reads, not backpressure. A generous capacity keeps the test
                           // deterministic even when the whole suite competes for CPU (the slow-
                           // consumer disconnect path has its own dedicated coverage elsewhere).
                100
        );
        AvenueConfig config = new AvenueConfig(base, "nio", 0);
        Assertions.assertTrue(config.isNioIoMode(), "Test config must select the NIO transport");

        server = new SingleNodeServer(config);
        server.start();
        port = server.getPort();
        Assertions.assertTrue(port > 0, "NIO server must report a bound ephemeral port");
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
        Assertions.assertEquals(TOKEN, token, "NIO server must issue the configured token for the valid secret");
        return client;
    }

    @Test
    void subscriber_receives_published_message_over_nio() throws Exception {
        try (TestClient subscriber = connectAuthedClient();
             TestClient publisher = connectAuthedClient()) {

            subscriber.subscribe("news", TOKEN, TIMEOUT, UNIT);
            publisher.publish("news", "hello-nio", "publisher-1", TOKEN);

            JSONObject received = subscriber.awaitPacket("PublishMessageOutboundPacket", TIMEOUT, UNIT);
            Assertions.assertNotNull(received, "Subscriber must receive the published message over NIO");
            Assertions.assertEquals("news", received.getJSONObject("header").getString("topic"));
            Assertions.assertEquals("publisher-1", received.getJSONObject("header").getString("source"));
            Assertions.assertEquals("hello-nio", received.getJSONObject("body").getString("data"));
        }
    }

    @Test
    void fan_out_reaches_two_subscribers_over_nio() throws Exception {
        try (TestClient subscriberA = connectAuthedClient();
             TestClient subscriberB = connectAuthedClient();
             TestClient publisher = connectAuthedClient()) {

            subscriberA.subscribe("sports", TOKEN, TIMEOUT, UNIT);
            subscriberB.subscribe("sports", TOKEN, TIMEOUT, UNIT);
            publisher.publish("sports", "goal", "publisher-1", TOKEN);

            JSONObject a = subscriberA.awaitPacket("PublishMessageOutboundPacket", TIMEOUT, UNIT);
            JSONObject b = subscriberB.awaitPacket("PublishMessageOutboundPacket", TIMEOUT, UNIT);
            Assertions.assertNotNull(a, "Subscriber A must receive the fan-out message");
            Assertions.assertNotNull(b, "Subscriber B must receive the fan-out message");
            Assertions.assertEquals("goal", a.getJSONObject("body").getString("data"));
            Assertions.assertEquals("goal", b.getJSONObject("body").getString("data"));
        }
    }

    @Test
    void publish_with_wrong_token_is_not_delivered_over_nio() throws Exception {
        try (TestClient subscriber = connectAuthedClient();
             TestClient badPublisher = connectAuthedClient()) {

            subscriber.subscribe("secure", TOKEN, TIMEOUT, UNIT);
            // Wrong token: the typed security gate throws -> with drop-unknown the offender is dropped
            // and nothing is routed (parity with the blocking transport).
            badPublisher.publish("secure", "should-not-arrive", "attacker", "WRONG-TOKEN");

            Assertions.assertTrue(
                    subscriber.expectNoPacket("PublishMessageOutboundPacket", 1, TimeUnit.SECONDS),
                    "A publish with an invalid token must not be delivered over NIO");
        }
    }

    @Test
    void many_messages_over_one_connection_are_all_delivered() throws Exception {
        final int messageCount = 5000;
        try (TestClient subscriber = connectAuthedClient();
             TestClient publisher = connectAuthedClient()) {

            subscriber.subscribe("stream", TOKEN, TIMEOUT, UNIT);

            // Fire 5000 publishes back-to-back from one connection. This stresses frame extraction
            // across partial reads and multiple frames coalesced into a single select() wakeup on
            // both the publisher's read side and the subscriber's write side.
            for (int i = 0; i < messageCount; i++) {
                publisher.publish("stream", "msg-" + i, "publisher-1", TOKEN);
            }

            int received = 0;
            for (int i = 0; i < messageCount; i++) {
                JSONObject packet = subscriber.awaitPacket("PublishMessageOutboundPacket", TIMEOUT, UNIT);
                Assertions.assertNotNull(packet,
                        "Subscriber must receive message " + i + " of " + messageCount);
                received++;
            }
            Assertions.assertEquals(messageCount, received,
                    "All " + messageCount + " messages must be delivered over a single NIO connection");
        }
    }
}
