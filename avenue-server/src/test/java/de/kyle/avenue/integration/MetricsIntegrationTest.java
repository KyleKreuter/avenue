package de.kyle.avenue.integration;

import de.kyle.avenue.SingleNodeServer;
import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.metrics.AvenueMetrics;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

/**
 * Metrics integration test (E20).
 * <p>
 * Drives a real publish/subscribe round trip and then asserts the lightweight
 * {@link AvenueMetrics} registry reflects it: connections were accepted, a message was
 * published and delivered, and the subscription gauge moved. No external metrics dependency
 * is involved.
 */
class MetricsIntegrationTest {

    private static final String HOST = "127.0.0.1";
    private static final String SECRET = "metrics-secret";
    private static final String TOKEN = "metrics-token";
    private static final int PACKET_SIZE = 65536;
    private static final long TIMEOUT = 5;
    private static final TimeUnit UNIT = TimeUnit.SECONDS;

    private SingleNodeServer server;
    private int port;

    @BeforeEach
    void startServer() {
        AvenueConfig config = new AvenueConfig(
                PACKET_SIZE, true, SECRET, TOKEN, 0, 1024, 100);
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

    @Test
    void metrics_reflect_a_publish_round_trip() throws Exception {
        AvenueMetrics metrics = server.getMetrics();

        try (TestClient subscriber = new TestClient(HOST, port, PACKET_SIZE);
             TestClient publisher = new TestClient(HOST, port, PACKET_SIZE)) {

            subscriber.authenticate(SECRET, TIMEOUT, UNIT);
            publisher.authenticate(SECRET, TIMEOUT, UNIT);

            subscriber.subscribe("metrics-topic", TOKEN, TIMEOUT, UNIT);
            publisher.publish("metrics-topic", "value", "pub", TOKEN);

            JSONObject received = subscriber.awaitPacket("PublishMessageOutboundPacket", TIMEOUT, UNIT);
            Assertions.assertNotNull(received, "Subscriber must receive the message");

            // Connections: both clients were accepted and are currently active.
            Assertions.assertTrue(metrics.getTotalConnectionsAccepted() >= 2,
                    "At least two connections must have been accepted");
            Assertions.assertTrue(metrics.getActiveConnections() >= 2,
                    "Both clients must currently count as active connections");

            // Publish + delivery counters moved.
            Assertions.assertTrue(metrics.getMessagesPublished() >= 1,
                    "messagesPublished must be > 0 after a publish");
            Assertions.assertTrue(metrics.getMessagesDelivered() >= 1,
                    "messagesDelivered must be > 0 after delivery to a subscriber");

            // Subscription gauge reflects the single active subscription.
            Assertions.assertEquals(1, metrics.getSubscriptionCount(),
                    "Exactly one subscription must be registered");
        }

        // After both clients disconnect the active-connection gauge drains back to zero.
        long deadline = System.nanoTime() + UNIT.toNanos(TIMEOUT);
        while (metrics.getActiveConnections() > 0 && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        Assertions.assertEquals(0, metrics.getActiveConnections(),
                "Active-connection gauge must drain to zero after all clients disconnect");
        Assertions.assertEquals(0, metrics.getSubscriptionCount(),
                "Subscription gauge must drain to zero after all subscribers disconnect");
    }
}
