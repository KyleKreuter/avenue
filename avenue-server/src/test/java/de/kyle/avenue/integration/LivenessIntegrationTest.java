package de.kyle.avenue.integration;

import de.kyle.avenue.SingleNodeServer;
import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.handler.client.BackpressurePolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Liveness / idle-timeout integration test (E16).
 * <p>
 * Starts a server with a short {@code server.client.idle-timeout-ms}. A client that connects
 * but never sends a byte must be reaped: the server-side read times out and closes the socket,
 * which the client observes as end-of-stream on its input.
 */
class LivenessIntegrationTest {

    private static final String HOST = "127.0.0.1";
    private static final String SECRET = "live-secret";
    private static final String TOKEN = "live-token";
    private static final int PACKET_SIZE = 65536;

    private SingleNodeServer server;

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop();
        }
    }

    private AvenueConfig configWithIdleTimeout(long idleTimeoutMs) {
        return new AvenueConfig(
                PACKET_SIZE, true, SECRET, TOKEN, 0, 1024, 100,
                false, UUID.randomUUID().toString(), 0, List.of(), "", 5_000L,
                idleTimeoutMs,                       // client idle-timeout
                10_000,
                BackpressurePolicy.DISCONNECT_SLOW_CONSUMER,
                0L,
                false, "", "", false, "", "", "", "");
    }

    @Test
    void idle_connection_is_closed_after_idle_timeout() throws Exception {
        // 300 ms idle-timeout: a silent client must be dropped quickly.
        server = new SingleNodeServer(configWithIdleTimeout(300));
        server.start();
        int port = server.getPort();
        Assertions.assertTrue(port > 0);

        try (Socket socket = new Socket(HOST, port)) {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            // We never send anything. The server must close the connection after the idle
            // window; the client then reads -1 (EOF). Bound the wait generously.
            socket.setSoTimeout(5_000);
            int firstByte;
            try {
                firstByte = in.read();
            } catch (IOException e) {
                // A reset is also an acceptable signal that the server dropped the connection.
                firstByte = -1;
            }
            Assertions.assertEquals(-1, firstByte,
                    "Server must close the idle connection (client observes EOF)");
        }
    }

    @Test
    void client_that_keeps_sending_is_not_dropped_by_idle_timeout() throws Exception {
        // The idle-timeout is a server-side SOCKET-READ timeout: it reaps a connection only if
        // no INBOUND bytes arrive within the window. A client that keeps sending (publishing)
        // resets the read timer on every frame and must therefore stay connected across a span
        // longer than the idle-timeout. (A purely-receiving subscriber that never sends is, by
        // design, subject to the read-idle cutoff — see docs/security.md.)
        server = new SingleNodeServer(configWithIdleTimeout(1_000));
        server.start();
        int port = server.getPort();

        try (TestClient subscriber = new TestClient(HOST, port, PACKET_SIZE);
             TestClient publisher = new TestClient(HOST, port, PACKET_SIZE)) {

            subscriber.authenticate(SECRET, 5, TimeUnit.SECONDS);
            publisher.authenticate(SECRET, 5, TimeUnit.SECONDS);
            subscriber.subscribe("alive", TOKEN, 5, TimeUnit.SECONDS);

            // Publisher keeps sending across a span (3 x 500ms = 1.5s) longer than the 1s
            // idle-timeout. Each publish keeps the publisher's read timer fresh; the subscriber
            // (which also re-sends a subscribe to stay active) must keep receiving.
            for (int i = 0; i < 3; i++) {
                subscriber.subscribe("alive", TOKEN); // inbound byte from the subscriber -> resets its timer
                publisher.publish("alive", "msg-" + i, "pub", TOKEN);
                Assertions.assertNotNull(
                        subscriber.awaitPacket("PublishMessageOutboundPacket", 5, TimeUnit.SECONDS),
                        "Active subscriber must keep receiving and must not be idle-reaped");
                Thread.sleep(500); // < idle-timeout per gap, accumulates beyond it across the loop
            }
        }
    }
}
