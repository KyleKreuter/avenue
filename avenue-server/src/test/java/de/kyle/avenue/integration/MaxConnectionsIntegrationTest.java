package de.kyle.avenue.integration;

import de.kyle.avenue.SingleNodeServer;
import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.handler.client.BackpressurePolicy;
import de.kyle.avenue.metrics.AvenueMetrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Max-connections limit integration test (E16).
 * <p>
 * With {@code server.max-connections=2}, a third concurrent connection must be rejected and
 * closed immediately by the server, and the {@code connectionsRejected} metric must increase.
 */
class MaxConnectionsIntegrationTest {

    private static final String HOST = "127.0.0.1";
    private static final String SECRET = "max-secret";
    private static final String TOKEN = "max-token";
    private static final int PACKET_SIZE = 65536;

    private SingleNodeServer server;
    private int port;

    @BeforeEach
    void startServer() {
        AvenueConfig config = new AvenueConfig(
                PACKET_SIZE, true, SECRET, TOKEN, 0, 1024, 100,
                false, UUID.randomUUID().toString(), 0, List.of(), "", 5_000L,
                0L,                                  // idle-timeout off so held sockets stay open
                2,                                   // max-connections = 2
                BackpressurePolicy.DISCONNECT_SLOW_CONSUMER,
                0L,
                false, "", "", false, "", "", "", "");
        server = new SingleNodeServer(config);
        server.start();
        port = server.getPort();
        Assertions.assertTrue(port > 0);
    }

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void connection_over_limit_is_rejected_and_counted() throws Exception {
        AvenueMetrics metrics = server.getMetrics();

        // Two authenticated clients fill the limit and stay connected.
        try (TestClient c1 = new TestClient(HOST, port, PACKET_SIZE);
             TestClient c2 = new TestClient(HOST, port, PACKET_SIZE)) {

            c1.authenticate(SECRET, 5, TimeUnit.SECONDS);
            c2.authenticate(SECRET, 5, TimeUnit.SECONDS);

            // Wait until both connections are counted as active.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (metrics.getActiveConnections() < 2 && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
            Assertions.assertEquals(2, metrics.getActiveConnections(),
                    "Two connections must fill the limit");

            // The third connection: the server accepts the TCP socket but immediately closes it
            // because the limit is reached. The client therefore reads EOF.
            try (Socket third = new Socket(HOST, port)) {
                third.setSoTimeout(5_000);
                DataInputStream in = new DataInputStream(third.getInputStream());
                int firstByte;
                try {
                    firstByte = in.read();
                } catch (IOException e) {
                    firstByte = -1; // connection reset is an acceptable rejection signal
                }
                Assertions.assertEquals(-1, firstByte,
                        "Connection over the limit must be closed immediately (EOF)");
            }

            // The rejection must be recorded, and the active gauge must remain at the limit.
            long rejDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (metrics.getConnectionsRejected() < 1 && System.nanoTime() < rejDeadline) {
                Thread.sleep(20);
            }
            Assertions.assertTrue(metrics.getConnectionsRejected() >= 1,
                    "Rejected-connection counter must increase");
            Assertions.assertEquals(2, metrics.getActiveConnections(),
                    "Rejected connection must not inflate the active gauge");
        }
    }
}
