package de.kyle.avenue.integration;

import de.kyle.avenue.SingleNodeServer;
import de.kyle.avenue.config.AvenueConfig;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Integration coverage for the blocking transport's outbound swap buffers.
 * <p>
 * The ordinary pub/sub tests only ever push small frames, which all fit in the fill buffer and
 * therefore never exercise two paths that the swap-buffer design introduced:
 * <ul>
 *   <li><b>Oversized frames.</b> A framed message larger than one buffer
 *       ({@code server.outbound.ring-bytes}, 64 KiB by default) cannot be copied into the buffer at
 *       all and takes the overflow queue instead. If that path were broken, any payload above the
 *       buffer size would stall or be dropped — while the packet-size limit says it is legal.</li>
 *   <li><b>FIFO across both paths.</b> Small frames go through the buffer and oversized ones through
 *       the overflow queue. Ordering must hold <em>across</em> the two, otherwise a small message
 *       published after a large one could overtake it on the wire.</li>
 * </ul>
 * Both are asserted here against a real server over real sockets.
 */
class OutboundBufferIntegrationTest {

    private static final String HOST = "127.0.0.1";
    private static final String SECRET = "buffer-secret";
    private static final String TOKEN = "buffer-token";
    /** Well above the 64 KiB default outbound buffer, so oversized frames are actually legal. */
    private static final int PACKET_SIZE = 1024 * 1024;
    private static final long TIMEOUT = 10;
    private static final TimeUnit UNIT = TimeUnit.SECONDS;

    private SingleNodeServer server;
    private int port;

    @BeforeEach
    void startServer() {
        AvenueConfig config = new AvenueConfig(PACKET_SIZE, true, SECRET, TOKEN, 0, 1024, 100);
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

    /** A payload whose framed size exceeds the outbound buffer must still be delivered intact. */
    @Test
    void oversized_payload_is_delivered_through_the_overflow_path() throws Exception {
        // 200 KiB payload against a 64 KiB outbound buffer: cannot fit, must take the overflow path.
        String payload = "x".repeat(200 * 1024);

        try (TestClient subscriber = new TestClient(HOST, port, PACKET_SIZE);
             TestClient publisher = new TestClient(HOST, port, PACKET_SIZE)) {

            subscriber.authenticate(SECRET, TIMEOUT, UNIT);
            publisher.authenticate(SECRET, TIMEOUT, UNIT);
            subscriber.subscribe("big", TOKEN, TIMEOUT, UNIT);

            publisher.publish("big", payload, "pub", TOKEN);

            JSONObject received = subscriber.awaitPacket("PublishMessageOutboundPacket", TIMEOUT, UNIT);
            Assertions.assertNotNull(received, "oversized message must arrive");
            Assertions.assertEquals(payload, received.getJSONObject("body").getString("data"),
                    "oversized payload must arrive byte-identical");
        }
    }

    /**
     * Interleaves small and oversized messages. The small ones travel through the fill buffer, the
     * large ones through the overflow queue; the subscriber must still see them in publish order.
     */
    @Test
    void ordering_holds_across_buffered_and_oversized_frames() throws Exception {
        String big = "y".repeat(100 * 1024);
        List<String> published = new ArrayList<>();

        try (TestClient subscriber = new TestClient(HOST, port, PACKET_SIZE);
             TestClient publisher = new TestClient(HOST, port, PACKET_SIZE)) {

            subscriber.authenticate(SECRET, TIMEOUT, UNIT);
            publisher.authenticate(SECRET, TIMEOUT, UNIT);
            subscriber.subscribe("mixed", TOKEN, TIMEOUT, UNIT);

            for (int round = 0; round < 5; round++) {
                String small = "small-" + round;
                publisher.publish("mixed", small, "pub", TOKEN);
                published.add(small);

                String large = big + "-" + round;
                publisher.publish("mixed", large, "pub", TOKEN);
                published.add(large);
            }

            List<String> received = new ArrayList<>();
            for (int i = 0; i < published.size(); i++) {
                JSONObject packet = subscriber.awaitPacket("PublishMessageOutboundPacket", TIMEOUT, UNIT);
                Assertions.assertNotNull(packet, "message " + i + " must arrive");
                received.add(packet.getJSONObject("body").getString("data"));
            }

            Assertions.assertEquals(published, received,
                    "messages must arrive in publish order regardless of which path they took");
        }
    }

    /** Many small frames in a burst must all survive the buffer swap without loss or reordering. */
    @Test
    void burst_of_small_frames_survives_buffer_swaps() throws Exception {
        int messages = 2000;

        try (TestClient subscriber = new TestClient(HOST, port, PACKET_SIZE);
             TestClient publisher = new TestClient(HOST, port, PACKET_SIZE)) {

            subscriber.authenticate(SECRET, TIMEOUT, UNIT);
            publisher.authenticate(SECRET, TIMEOUT, UNIT);
            subscriber.subscribe("burst", TOKEN, TIMEOUT, UNIT);

            for (int i = 0; i < messages; i++) {
                publisher.publish("burst", "m-" + i, "pub", TOKEN);
            }

            for (int i = 0; i < messages; i++) {
                JSONObject packet = subscriber.awaitPacket("PublishMessageOutboundPacket", TIMEOUT, UNIT);
                Assertions.assertNotNull(packet, "message " + i + " must arrive");
                Assertions.assertEquals("m-" + i, packet.getJSONObject("body").getString("data"),
                        "burst messages must arrive in order");
            }
        }
    }
}
