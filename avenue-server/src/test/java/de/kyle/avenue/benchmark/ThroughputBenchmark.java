package de.kyle.avenue.benchmark;

import com.google.protobuf.ByteString;
import de.kyle.avenue.SingleNodeServer;
import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.proto.AuthTokenRequest;
import de.kyle.avenue.proto.ClientEnvelope;
import de.kyle.avenue.proto.PublishInbound;
import de.kyle.avenue.proto.Subscribe;
import de.kyle.avenue.serialization.PacketFraming;
import de.kyle.avenue.serialization.WireCodec;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Standalone throughput benchmark for the single-node pub-sub server. It is intentionally a
 * plain {@code main} class (NOT a {@code *Test}), so the Surefire include pattern
 * ({@code **}{@code /*Test.java}) never runs it during {@code mvn test} and it never lands in
 * the production jar (it lives under {@code src/test/java}).
 * <p>
 * Topology: 1 server, {@value #PUBLISHERS} publishers and {@value #SUBSCRIBERS} subscribers on a
 * single topic. Every published message fans out to every subscriber, so the measured
 * "delivered messages/s" already includes the fan-out factor.
 * <p>
 * How to run (from the repository root):
 * <pre>
 *   export JAVA_HOME=/path/to/jdk-21
 *   /opt/homebrew/bin/mvn -q -pl avenue-server test-compile
 *   /opt/homebrew/bin/mvn -q -pl avenue-server exec:java \
 *       -Dexec.classpathScope=test \
 *       -Dexec.mainClass=de.kyle.avenue.benchmark.ThroughputBenchmark
 * </pre>
 * Or directly with the test classpath:
 * <pre>
 *   java -cp "$(/opt/homebrew/bin/mvn -q -pl avenue-server dependency:build-classpath \
 *       -Dmdep.outputFile=/dev/stdout):avenue-server/target/classes:avenue-server/target/test-classes" \
 *       de.kyle.avenue.benchmark.ThroughputBenchmark
 * </pre>
 */
public final class ThroughputBenchmark {

    private static final String HOST = "127.0.0.1";
    private static final String SECRET = "bench-secret";
    private static final String TOKEN = "bench-token";
    private static final String TOPIC = "bench";
    private static final int PACKET_SIZE = 65536;

    private static final int PUBLISHERS = 4;
    private static final int SUBSCRIBERS = 4;
    private static final int MESSAGES_PER_PUBLISHER = 50_000;

    private ThroughputBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        AvenueConfig config = new AvenueConfig(
                PACKET_SIZE,
                false,            // never drop a slow publisher mid-benchmark
                SECRET,
                TOKEN,
                0,                // ephemeral port
                1_000_000,        // large outbound queue so we measure throughput, not backpressure drops
                1000
        );
        SingleNodeServer server = new SingleNodeServer(config);
        server.start();
        int port = server.getPort();
        System.out.printf("Server bound on port %d%n", port);

        long totalToDeliver = (long) PUBLISHERS * MESSAGES_PER_PUBLISHER * SUBSCRIBERS;
        AtomicLong delivered = new AtomicLong();
        CountDownLatch allDelivered = new CountDownLatch(1);

        // Subscribers: count every delivered message; signal when the expected total arrives.
        List<BenchSubscriber> subscribers = new ArrayList<>();
        for (int i = 0; i < SUBSCRIBERS; i++) {
            BenchSubscriber sub = new BenchSubscriber(port, delivered, totalToDeliver, allDelivered);
            sub.authenticateAndSubscribe();
            subscribers.add(sub);
        }

        // Publishers: each blasts MESSAGES_PER_PUBLISHER publishes on the topic.
        List<Socket> publisherSockets = new ArrayList<>();
        List<Thread> publisherThreads = new ArrayList<>();
        for (int p = 0; p < PUBLISHERS; p++) {
            Socket socket = new Socket(HOST, port);
            publisherSockets.add(socket);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());
            authenticate(out, in);
            final int publisherId = p;
            Thread t = new Thread(() -> {
                try {
                    for (int m = 0; m < MESSAGES_PER_PUBLISHER; m++) {
                        writeFrame(out, publishEnvelope(
                                TOPIC, "msg-" + m, "publisher-" + publisherId, TOKEN));
                    }
                } catch (IOException e) {
                    System.err.println("Publisher " + publisherId + " failed: " + e.getMessage());
                }
            }, "bench-publisher-" + p);
            publisherThreads.add(t);
        }

        System.out.printf("Publishing %d messages (%d publishers x %d), fan-out to %d subscribers -> %d deliveries%n",
                (long) PUBLISHERS * MESSAGES_PER_PUBLISHER, PUBLISHERS, MESSAGES_PER_PUBLISHER,
                SUBSCRIBERS, totalToDeliver);

        long start = System.nanoTime();
        publisherThreads.forEach(Thread::start);

        boolean completed = allDelivered.await(120, TimeUnit.SECONDS);
        long elapsedNanos = System.nanoTime() - start;

        for (Thread t : publisherThreads) {
            t.join(5_000);
        }

        long actuallyDelivered = delivered.get();
        double seconds = elapsedNanos / 1_000_000_000.0;
        double throughput = actuallyDelivered / seconds;

        System.out.println("==================================================");
        if (!completed) {
            System.out.printf("TIMEOUT: only %d / %d deliveries observed%n", actuallyDelivered, totalToDeliver);
        }
        System.out.printf("Delivered messages : %d%n", actuallyDelivered);
        System.out.printf("Elapsed time       : %.3f s%n", seconds);
        System.out.printf("Throughput         : %,.0f delivered msg/s%n", throughput);
        System.out.printf("Publish throughput : %,.0f published msg/s%n",
                ((double) PUBLISHERS * MESSAGES_PER_PUBLISHER) / seconds);
        System.out.println("==================================================");

        // Teardown.
        for (BenchSubscriber sub : subscribers) {
            sub.close();
        }
        for (Socket s : publisherSockets) {
            try {
                s.close();
            } catch (IOException ignored) {
                // best effort
            }
        }
        server.stop();
        System.exit(0);
    }

    private static void authenticate(DataOutputStream out, DataInputStream in) throws IOException {
        writeFrame(out, ClientEnvelope.newBuilder()
                .setAuthRequest(AuthTokenRequest.newBuilder().setSecret(SECRET).build())
                .build());
        // Consume the single auth-token response frame.
        PacketFraming.readFrame(in, PACKET_SIZE);
    }

    private static ClientEnvelope publishEnvelope(String topic, String data, String source, String token) {
        return ClientEnvelope.newBuilder()
                .setPublishInbound(PublishInbound.newBuilder()
                        .setTopic(topic).setData(ByteString.copyFromUtf8(data)).setSource(source).setToken(token).build())
                .build();
    }

    private static void writeFrame(DataOutputStream out, ClientEnvelope envelope) throws IOException {
        byte[] payload = WireCodec.encodeClient(envelope, PACKET_SIZE);
        synchronized (out) {
            PacketFraming.writeFrame(out, payload);
        }
    }

    /** A subscriber that drains delivered frames and counts them on its own daemon thread. */
    private static final class BenchSubscriber {
        private final Socket socket;
        private final DataOutputStream out;
        private final DataInputStream in;
        private final AtomicLong delivered;
        private final long totalToDeliver;
        private final CountDownLatch allDelivered;
        private volatile boolean running = true;

        BenchSubscriber(int port, AtomicLong delivered, long totalToDeliver, CountDownLatch allDelivered)
                throws IOException {
            this.socket = new Socket(HOST, port);
            this.out = new DataOutputStream(socket.getOutputStream());
            this.in = new DataInputStream(socket.getInputStream());
            this.delivered = delivered;
            this.totalToDeliver = totalToDeliver;
            this.allDelivered = allDelivered;
        }

        void authenticateAndSubscribe() throws IOException {
            authenticate(out, in);
            writeFrame(out, ClientEnvelope.newBuilder()
                    .setSubscribe(Subscribe.newBuilder().setTopic(TOPIC).setToken(TOKEN).build())
                    .build());
            Thread reader = new Thread(this::readLoop, "bench-subscriber-reader");
            reader.setDaemon(true);
            reader.start();
        }

        private void readLoop() {
            try {
                while (running) {
                    PacketFraming.readFrame(in, PACKET_SIZE);
                    long count = delivered.incrementAndGet();
                    if (count >= totalToDeliver) {
                        allDelivered.countDown();
                    }
                }
            } catch (IOException e) {
                // Expected on close.
            }
        }

        void close() {
            running = false;
            try {
                socket.close();
            } catch (IOException ignored) {
                // best effort
            }
        }
    }
}
