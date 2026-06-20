package de.kyle.avenue.benchmark;

import de.kyle.avenue.cluster.ClusterNode;
import de.kyle.avenue.cluster.ReplayBuffer;
import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.config.ClusterTuning;
import de.kyle.avenue.proto.AuthTokenRequest;
import de.kyle.avenue.proto.ClientEnvelope;
import de.kyle.avenue.proto.PublishInbound;
import de.kyle.avenue.proto.PublishOutbound;
import de.kyle.avenue.proto.Subscribe;
import de.kyle.avenue.serialization.PacketFraming;
import de.kyle.avenue.serialization.WireCodec;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Standalone <b>cross-node</b> cluster throughput + latency benchmark (Phase F).
 * <p>
 * Like {@link ThroughputBenchmark} it is a plain {@code main} (NOT a {@code *Test}), so the Surefire
 * include pattern ({@code **}{@code /*Test.java}) never runs it during {@code mvn test} and it never
 * lands in the production jar.
 * <p>
 * Topology: {@value #NODES} in-process {@link ClusterNode}s wired into a full mesh via SWIM seeds. A
 * publisher connects to node1 and a subscriber to node2 (and node3 if present), so every measured
 * delivery crosses a cluster link. Each published payload embeds the publish timestamp (nanos); the
 * subscriber computes the end-to-end latency on receipt and the benchmark reports cross-node
 * throughput (delivered msg/s) and p50/p99 latency from a sorted long array.
 * <p>
 * CLI flags (all optional, {@code key=value}):
 * <pre>
 *   messages=200000      total messages the publisher sends
 *   nodes=2              number of cluster nodes (2 or 3)
 *   nodelay=true|false   TCP_NODELAY on the benchmark client sockets (server side is already nodelay)
 * </pre>
 * How to run (from the repository root):
 * <pre>
 *   export JAVA_HOME=/path/to/jdk-21
 *   /opt/homebrew/bin/mvn -q -pl avenue-server test-compile
 *   /opt/homebrew/bin/mvn -q -pl avenue-server exec:java \
 *       -Dexec.classpathScope=test \
 *       -Dexec.mainClass=de.kyle.avenue.benchmark.ClusterThroughputBenchmark \
 *       -Dexec.args="messages=200000 nodes=3 nodelay=true"
 * </pre>
 */
public final class ClusterThroughputBenchmark {

    private static final String HOST = "127.0.0.1";
    private static final String SECRET = "bench-secret";
    private static final String TOKEN = "bench-token";
    private static final String CLUSTER_SECRET = "bench-cluster-secret";
    private static final String TOPIC = "bench-cross";
    private static final int PACKET_SIZE = 65536;
    private static final int NODES = 2;

    private ClusterThroughputBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        long messages = longArg(args, "messages", 200_000L);
        int nodeCount = (int) longArg(args, "nodes", NODES);
        boolean nodelay = boolArg(args, "nodelay", true);
        nodeCount = Math.max(2, Math.min(3, nodeCount));

        System.out.printf("Cluster benchmark: nodes=%d messages=%d nodelay=%s%n",
                nodeCount, messages, nodelay);

        List<ClusterNode> nodes = startMesh(nodeCount);
        ClusterNode node1 = nodes.get(0);
        ClusterNode node2 = nodes.get(1);

        // Wait for the mesh to converge so the publish is actually routed cross-node.
        for (ClusterNode n : nodes) {
            if (!n.awaitMembership(nodeCount - 1, 30, TimeUnit.SECONDS)) {
                System.err.println("WARNING: cluster did not fully converge before benchmark");
            }
        }

        // Subscriber(s) on node2 (and node3). Each delivery crosses a cluster link.
        long totalToDeliver = messages * (nodeCount - 1);
        AtomicLong delivered = new AtomicLong();
        long[] latenciesNanos = new long[(int) Math.min(totalToDeliver, 2_000_000)];
        AtomicLong latIndex = new AtomicLong();
        CountDownLatch done = new CountDownLatch(1);

        List<BenchSub> subs = new ArrayList<>();
        for (int i = 1; i < nodeCount; i++) {
            BenchSub sub = new BenchSub(nodes.get(i).getClientPort(), nodelay,
                    delivered, totalToDeliver, latenciesNanos, latIndex, done);
            sub.authenticateAndSubscribe();
            subs.add(sub);
        }

        // Give interest propagation a brief, polled head-start (publisher must know node2's interest).
        Thread.sleep(500);

        Socket pubSocket = new Socket(HOST, node1.getClientPort());
        pubSocket.setTcpNoDelay(nodelay);
        DataOutputStream pubOut = new DataOutputStream(pubSocket.getOutputStream());
        DataInputStream pubIn = new DataInputStream(pubSocket.getInputStream());
        authenticate(pubOut, pubIn);

        System.out.printf("Publishing %d messages from node1, %d subscriber(s) cross-node -> %d deliveries%n",
                messages, nodeCount - 1, totalToDeliver);

        long start = System.nanoTime();
        for (long m = 0; m < messages; m++) {
            String payload = System.nanoTime() + ":" + m;
            writeFrame(pubOut, publishEnvelope(TOPIC, payload, "bench-pub", TOKEN));
        }

        boolean completed = done.await(180, TimeUnit.SECONDS);
        long elapsedNanos = System.nanoTime() - start;

        long actuallyDelivered = delivered.get();
        double seconds = elapsedNanos / 1_000_000_000.0;
        double throughput = actuallyDelivered / seconds;

        int n = (int) Math.min(latIndex.get(), latenciesNanos.length);
        long[] lat = Arrays.copyOf(latenciesNanos, n);
        Arrays.sort(lat);

        System.out.println("==================================================");
        if (!completed) {
            System.out.printf("TIMEOUT: only %d / %d cross-node deliveries observed%n",
                    actuallyDelivered, totalToDeliver);
        }
        System.out.printf("Cross-node delivered : %d%n", actuallyDelivered);
        System.out.printf("Elapsed time         : %.3f s%n", seconds);
        System.out.printf("Throughput           : %,.0f delivered msg/s (cross-node)%n", throughput);
        if (n > 0) {
            System.out.printf("Latency p50          : %.3f ms%n", percentile(lat, 50) / 1_000_000.0);
            System.out.printf("Latency p99          : %.3f ms%n", percentile(lat, 99) / 1_000_000.0);
            System.out.printf("Latency max          : %.3f ms%n", lat[lat.length - 1] / 1_000_000.0);
        }
        System.out.println("==================================================");

        for (BenchSub s : subs) {
            s.close();
        }
        try {
            pubSocket.close();
        } catch (IOException ignored) {
        }
        for (int i = nodes.size() - 1; i >= 0; i--) {
            nodes.get(i).stop();
        }
        System.exit(0);
    }

    private static List<ClusterNode> startMesh(int count) {
        List<ClusterNode> nodes = new ArrayList<>();
        ClusterNode first = new ClusterNode(config("node-1", List.of()));
        first.start();
        nodes.add(first);
        String seed = HOST + ":" + first.getClusterPort();
        for (int i = 2; i <= count; i++) {
            ClusterNode node = new ClusterNode(config("node-" + i, List.of(seed)));
            node.start();
            nodes.add(node);
        }
        return nodes;
    }

    private static AvenueConfig config(String nodeId, List<String> seeds) {
        return new AvenueConfig(
                PACKET_SIZE, false, SECRET, TOKEN,
                0, 1_000_000, 1000,
                true, nodeId, 0, seeds, CLUSTER_SECRET, 1000L,
                0L, 100_000, null, 0L,
                false, "", "",
                false, "", "", "", "",
                ClusterTuning.defaults());
    }

    private static long percentile(long[] sorted, int p) {
        if (sorted.length == 0) {
            return 0;
        }
        int idx = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
        idx = Math.max(0, Math.min(sorted.length - 1, idx));
        return sorted[idx];
    }

    private static long longArg(String[] args, String key, long def) {
        for (String a : args) {
            if (a.startsWith(key + "=")) {
                return Long.parseLong(a.substring(key.length() + 1).trim());
            }
        }
        return def;
    }

    private static boolean boolArg(String[] args, String key, boolean def) {
        for (String a : args) {
            if (a.startsWith(key + "=")) {
                return Boolean.parseBoolean(a.substring(key.length() + 1).trim());
            }
        }
        return def;
    }

    private static void authenticate(DataOutputStream out, DataInputStream in) throws IOException {
        writeFrame(out, ClientEnvelope.newBuilder()
                .setAuthRequest(AuthTokenRequest.newBuilder().setSecret(SECRET).build())
                .build());
        PacketFraming.readFrame(in, PACKET_SIZE);
    }

    private static ClientEnvelope publishEnvelope(String topic, String data, String source, String token) {
        return ClientEnvelope.newBuilder()
                .setPublishInbound(PublishInbound.newBuilder()
                        .setTopic(topic).setData(data).setSource(source).setToken(token).build())
                .build();
    }

    private static void writeFrame(DataOutputStream out, ClientEnvelope envelope) throws IOException {
        byte[] payload = WireCodec.encodeClient(envelope, PACKET_SIZE);
        synchronized (out) {
            PacketFraming.writeFrame(out, payload);
        }
    }

    /** A cross-node subscriber that records per-message end-to-end latency from the embedded timestamp. */
    private static final class BenchSub {
        private final Socket socket;
        private final DataOutputStream out;
        private final DataInputStream in;
        private final AtomicLong delivered;
        private final long totalToDeliver;
        private final long[] latencies;
        private final AtomicLong latIndex;
        private final CountDownLatch done;
        private volatile boolean running = true;

        BenchSub(int port, boolean nodelay, AtomicLong delivered, long totalToDeliver,
                 long[] latencies, AtomicLong latIndex, CountDownLatch done) throws IOException {
            this.socket = new Socket(HOST, port);
            this.socket.setTcpNoDelay(nodelay);
            this.out = new DataOutputStream(socket.getOutputStream());
            this.in = new DataInputStream(socket.getInputStream());
            this.delivered = delivered;
            this.totalToDeliver = totalToDeliver;
            this.latencies = latencies;
            this.latIndex = latIndex;
            this.done = done;
        }

        void authenticateAndSubscribe() throws IOException {
            authenticate(out, in);
            writeFrame(out, ClientEnvelope.newBuilder()
                    .setSubscribe(Subscribe.newBuilder().setTopic(TOPIC).setToken(TOKEN).build())
                    .build());
            Thread reader = new Thread(this::readLoop, "bench-cluster-sub-reader");
            reader.setDaemon(true);
            reader.start();
        }

        private void readLoop() {
            try {
                while (running) {
                    byte[] frame = PacketFraming.readFrame(in, PACKET_SIZE);
                    long recvNanos = System.nanoTime();
                    ClientEnvelope env = WireCodec.decodeClient(frame, PACKET_SIZE);
                    if (env.getMsgCase() != ClientEnvelope.MsgCase.PUBLISH_OUTBOUND) {
                        continue;
                    }
                    PublishOutbound publish = env.getPublishOutbound();
                    String data = publish.getData();
                    int colon = data.indexOf(':');
                    if (colon > 0) {
                        try {
                            long sent = Long.parseLong(data.substring(0, colon));
                            int idx = (int) latIndex.getAndIncrement();
                            if (idx < latencies.length) {
                                latencies[idx] = recvNanos - sent;
                            }
                        } catch (NumberFormatException ignored) {
                        }
                    }
                    long count = delivered.incrementAndGet();
                    if (count >= totalToDeliver) {
                        done.countDown();
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
            }
        }
    }
}
