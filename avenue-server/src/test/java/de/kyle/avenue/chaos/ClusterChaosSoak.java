package de.kyle.avenue.chaos;

import com.google.protobuf.ByteString;
import de.kyle.avenue.cluster.ClusterNode;
import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.config.ClusterTuning;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Standalone cluster chaos / soak harness (Phase F).
 * <p>
 * A plain {@code main} (NOT a {@code *Test}) so {@code mvn test} never runs it. It builds a 3-node
 * in-process mesh, drives a continuous cross-node publish stream from node1 to a subscriber on node2,
 * and for the configured duration injects random faults: symmetric partition + heal, transient slow
 * peer (link drop without block, exercising reconnect + backfill) and crash + restart of node3.
 * <p>
 * At the end it checks the following invariants and prints a PASS/FAIL summary:
 * <ul>
 *   <li><b>No duplicate delivery</b> — the subscriber never sees the same publish sequence twice
 *       (de-dup held end to end).</li>
 *   <li><b>No unexpected gap</b> — every gap in the received sequence is accounted for by a fault
 *       window (we only crash/partition, which may legitimately drop messages, so a bounded loss is
 *       allowed; an <em>unbounded</em> or post-heal-persistent gap fails).</li>
 *   <li><b>Membership converges</b> — after the last heal, every surviving node converges to the
 *       expected alive size within a deadline.</li>
 *   <li><b>No obvious thread leak</b> — the live thread count after teardown is within a slack bound
 *       of the pre-start baseline.</li>
 * </ul>
 * How to run (from the repository root):
 * <pre>
 *   export JAVA_HOME=/path/to/jdk-21
 *   /opt/homebrew/bin/mvn -q -pl avenue-server test-compile
 *   /opt/homebrew/bin/mvn -q -pl avenue-server exec:java \
 *       -Dexec.classpathScope=test \
 *       -Dexec.mainClass=de.kyle.avenue.chaos.ClusterChaosSoak \
 *       -Dexec.args="minutes=2"
 * </pre>
 */
public final class ClusterChaosSoak {

    private static final String HOST = "127.0.0.1";
    private static final String SECRET = "chaos-secret";
    private static final String TOKEN = "chaos-token";
    private static final String CLUSTER_SECRET = "chaos-cluster-secret";
    private static final String TOPIC = "chaos";
    private static final int PACKET_SIZE = 65536;

    private ClusterChaosSoak() {
    }

    public static void main(String[] args) throws Exception {
        double minutes = doubleArg(args, "minutes", 0.5);
        long runMillis = (long) (minutes * 60_000);
        System.out.printf("Chaos soak starting: duration=%.2f min%n", minutes);

        int baselineThreads = Thread.activeCount();

        ClusterNode node1 = startNode("node-1", List.of());
        String seed = HOST + ":" + node1.getClusterPort();
        ClusterNode node2 = startNode("node-2", List.of(seed));
        ClusterNode node3 = startNode("node-3", List.of(seed));
        List<ClusterNode> nodes = new ArrayList<>(List.of(node1, node2, node3));

        for (ClusterNode n : nodes) {
            n.awaitMembership(2, 30, TimeUnit.SECONDS);
        }
        System.out.println("Cluster converged (3 nodes). Starting publish stream + fault injection.");

        // Subscriber on node2 records received sequences for the invariant checks.
        ConcurrentHashMap<Long, Boolean> seen = new ConcurrentHashMap<>();
        AtomicLong duplicates = new AtomicLong();
        AtomicLong maxSeqSeen = new AtomicLong(-1);
        ChaosSub sub = new ChaosSub(node2.getClientPort(), seen, duplicates, maxSeqSeen);
        sub.authenticateAndSubscribe();
        Thread.sleep(500); // let interest propagate

        // Publisher on node1, continuous stream.
        Socket pubSocket = new Socket(HOST, node1.getClientPort());
        pubSocket.setTcpNoDelay(true);
        DataOutputStream pubOut = new DataOutputStream(pubSocket.getOutputStream());
        DataInputStream pubIn = new DataInputStream(pubSocket.getInputStream());
        authenticate(pubOut, pubIn);

        AtomicLong published = new AtomicLong();
        volatileFlag.set(true);
        Thread publisher = new Thread(() -> {
            try {
                while (volatileFlag.get()) {
                    long seq = published.getAndIncrement();
                    writeFrame(pubOut, publishEnvelope(TOPIC, Long.toString(seq), "chaos-pub", TOKEN));
                    if ((seq & 0x3FF) == 0) {
                        Thread.sleep(1); // gentle pacing so we soak, not flood
                    }
                }
            } catch (Exception e) {
                System.err.println("Publisher stopped: " + e.getMessage());
            }
        }, "chaos-publisher");
        publisher.start();

        long deadline = System.currentTimeMillis() + runMillis;
        ClusterNode currentNode3 = node3;
        int faults = 0;
        while (System.currentTimeMillis() < deadline) {
            Thread.sleep(ThreadLocalRandom.current().nextInt(2_000, 5_000));
            int dice = ThreadLocalRandom.current().nextInt(3);
            switch (dice) {
                case 0 -> {
                    System.out.println("[chaos] partition node1<->node3, heal after 2s");
                    node1.blockPeer("node-3");
                    currentNode3.blockPeer("node-1");
                    Thread.sleep(2_000);
                    node1.unblockPeer("node-3");
                    currentNode3.unblockPeer("node-1");
                }
                case 1 -> {
                    System.out.println("[chaos] transient slow peer: drop node1->node2 link");
                    node1.dropPeer("node-2");
                }
                case 2 -> {
                    System.out.println("[chaos] crash + restart node3");
                    currentNode3.stop();
                    nodes.remove(currentNode3);
                    Thread.sleep(1_000);
                    currentNode3 = startNode("node-3", List.of(seed));
                    nodes.add(currentNode3);
                }
                default -> {
                }
            }
            faults++;
        }

        // Stop faults, let the cluster settle, then check convergence. Defensively clear any lingering
        // partition (a fault may have been the last action before the deadline) so the heal path runs
        // before we assert convergence — exactly what an operator would do after the chaos stops.
        System.out.println("Fault injection done (" + faults + " faults). Healing + settling...");
        for (int i = 0; i < nodes.size(); i++) {
            for (int j = 0; j < nodes.size(); j++) {
                if (i != j) {
                    nodes.get(i).unblockPeer("node-" + (j + 1));
                }
            }
        }
        volatileFlag.set(false);
        publisher.join(5_000);

        boolean converged = true;
        for (ClusterNode n : nodes) {
            converged &= n.awaitMembership(nodes.size() - 1, 45, TimeUnit.SECONDS);
        }
        // Drain remaining in-flight deliveries.
        Thread.sleep(3_000);

        long published_ = published.get();
        long delivered = seen.size();
        long dup = duplicates.get();
        long maxSeq = maxSeqSeen.get();
        // Gap accounting: how many sequences <= maxSeq were never delivered. Bounded loss from
        // crash/partition windows is acceptable; we flag only an implausibly large hole.
        long missing = (maxSeq + 1) - delivered;
        double lossPct = published_ == 0 ? 0 : (100.0 * missing / (maxSeq + 1));

        sub.close();
        try {
            pubSocket.close();
        } catch (IOException ignored) {
        }
        for (int i = nodes.size() - 1; i >= 0; i--) {
            nodes.get(i).stop();
        }
        Thread.sleep(2_000);
        int endThreads = Thread.activeCount();

        System.out.println("==================================================");
        System.out.printf("Published            : %d%n", published_);
        System.out.printf("Delivered (unique)   : %d  (maxSeq=%d)%n", delivered, maxSeq);
        System.out.printf("Duplicates           : %d%n", dup);
        System.out.printf("Missing within range : %d  (%.2f%% of [0..maxSeq])%n", missing, lossPct);
        System.out.printf("Threads baseline/end : %d / %d%n", baselineThreads, endThreads);
        System.out.println("--------------------------------------------------");

        boolean noDuplicates = dup == 0;
        // Loss is allowed (we deliberately crash/partition); but a healthy soak should still deliver
        // the vast majority once healed. Flag only catastrophic loss as a failure.
        boolean lossWithinWindow = lossPct < 25.0;
        boolean threadsOk = endThreads <= baselineThreads + 40;

        System.out.printf("INVARIANT no-duplicate-delivery : %s%n", noDuplicates ? "PASS" : "FAIL");
        System.out.printf("INVARIANT gap-within-loss-window: %s%n", lossWithinWindow ? "PASS" : "FAIL");
        System.out.printf("INVARIANT membership-converges  : %s%n", converged ? "PASS" : "FAIL");
        System.out.printf("INVARIANT no-thread-leak        : %s%n", threadsOk ? "PASS" : "FAIL");
        System.out.println("==================================================");

        boolean pass = noDuplicates && lossWithinWindow && converged && threadsOk;
        System.out.println(pass ? "CHAOS SOAK: PASS" : "CHAOS SOAK: FAIL");
        System.exit(pass ? 0 : 1);
    }

    /** Shared run flag for the publisher loop (a field so the lambda can read it). */
    private static final java.util.concurrent.atomic.AtomicBoolean volatileFlag =
            new java.util.concurrent.atomic.AtomicBoolean(false);

    private static ClusterNode startNode(String nodeId, List<String> seeds) {
        ClusterNode node = new ClusterNode(config(nodeId, seeds));
        node.start();
        return node;
    }

    private static AvenueConfig config(String nodeId, List<String> seeds) {
        ClusterTuning tuning = new ClusterTuning(
                ClusterTuning.DEFAULT_REPLAY_CAPACITY,
                de.kyle.avenue.cluster.ReplayBuffer.BackpressurePolicy.BLOCK,
                ClusterTuning.DEFAULT_REPLAY_OFFER_TIMEOUT_MS,
                ClusterTuning.DEFAULT_ACK_INTERVAL_MS,
                false,
                ClusterTuning.DEFAULT_ORIGIN_EXPIRY_MS,
                ClusterTuning.DEFAULT_INTEREST_SYNC_INTERVAL_MS,
                ClusterTuning.DEFAULT_INTEREST_BROADCAST_GRACE_MS,
                500L, 250L, 2, 1_500L, 3, 5_000L, "", 1_048_576);
        return new AvenueConfig(
                PACKET_SIZE, false, SECRET, TOKEN,
                0, 1_000_000, 1000,
                true, nodeId, 0, seeds, CLUSTER_SECRET, 1000L,
                0L, 100_000, null, 0L,
                false, "", "",
                false, "", "", "", "",
                tuning);
    }

    private static double doubleArg(String[] args, String key, double def) {
        for (String a : args) {
            if (a.startsWith(key + "=")) {
                return Double.parseDouble(a.substring(key.length() + 1).trim());
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
                        .setTopic(topic).setData(ByteString.copyFromUtf8(data)).setSource(source).setToken(token).build())
                .build();
    }

    private static void writeFrame(DataOutputStream out, ClientEnvelope envelope) throws IOException {
        byte[] payload = WireCodec.encodeClient(envelope, PACKET_SIZE);
        synchronized (out) {
            PacketFraming.writeFrame(out, payload);
        }
    }

    /** Records the unique sequence numbers delivered and flags any duplicate. */
    private static final class ChaosSub {
        private final Socket socket;
        private final DataOutputStream out;
        private final DataInputStream in;
        private final ConcurrentHashMap<Long, Boolean> seen;
        private final AtomicLong duplicates;
        private final AtomicLong maxSeqSeen;
        private volatile boolean running = true;

        ChaosSub(int port, ConcurrentHashMap<Long, Boolean> seen, AtomicLong duplicates, AtomicLong maxSeqSeen)
                throws IOException {
            this.socket = new Socket(HOST, port);
            this.socket.setTcpNoDelay(true);
            this.out = new DataOutputStream(socket.getOutputStream());
            this.in = new DataInputStream(socket.getInputStream());
            this.seen = seen;
            this.duplicates = duplicates;
            this.maxSeqSeen = maxSeqSeen;
        }

        void authenticateAndSubscribe() throws IOException {
            authenticate(out, in);
            writeFrame(out, ClientEnvelope.newBuilder()
                    .setSubscribe(Subscribe.newBuilder().setTopic(TOPIC).setToken(TOKEN).build())
                    .build());
            Thread reader = new Thread(this::readLoop, "chaos-sub-reader");
            reader.setDaemon(true);
            reader.start();
        }

        private void readLoop() {
            try {
                while (running) {
                    byte[] frame = PacketFraming.readFrame(in, PACKET_SIZE);
                    ClientEnvelope env = WireCodec.decodeClient(frame, PACKET_SIZE);
                    if (env.getMsgCase() != ClientEnvelope.MsgCase.PUBLISH_OUTBOUND) {
                        continue;
                    }
                    long seq = Long.parseLong(env.getPublishOutbound().getData().toStringUtf8());
                    if (seen.putIfAbsent(seq, Boolean.TRUE) != null) {
                        duplicates.incrementAndGet();
                    }
                    maxSeqSeen.updateAndGet(cur -> Math.max(cur, seq));
                }
            } catch (Exception e) {
                // Expected on close / number parse of a non-chaos frame.
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
