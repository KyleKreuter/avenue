package de.kyle.avenue.benchmark;

import de.kyle.avenue.SingleNodeServer;
import de.kyle.avenue.config.AvenueConfig;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Load-driven benchmark harness for the single-node pub/sub server. It is a plain {@code main}
 * (NOT a {@code *Test}), so the Surefire include pattern ({@code **}{@code /*Test.java}) never runs
 * it during {@code mvn test} and it never lands in the production jar (it lives under
 * {@code src/test/java}).
 *
 * <h2>Why this exists next to {@link ThroughputBenchmark}</h2>
 * The older benchmarks conflate <em>publish</em> rate and <em>delivery</em> rate into one number,
 * have no warm-up / steady-state separation, and only ever measure latency under full saturation
 * (the publisher fires unbounded, so the reported p99 is queueing latency, not service latency).
 * This harness fixes all three:
 * <ul>
 *   <li><b>Separate rates.</b> It reports the accepted <em>publish</em> rate (publishes that made it
 *       onto the wire, aggregated over all publishers) and the <em>delivery</em> rate (frames
 *       received across all subscribers, i.e. already multiplied by the fan-out) as two distinct
 *       figures.</li>
 *   <li><b>Warm-up.</b> A dedicated warm-up phase (default 5 s) runs first and its results are
 *       discarded, so the JIT is fully warmed before the steady-state measurement window.</li>
 *   <li><b>Fixed-load latency.</b> In latency mode each publisher paces itself to a fixed target
 *       rate (nanosecond pacing), so the end-to-end latency is measured away from saturation and is
 *       not inflated by queueing.</li>
 * </ul>
 *
 * <h2>Two modes</h2>
 * <ul>
 *   <li><b>Throughput mode</b> ({@code rate=0}, default): publishers blast as fast as they can for
 *       {@code seconds}. The harness reports publish rate and delivery rate.</li>
 *   <li><b>Latency mode</b> ({@code rate>0}): each publisher publishes at a fixed {@code rate} msg/s.
 *       Every payload embeds its publish timestamp; subscribers compute the end-to-end latency on
 *       receipt and the harness reports p50/p99/p999/max from a sorted long array.</li>
 * </ul>
 *
 * <h2>CLI flags</h2>
 * All optional, {@code key=value} form:
 * <pre>
 *   publishers=4         number of parallel publisher connections
 *   subscribers=4        subscribers per topic (= fan-out)
 *   topics=1             number of topics
 *   msgSize=64           payload size in bytes
 *   msgSizes=64,256,1024 sweep over several payload sizes (overrides msgSize)
 *   seconds=10           steady-state measurement duration per point
 *   warmupSeconds=5      warm-up duration per point (results discarded)
 *   rate=0               per-publisher target publish rate; 0 = unbounded (throughput mode)
 * </pre>
 *
 * <h2>How to run</h2>
 * From the repository root:
 * <pre>
 *   export JAVA_HOME=/path/to/jdk-21
 *   /opt/homebrew/bin/mvn -q -pl avenue-server test-compile
 *
 *   # (a) Throughput sweep over payload sizes:
 *   /opt/homebrew/bin/mvn -q -pl avenue-server exec:java \
 *       -Dexec.classpathScope=test \
 *       -Dexec.mainClass=de.kyle.avenue.benchmark.LoadHarness \
 *       -Dexec.args="publishers=4 subscribers=4 msgSizes=64,256,1024 seconds=10"
 *
 *   # (b) Fixed-load latency at 50k msg/s per publisher:
 *   /opt/homebrew/bin/mvn -q -pl avenue-server exec:java \
 *       -Dexec.classpathScope=test \
 *       -Dexec.mainClass=de.kyle.avenue.benchmark.LoadHarness \
 *       -Dexec.args="publishers=4 subscribers=4 msgSize=64 rate=50000 seconds=10"
 * </pre>
 *
 * <h2>Determinism</h2>
 * Readiness is established without sleeps: each subscriber blocks on its {@code SubscribeAck}
 * (via a per-topic latch) before any publisher starts, so a publish can never race ahead of a
 * subscription. Only the measurement windows themselves use timed sleeps.
 */
public final class LoadHarness {

    private static final String HOST = "127.0.0.1";
    private static final String SECRET = "bench-secret";
    private static final String TOKEN = "bench-token";
    private static final int PACKET_SIZE = 1 << 20; // 1 MiB, large enough for any swept payload

    private LoadHarness() {
    }

    public static void main(String[] args) throws Exception {
        int publishers = (int) longArg(args, "publishers", 4);
        int subscribers = (int) longArg(args, "subscribers", 4);
        int topics = (int) longArg(args, "topics", 1);
        int seconds = (int) longArg(args, "seconds", 10);
        int warmupSeconds = (int) longArg(args, "warmupSeconds", 5);
        long rate = longArg(args, "rate", 0);
        int[] msgSizes = msgSizesArg(args);

        boolean latencyMode = rate > 0;

        System.out.printf(Locale.ROOT,
                "LoadHarness: publishers=%d subscribers=%d topics=%d seconds=%d warmupSeconds=%d rate=%d mode=%s%n",
                publishers, subscribers, topics, seconds, warmupSeconds, rate,
                latencyMode ? "LATENCY" : "THROUGHPUT");

        List<Result> results = new ArrayList<>();
        for (int msgSize : msgSizes) {
            results.add(runPoint(publishers, subscribers, topics, msgSize, seconds, warmupSeconds, rate));
        }

        printResultTable(results, latencyMode);
        System.exit(0);
    }

    /**
     * Runs one full measurement point (one payload size): boots a fresh server, wires the topology,
     * runs warm-up then the steady-state window, and tears everything down. A fresh server per point
     * keeps the points independent (no leftover queue state bleeds between sizes).
     */
    private static Result runPoint(int publishers, int subscribers, int topics, int msgSize,
                                   int seconds, int warmupSeconds, long rate) throws Exception {
        boolean latencyMode = rate > 0;

        AvenueConfig config = new AvenueConfig(
                PACKET_SIZE,
                false,            // never drop a slow consumer mid-benchmark
                SECRET,
                TOKEN,
                0,                // ephemeral port
                1_000_000,        // large outbound queue: measure the path, not backpressure drops
                1000
        );
        SingleNodeServer server = new SingleNodeServer(config);
        server.start();
        int port = server.getPort();

        String[] topicNames = new String[topics];
        for (int t = 0; t < topics; t++) {
            topicNames[t] = "bench-" + t;
        }

        // Shared counters. deliveredTotal counts every received frame across all subscribers
        // (already includes fan-out). Latencies are only recorded while 'recording' is true.
        LongAdder deliveredTotal = new LongAdder();
        LatencyRecorder latency = new LatencyRecorder(latencyMode ? 4_000_000 : 0);
        FlagBox recording = new FlagBox();

        // Subscribers: 'subscribers' per topic. Each blocks on its SubscribeAck before we proceed,
        // so the whole topology is provably ready before any publisher fires (no readiness sleeps).
        List<Sub> subs = new ArrayList<>();
        for (String topic : topicNames) {
            for (int i = 0; i < subscribers; i++) {
                Sub sub = new Sub(port, topic, deliveredTotal, latency, recording);
                sub.connectAndSubscribe();
                subs.add(sub);
            }
        }

        // Publishers: each is pinned to one topic (round-robin) and runs on its own thread.
        LongAdder publishedTotal = new LongAdder();
        List<Pub> pubs = new ArrayList<>();
        // Start barrier: publishers build their connection up front, then all begin together.
        CountDownLatch goLatch = new CountDownLatch(1);
        CountDownLatch readyLatch = new CountDownLatch(publishers);
        for (int p = 0; p < publishers; p++) {
            String topic = topicNames[p % topics];
            Pub pub = new Pub(port, topic, msgSize, rate, publishedTotal, latencyMode,
                    goLatch, readyLatch);
            pub.connect();
            pubs.add(pub);
        }

        for (Pub pub : pubs) {
            pub.start();
        }
        readyLatch.await(); // all publisher threads have authenticated and are parked on goLatch

        // ---- Warm-up phase: run the real load, then discard the counters. ----
        recording.value = false;
        goLatch.countDown();
        if (warmupSeconds > 0) {
            sleepSeconds(warmupSeconds);
        }

        // ---- Steady-state window: snapshot, sleep, snapshot. ----
        long publishedBefore = publishedTotal.sum();
        long deliveredBefore = deliveredTotal.sum();
        latency.reset();
        recording.value = true;
        long startNanos = System.nanoTime();
        sleepSeconds(seconds);
        long elapsedNanos = System.nanoTime() - startNanos;
        recording.value = false;

        long publishedDelta = publishedTotal.sum() - publishedBefore;
        long deliveredDelta = deliveredTotal.sum() - deliveredBefore;

        // Stop publishers and let in-flight deliveries drain briefly so the rates line up.
        for (Pub pub : pubs) {
            pub.stop();
        }

        double secs = elapsedNanos / 1_000_000_000.0;
        double publishRate = publishedDelta / secs;
        double deliveryRate = deliveredDelta / secs;

        long[] sortedLat = latency.snapshotSorted();
        double p50 = percentileMillis(sortedLat, 50.0);
        double p99 = percentileMillis(sortedLat, 99.0);
        double p999 = percentileMillis(sortedLat, 99.9);
        double max = sortedLat.length > 0 ? sortedLat[sortedLat.length - 1] / 1_000_000.0 : Double.NaN;

        Result result = new Result(publishers, subscribers, topics, msgSize,
                publishRate, deliveryRate, p50, p99, p999, max, sortedLat.length);

        System.out.printf(Locale.ROOT,
                "  point msgSize=%d -> publish=%.0f/s delivery=%.0f/s",
                msgSize, publishRate, deliveryRate);
        if (latencyMode) {
            System.out.printf(Locale.ROOT, " p50=%.3fms p99=%.3fms p999=%.3fms max=%.3fms (n=%d)",
                    p50, p99, p999, max, sortedLat.length);
        }
        System.out.println();

        // Teardown.
        for (Sub s : subs) {
            s.close();
        }
        for (Pub p : pubs) {
            p.close();
        }
        server.stop();

        return result;
    }

    private static void printResultTable(List<Result> results, boolean latencyMode) {
        System.out.println();
        System.out.println("================================ RESULTS ================================");
        System.out.printf(Locale.ROOT, "%-4s %-4s %-8s %14s %14s %9s %9s %9s%n",
                "pub", "sub", "msgSize", "publishRate", "deliveryRate", "p50(ms)", "p99(ms)", "p999(ms)");
        for (Result r : results) {
            System.out.printf(Locale.ROOT, "%-4d %-4d %-8d %14.0f %14.0f %9s %9s %9s%n",
                    r.publishers, r.subscribers, r.msgSize, r.publishRate, r.deliveryRate,
                    latencyMode ? String.format(Locale.ROOT, "%.3f", r.p50) : "-",
                    latencyMode ? String.format(Locale.ROOT, "%.3f", r.p99) : "-",
                    latencyMode ? String.format(Locale.ROOT, "%.3f", r.p999) : "-");
        }

        // CSV lines (copy-paste friendly; '.' decimal separator via Locale.ROOT).
        System.out.println();
        System.out.println("CSV (publishers,subscribers,msgSize,publishRate,deliveryRate,p50ms,p99ms,p999ms,maxms,latSamples):");
        for (Result r : results) {
            System.out.printf(Locale.ROOT, "%d,%d,%d,%.0f,%.0f,%.3f,%.3f,%.3f,%.3f,%d%n",
                    r.publishers, r.subscribers, r.msgSize, r.publishRate, r.deliveryRate,
                    r.p50, r.p99, r.p999, r.max, r.latSamples);
        }
        System.out.println("========================================================================");
    }

    // ----------------------------------------------------------------------------------------------
    // Publisher
    // ----------------------------------------------------------------------------------------------

    /**
     * A single publisher connection on its own thread. In throughput mode it writes frames in a tight
     * loop; in latency mode it paces itself to {@code rate} msg/s using nanosecond pacing. Every
     * publish that returns from {@code writeFrame} (i.e. made it onto the wire) increments the shared
     * accepted-publish counter — that is the figure reported as the publish rate.
     */
    private static final class Pub {
        private final Socket socket;
        private final DataOutputStream out;
        private final DataInputStream in;
        private final String topic;
        private final int msgSize;
        private final long rate;
        private final LongAdder publishedTotal;
        private final boolean latencyMode;
        private final CountDownLatch goLatch;
        private final CountDownLatch readyLatch;
        private final byte[] filler;
        private volatile boolean running = true;
        private Thread thread;

        Pub(int port, String topic, int msgSize, long rate, LongAdder publishedTotal,
            boolean latencyMode, CountDownLatch goLatch, CountDownLatch readyLatch) throws IOException {
            this.socket = new Socket(HOST, port);
            this.socket.setTcpNoDelay(true);
            this.out = new DataOutputStream(socket.getOutputStream());
            this.in = new DataInputStream(socket.getInputStream());
            this.topic = topic;
            this.msgSize = msgSize;
            this.rate = rate;
            this.publishedTotal = publishedTotal;
            this.latencyMode = latencyMode;
            this.goLatch = goLatch;
            this.readyLatch = readyLatch;
            // Static filler so each frame reaches the requested payload size without per-message
            // allocation churn dominating the measurement.
            this.filler = new byte[Math.max(0, msgSize)];
            Arrays.fill(this.filler, (byte) 'x');
        }

        void connect() throws IOException {
            authenticate(out, in);
        }

        void start() {
            this.thread = new Thread(this::run, "load-pub-" + topic);
            this.thread.start();
        }

        private void run() {
            try {
                readyLatch.countDown();
                goLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            if (latencyMode) {
                runPaced();
            } else {
                runUnbounded();
            }
        }

        /** Throughput mode: write as fast as the socket accepts. */
        private void runUnbounded() {
            try {
                while (running) {
                    writeFrame(out, publishEnvelope(topic, buildPayload()));
                    publishedTotal.increment();
                }
            } catch (IOException e) {
                // Expected on stop()/close().
            }
        }

        /**
         * Latency mode: emit one message every {@code 1e9 / rate} ns. Pacing is done against a fixed
         * schedule (nextNanos advances by the fixed period) so transient stalls are caught up rather
         * than permanently shifting the cadence. A busy/park hybrid keeps the cadence tight.
         */
        private void runPaced() {
            long periodNanos = 1_000_000_000L / rate;
            long next = System.nanoTime();
            try {
                while (running) {
                    long now = System.nanoTime();
                    long wait = next - now;
                    if (wait > 0) {
                        parkNanos(wait);
                    }
                    writeFrame(out, publishEnvelope(topic, buildPayload()));
                    publishedTotal.increment();
                    next += periodNanos;
                    // If we have fallen far behind (e.g. server backpressure), resync the schedule to
                    // "now" so we stop accumulating an unbounded debt that would distort the rate.
                    if (next < now - periodNanos) {
                        next = now;
                    }
                }
            } catch (IOException e) {
                // Expected on stop()/close().
            }
        }

        /**
         * Builds the payload. In latency mode the first bytes carry the publish timestamp (decimal
         * nanos) followed by a colon, so the subscriber can compute end-to-end latency; the rest is
         * static filler to reach {@code msgSize}. In throughput mode the whole payload is filler.
         */
        private String buildPayload() {
            if (!latencyMode) {
                return new String(filler, StandardCharsets.US_ASCII);
            }
            String stamp = System.nanoTime() + ":";
            byte[] stampBytes = stamp.getBytes(StandardCharsets.US_ASCII);
            int total = Math.max(stampBytes.length, msgSize);
            byte[] buf = new byte[total];
            System.arraycopy(stampBytes, 0, buf, 0, stampBytes.length);
            for (int i = stampBytes.length; i < total; i++) {
                buf[i] = 'x';
            }
            return new String(buf, StandardCharsets.US_ASCII);
        }

        void stop() {
            running = false;
            if (thread != null) {
                thread.interrupt();
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

    // ----------------------------------------------------------------------------------------------
    // Subscriber
    // ----------------------------------------------------------------------------------------------

    /**
     * A subscriber on its own daemon reader thread. It blocks on the {@code SubscribeAck} during
     * {@link #connectAndSubscribe()} so the publisher can never race ahead of the subscription.
     * Each received PUBLISH_OUTBOUND frame increments the shared delivery counter, and — while
     * recording is active in latency mode — records the end-to-end latency from the embedded
     * timestamp.
     */
    private static final class Sub {
        private final Socket socket;
        private final DataOutputStream out;
        private final DataInputStream in;
        private final String topic;
        private final LongAdder deliveredTotal;
        private final LatencyRecorder latency;
        private final FlagBox recording;
        private final CountDownLatch ackLatch = new CountDownLatch(1);
        private volatile boolean running = true;

        Sub(int port, String topic, LongAdder deliveredTotal, LatencyRecorder latency,
            FlagBox recording) throws IOException {
            this.socket = new Socket(HOST, port);
            this.socket.setTcpNoDelay(true);
            this.out = new DataOutputStream(socket.getOutputStream());
            this.in = new DataInputStream(socket.getInputStream());
            this.topic = topic;
            this.deliveredTotal = deliveredTotal;
            this.latency = latency;
            this.recording = recording;
        }

        void connectAndSubscribe() throws IOException, InterruptedException {
            authenticate(out, in);
            Thread reader = new Thread(this::readLoop, "load-sub-" + topic);
            reader.setDaemon(true);
            reader.start();
            // Subscribe and block until the server acknowledges (deterministic readiness).
            writeFrame(out, ClientEnvelope.newBuilder()
                    .setSubscribe(Subscribe.newBuilder().setTopic(topic).setToken(TOKEN).build())
                    .build());
            if (!ackLatch.await(30, TimeUnit.SECONDS)) {
                throw new IOException("No SubscribeAck for topic '" + topic + "' within timeout");
            }
        }

        private void readLoop() {
            try {
                while (running) {
                    byte[] frame = PacketFraming.readFrame(in, PACKET_SIZE);
                    long recvNanos = System.nanoTime();
                    ClientEnvelope env = WireCodec.decodeClient(frame, PACKET_SIZE);
                    switch (env.getMsgCase()) {
                        case SUBSCRIBE_ACK -> ackLatch.countDown();
                        case PUBLISH_OUTBOUND -> onDelivery(env.getPublishOutbound(), recvNanos);
                        default -> {
                            // auth_response and anything else: ignore.
                        }
                    }
                }
            } catch (IOException e) {
                // Expected on socket close / server shutdown.
            }
        }

        private void onDelivery(PublishOutbound publish, long recvNanos) {
            deliveredTotal.increment();
            if (!recording.value) {
                return;
            }
            // Latency mode: payload is "<sentNanos>:<filler>". Parse the timestamp prefix.
            String data = publish.getData();
            int colon = data.indexOf(':');
            if (colon <= 0) {
                return;
            }
            try {
                long sent = Long.parseLong(data, 0, colon, 10);
                latency.record(recvNanos - sent);
            } catch (NumberFormatException ignored) {
                // Throughput-mode filler has no timestamp; nothing to record.
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

    // ----------------------------------------------------------------------------------------------
    // Latency recording
    // ----------------------------------------------------------------------------------------------

    /**
     * Lock-free bounded latency sink. Samples (nanos) are appended into a fixed-capacity array using
     * an atomic index; once full, further samples are dropped (the steady-state window is sized so
     * the array is comfortably large). {@link #snapshotSorted()} returns a sorted copy of the
     * recorded prefix for percentile extraction.
     */
    private static final class LatencyRecorder {
        private final long[] samples;
        private final AtomicLong index = new AtomicLong();

        LatencyRecorder(int capacity) {
            this.samples = new long[Math.max(0, capacity)];
        }

        void reset() {
            index.set(0);
        }

        void record(long nanos) {
            if (samples.length == 0) {
                return;
            }
            int i = (int) index.getAndIncrement();
            if (i >= 0 && i < samples.length) {
                samples[i] = nanos;
            }
        }

        long[] snapshotSorted() {
            int n = (int) Math.min(index.get(), samples.length);
            if (n <= 0) {
                return new long[0];
            }
            long[] copy = Arrays.copyOf(samples, n);
            Arrays.sort(copy);
            return copy;
        }
    }

    /** Tiny mutable volatile boolean holder shared between the control thread and the workers. */
    private static final class FlagBox {
        volatile boolean value;
    }

    // ----------------------------------------------------------------------------------------------
    // Wire helpers (shared with the other benchmarks' style)
    // ----------------------------------------------------------------------------------------------

    private static void authenticate(DataOutputStream out, DataInputStream in) throws IOException {
        writeFrame(out, ClientEnvelope.newBuilder()
                .setAuthRequest(AuthTokenRequest.newBuilder().setSecret(SECRET).build())
                .build());
        // Consume the single auth-token response frame so it does not leak into the read loop.
        PacketFraming.readFrame(in, PACKET_SIZE);
    }

    private static ClientEnvelope publishEnvelope(String topic, String data) {
        return ClientEnvelope.newBuilder()
                .setPublishInbound(PublishInbound.newBuilder()
                        .setTopic(topic).setData(data).setSource("load").setToken(TOKEN).build())
                .build();
    }

    private static void writeFrame(DataOutputStream out, ClientEnvelope envelope) throws IOException {
        byte[] payload = WireCodec.encodeClient(envelope, PACKET_SIZE);
        synchronized (out) {
            PacketFraming.writeFrame(out, payload);
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Small utilities
    // ----------------------------------------------------------------------------------------------

    private static void sleepSeconds(int seconds) {
        try {
            Thread.sleep(seconds * 1000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Hybrid wait used by the pacer: park for the bulk, spin for the final sub-millisecond tail. */
    private static void parkNanos(long nanos) {
        if (nanos <= 0) {
            return;
        }
        long deadline = System.nanoTime() + nanos;
        if (nanos > 1_500_000L) {
            // Coarse sleep for most of the interval; LockSupport.parkNanos resolution is OS-dependent.
            java.util.concurrent.locks.LockSupport.parkNanos(nanos - 1_000_000L);
        }
        // Busy-spin the remaining sub-millisecond tail for tight cadence.
        while (System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
    }

    private static double percentileMillis(long[] sortedNanos, double p) {
        if (sortedNanos.length == 0) {
            return Double.NaN;
        }
        int idx = (int) Math.ceil(p / 100.0 * sortedNanos.length) - 1;
        idx = Math.max(0, Math.min(sortedNanos.length - 1, idx));
        return sortedNanos[idx] / 1_000_000.0;
    }

    private static long longArg(String[] args, String key, long def) {
        for (String a : args) {
            if (a.startsWith(key + "=")) {
                return Long.parseLong(a.substring(key.length() + 1).trim());
            }
        }
        return def;
    }

    /** Reads {@code msgSizes=a,b,c} if present, otherwise a single {@code msgSize=...} (default 64). */
    private static int[] msgSizesArg(String[] args) {
        for (String a : args) {
            if (a.startsWith("msgSizes=")) {
                String[] parts = a.substring("msgSizes=".length()).split(",");
                int[] sizes = new int[parts.length];
                for (int i = 0; i < parts.length; i++) {
                    sizes[i] = Integer.parseInt(parts[i].trim());
                }
                return sizes;
            }
        }
        return new int[]{(int) longArg(args, "msgSize", 64)};
    }

    // ----------------------------------------------------------------------------------------------
    // Result row
    // ----------------------------------------------------------------------------------------------

    private record Result(
            int publishers, int subscribers, int topics, int msgSize,
            double publishRate, double deliveryRate,
            double p50, double p99, double p999, double max,
            int latSamples) {
    }
}
