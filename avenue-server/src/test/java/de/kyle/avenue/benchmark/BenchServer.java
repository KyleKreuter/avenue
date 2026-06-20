package de.kyle.avenue.benchmark;

import de.kyle.avenue.SingleNodeServer;
import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.handler.client.BackpressurePolicy;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;

/**
 * Standalone benchmark server: boots a single {@link SingleNodeServer} on a FIXED port and then
 * blocks forever, so it can be driven by an out-of-process {@link LoadHarness} (started with
 * {@code connect=host:port}) running in a <em>separate JVM</em>.
 *
 * <h2>Why a separate process</h2>
 * The default {@code LoadHarness} boots the server in-proc (same JVM as the load generator). Load
 * generator and server then share CPU cores, GC and JIT, so the machine saturates while measuring
 * <em>load + server</em> together instead of the server's standalone capacity (~300k msg/s on the
 * reference machine). Running the server in its own JVM/scheduler via this class — and the harness
 * in a second one — isolates the server's capacity. Use {@code scripts/bench-split.sh} to wire the
 * two processes together. This class is a plain {@code main} (NOT a {@code *Test}), so Surefire never
 * runs it during {@code mvn test} and it never lands in the production jar (it lives under
 * {@code src/test/java}).
 *
 * <h2>Config</h2>
 * The server uses a direct-value (test) config matching what {@link LoadHarness} and the raw-socket
 * clients expect:
 * <ul>
 *   <li><b>Fixed port</b> ({@code port=}, default {@value #DEFAULT_PORT}) — the harness connects to a
 *       known port, no ephemeral-port discovery.</li>
 *   <li><b>Known secret/token</b> — {@link LoadHarness#SECRET}/{@link LoadHarness#TOKEN}, so auth
 *       succeeds.</li>
 *   <li><b>Large packet size</b> (1 MiB) — covers any swept payload.</li>
 *   <li><b>Idle-timeout 0</b> — no idle reaping of connections during a measurement.</li>
 *   <li><b>TCP_NODELAY on</b> — direct-value configs default {@code serverTcpNoDelay=true}.</li>
 *   <li><b>Unlimited connections, large outbound queue</b> — measure the path, not backpressure.</li>
 * </ul>
 *
 * <h2>How to run (own terminal/process!)</h2>
 * From the repository root, in a DEDICATED terminal (it blocks until Ctrl-C / kill):
 * <pre>
 *   export JAVA_HOME=/path/to/jdk-21
 *   /opt/homebrew/bin/mvn -q -pl avenue-server test-compile
 *   /opt/homebrew/bin/mvn -q -pl avenue-server exec:java \
 *       -Dexec.classpathScope=test \
 *       -Dexec.mainClass=de.kyle.avenue.benchmark.BenchServer \
 *       -Dexec.args="port=4180"
 * </pre>
 * Then, in a SECOND terminal/process, point the harness at it:
 * <pre>
 *   /opt/homebrew/bin/mvn -q -pl avenue-server exec:java \
 *       -Dexec.classpathScope=test \
 *       -Dexec.mainClass=de.kyle.avenue.benchmark.LoadHarness \
 *       -Dexec.args="connect=127.0.0.1:4180 publishers=1 subscribers=1 msgSize=100 seconds=8"
 * </pre>
 * The convenience script {@code scripts/bench-split.sh} automates exactly this two-process wiring
 * with plain {@code java} processes (not {@code mvn exec:java}) so the two JVMs are fully separate.
 */
public final class BenchServer {

    /** Default fixed bind port. Overridable via {@code port=}. */
    public static final int DEFAULT_PORT = 4180;

    private static final int PACKET_SIZE = 1 << 20; // 1 MiB

    private BenchServer() {
    }

    public static void main(String[] args) throws Exception {
        int port = (int) longArg(args, "port", DEFAULT_PORT);
        String host = stringArg(args, "host", "127.0.0.1");
        // Transport selector for the benchmark: io-mode=blocking (default) or io-mode=nio. nio also
        // accepts an optional io-threads= override (default = availableProcessors). This lets the
        // perf comparison drive the exact same BenchServer config through both transports.
        String ioMode = stringArg(args, "io-mode", "blocking");
        int ioThreads = (int) longArg(args, "io-threads", 0);

        // Direct-value (test) config. The 26-arg overload is the one that lets us pin
        // clientIdleTimeoutMillis=0 (no reaping) and maxConnections=0 (unlimited). serverTcpNoDelay
        // defaults to true on the direct-value path. Secret/token MUST match LoadHarness.
        AvenueConfig config = new AvenueConfig(
                PACKET_SIZE,
                false,                  // dropUnknownPackets: never drop mid-benchmark
                LoadHarness.SECRET,
                LoadHarness.TOKEN,
                port,                   // FIXED port
                1_000_000,              // large outbound queue: measure the path, not backpressure
                1000,                   // outbound offer timeout (ms)
                false,                  // clusterEnabled
                "bench-node",           // nodeId
                0,                      // clusterPort (unused)
                List.of(),              // clusterPeers
                "",                     // clusterSecret
                5_000L,                 // clusterHeartbeatIntervalMs
                0L,                     // clientIdleTimeoutMillis = 0 -> no idle reaping
                0,                      // maxConnections = 0 -> unlimited
                BackpressurePolicy.DISCONNECT_SLOW_CONSUMER,
                0L,                     // metricsLogIntervalSeconds = 0 -> no periodic log noise
                false, "", "",          // server TLS off
                false, "", "", "", ""   // cluster TLS off
        );
        // Flip the transport via the copy constructor (direct-value configs always default to
        // blocking). io-mode=blocking leaves the config untouched.
        config = new AvenueConfig(config, ioMode, ioThreads);
        System.out.printf(Locale.ROOT, "BenchServer io-mode=%s nioIoThreads=%d%n",
                config.getServerIoMode(), config.getServerNioIoThreads());

        SingleNodeServer server = new SingleNodeServer(config);
        // Clean shutdown on Ctrl-C / process kill (SIGTERM). SingleNodeServer also registers its own
        // hook, but registering ours makes the intent explicit and is idempotent (stop() is a no-op
        // once stopped).
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop, "bench-server-shutdown"));

        server.start();
        int boundPort = server.getPort();

        // Clear, machine-parseable readiness line on stdout. The split script greps for "BENCH SERVER
        // READY" before it starts the load process, so there is no readiness sleep.
        System.out.printf(Locale.ROOT, "BENCH SERVER READY host=%s port=%d%n", host, boundPort);
        System.out.flush();

        // Block forever; the process is meant to be killed (Ctrl-C / kill) by the operator/script.
        new CountDownLatch(1).await();
    }

    private static long longArg(String[] args, String key, long def) {
        for (String a : args) {
            if (a.startsWith(key + "=")) {
                return Long.parseLong(a.substring(key.length() + 1).trim());
            }
        }
        return def;
    }

    private static String stringArg(String[] args, String key, String def) {
        for (String a : args) {
            if (a.startsWith(key + "=")) {
                return a.substring(key.length() + 1).trim();
            }
        }
        return def;
    }
}
