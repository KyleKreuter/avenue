package de.kyle.avenue.config;

import de.kyle.avenue.handler.client.BackpressurePolicy;
import io.github.cdimascio.dotenv.Dotenv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;

public class AvenueConfig {
    private static final Logger log = LoggerFactory.getLogger(AvenueConfig.class);

    private static final Path CONFIG_FOLDER = Path.of("config");
    private static final String DEFAULT_PROPERTIES_CLASSPATH = "/default.properties";

    private final int packetSize;
    private final boolean dropUnknownPackets;
    private final String authenticationSecret;
    private final String authenticationToken;
    private final int port;
    private final int outboundQueueCapacity;
    private final long outboundQueueOfferTimeoutMillis;

    /**
     * Whether accepted client sockets disable Nagle's algorithm ({@code TCP_NODELAY}). Defaults
     * to {@code true} so small pub/sub frames flush immediately. Direct-value (test) constructors
     * use the default; only the file/{@code .env} constructor reads {@code server.tcp-nodelay}.
     */
    private final boolean serverTcpNoDelay;

    // -------------------------------------------------------------------------
    // Cluster fields (all optional; cluster is disabled by default)
    // -------------------------------------------------------------------------

    private final boolean clusterEnabled;
    private final String nodeId;
    private final int clusterPort;
    private final List<String> clusterPeers;
    private final String clusterSecret;
    private final long clusterHeartbeatIntervalMs;

    /**
     * Phase C cluster tuning (replay/ack/ordering). Bundled into one record so the legacy
     * 7/13/26-arg constructors stay source-compatible: they all delegate with
     * {@link ClusterTuning#defaults()}. Only the file/{@code .env} constructor reads the
     * {@code cluster.replay.*} / {@code cluster.ack-interval-ms} / {@code cluster.strict-ordering} /
     * {@code cluster.origin.expiry-ms} properties.
     */
    private final ClusterTuning clusterTuning;

    // -------------------------------------------------------------------------
    // Wave 5 — Security & Ops fields (all optional, safe defaults)
    // -------------------------------------------------------------------------

    /** Socket read idle-timeout in ms for client connections. {@code 0} disables it. */
    private final long clientIdleTimeoutMillis;
    /** Maximum number of concurrent client connections. {@code 0} means unlimited. */
    private final int maxConnections;
    /** Backpressure policy applied when a client's outbound queue stays full. */
    private final BackpressurePolicy backpressurePolicy;
    /** Interval (seconds) for the periodic metrics log. {@code 0} disables periodic logging. */
    private final long metricsLogIntervalSeconds;

    // TLS for the client-facing port.
    private final boolean serverTlsEnabled;
    private final String serverTlsKeystorePath;
    private final String serverTlsKeystorePassword;

    // TLS for the cluster transport.
    private final boolean clusterTlsEnabled;
    private final String clusterTlsKeystorePath;
    private final String clusterTlsKeystorePassword;
    private final String clusterTlsTruststorePath;
    private final String clusterTlsTruststorePassword;

    /**
     * Direct-value constructor used for tests (and any embedding that already holds its
     * configuration in memory). Bypasses file/{@code .env} loading entirely. Clustering is
     * disabled when this constructor is used; use the full cluster constructor
     * {@link #AvenueConfig(int, boolean, String, String, int, int, long, boolean, String, int, List, String, long)}
     * for cluster-aware tests.
     *
     * @param packetSize                      maximum allowed payload size in bytes
     * @param dropUnknownPackets              whether malformed/unknown packets disconnect the client
     * @param authenticationSecret            the shared secret a client must present to obtain a token
     * @param authenticationToken             the token issued on a valid secret and required by secured handlers
     * @param port                            the TCP port to bind ({@code 0} = ephemeral)
     * @param outboundQueueCapacity           per-client outbound queue capacity
     * @param outboundQueueOfferTimeoutMillis backpressure offer timeout in milliseconds
     */
    public AvenueConfig(
            int packetSize,
            boolean dropUnknownPackets,
            String authenticationSecret,
            String authenticationToken,
            int port,
            int outboundQueueCapacity,
            long outboundQueueOfferTimeoutMillis
    ) {
        this(packetSize, dropUnknownPackets, authenticationSecret, authenticationToken,
                port, outboundQueueCapacity, outboundQueueOfferTimeoutMillis,
                false, UUID.randomUUID().toString(), 0, List.of(), "", 5_000L);
    }

    /**
     * Full direct-value constructor including all cluster settings. Used by cluster
     * integration tests to spin up nodes with known configuration.
     *
     * @param packetSize                      maximum allowed payload size in bytes
     * @param dropUnknownPackets              whether malformed/unknown packets disconnect the client
     * @param authenticationSecret            the shared secret a client must present to obtain a token
     * @param authenticationToken             the token issued on a valid secret and required by secured handlers
     * @param port                            the TCP client port to bind ({@code 0} = ephemeral)
     * @param outboundQueueCapacity           per-client outbound queue capacity
     * @param outboundQueueOfferTimeoutMillis backpressure offer timeout in milliseconds
     * @param clusterEnabled                  {@code true} to activate clustering
     * @param nodeId                          unique identifier for this node (e.g. UUID)
     * @param clusterPort                     the TCP cluster port to bind ({@code 0} = ephemeral)
     * @param clusterPeers                    list of peer addresses in {@code host:port} format
     * @param clusterSecret                   shared secret for peer authentication
     * @param clusterHeartbeatIntervalMs      interval between heartbeat packets in milliseconds
     */
    public AvenueConfig(
            int packetSize,
            boolean dropUnknownPackets,
            String authenticationSecret,
            String authenticationToken,
            int port,
            int outboundQueueCapacity,
            long outboundQueueOfferTimeoutMillis,
            boolean clusterEnabled,
            String nodeId,
            int clusterPort,
            List<String> clusterPeers,
            String clusterSecret,
            long clusterHeartbeatIntervalMs
    ) {
        // Delegate to the full Wave-5 constructor using safe defaults: TLS off, generous
        // connection limit, idle-timeout off, default backpressure policy. This keeps every
        // existing 7- and 13-argument caller (including all tests) source-compatible.
        this(packetSize, dropUnknownPackets, authenticationSecret, authenticationToken,
                port, outboundQueueCapacity, outboundQueueOfferTimeoutMillis,
                clusterEnabled, nodeId, clusterPort, clusterPeers, clusterSecret,
                clusterHeartbeatIntervalMs,
                0L,                                  // clientIdleTimeoutMillis (0 = disabled)
                10_000,                              // maxConnections
                BackpressurePolicy.DISCONNECT_SLOW_CONSUMER,
                0L,                                  // metricsLogIntervalSeconds (0 = disabled in tests)
                false, "", "",                       // server TLS off
                false, "", "", "", "");              // cluster TLS off
    }

    /**
     * Full direct-value constructor including all Wave-5 security/ops settings. Used by the
     * TLS and metrics tests to spin up nodes with known configuration without file/env loading.
     *
     * @param clientIdleTimeoutMillis      socket read idle-timeout in ms ({@code 0} = disabled)
     * @param maxConnections               max concurrent client connections ({@code 0} = unlimited)
     * @param backpressurePolicy           policy applied when an outbound queue stays full
     * @param metricsLogIntervalSeconds    periodic metrics log interval in s ({@code 0} = disabled)
     * @param serverTlsEnabled             enable TLS on the client port
     * @param serverTlsKeystorePath        keystore path for the client-port TLS server socket
     * @param serverTlsKeystorePassword    keystore password for the client-port TLS server socket
     * @param clusterTlsEnabled            enable TLS on the cluster transport
     * @param clusterTlsKeystorePath       keystore path for the cluster TLS server socket
     * @param clusterTlsKeystorePassword   keystore password for the cluster TLS server socket
     * @param clusterTlsTruststorePath     truststore path used by cluster TLS client sockets
     * @param clusterTlsTruststorePassword truststore password used by cluster TLS client sockets
     */
    public AvenueConfig(
            int packetSize,
            boolean dropUnknownPackets,
            String authenticationSecret,
            String authenticationToken,
            int port,
            int outboundQueueCapacity,
            long outboundQueueOfferTimeoutMillis,
            boolean clusterEnabled,
            String nodeId,
            int clusterPort,
            List<String> clusterPeers,
            String clusterSecret,
            long clusterHeartbeatIntervalMs,
            long clientIdleTimeoutMillis,
            int maxConnections,
            BackpressurePolicy backpressurePolicy,
            long metricsLogIntervalSeconds,
            boolean serverTlsEnabled,
            String serverTlsKeystorePath,
            String serverTlsKeystorePassword,
            boolean clusterTlsEnabled,
            String clusterTlsKeystorePath,
            String clusterTlsKeystorePassword,
            String clusterTlsTruststorePath,
            String clusterTlsTruststorePassword
    ) {
        // Direct-value (test) callers get the production-default cluster tuning. The dedicated
        // tuning-aware overload below lets cluster tests pin replay/ack/ordering knobs.
        this(packetSize, dropUnknownPackets, authenticationSecret, authenticationToken, port,
                outboundQueueCapacity, outboundQueueOfferTimeoutMillis, clusterEnabled, nodeId,
                clusterPort, clusterPeers, clusterSecret, clusterHeartbeatIntervalMs,
                clientIdleTimeoutMillis, maxConnections, backpressurePolicy, metricsLogIntervalSeconds,
                serverTlsEnabled, serverTlsKeystorePath, serverTlsKeystorePassword,
                clusterTlsEnabled, clusterTlsKeystorePath, clusterTlsKeystorePassword,
                clusterTlsTruststorePath, clusterTlsTruststorePassword,
                ClusterTuning.defaults());
    }

    /**
     * Tuning-aware full direct-value constructor. Identical to the 26-argument overload but accepts
     * an explicit {@link ClusterTuning}, so Phase C cluster tests can pin replay capacity, ack
     * interval, backpressure policy and strict ordering deterministically. Production code never
     * calls this directly — it uses the file/{@code .env} constructor.
     */
    public AvenueConfig(
            int packetSize,
            boolean dropUnknownPackets,
            String authenticationSecret,
            String authenticationToken,
            int port,
            int outboundQueueCapacity,
            long outboundQueueOfferTimeoutMillis,
            boolean clusterEnabled,
            String nodeId,
            int clusterPort,
            List<String> clusterPeers,
            String clusterSecret,
            long clusterHeartbeatIntervalMs,
            long clientIdleTimeoutMillis,
            int maxConnections,
            BackpressurePolicy backpressurePolicy,
            long metricsLogIntervalSeconds,
            boolean serverTlsEnabled,
            String serverTlsKeystorePath,
            String serverTlsKeystorePassword,
            boolean clusterTlsEnabled,
            String clusterTlsKeystorePath,
            String clusterTlsKeystorePassword,
            String clusterTlsTruststorePath,
            String clusterTlsTruststorePassword,
            ClusterTuning clusterTuning
    ) {
        this.packetSize = packetSize;
        this.dropUnknownPackets = dropUnknownPackets;
        this.authenticationSecret = authenticationSecret;
        this.authenticationToken = authenticationToken;
        this.port = port;
        this.outboundQueueCapacity = outboundQueueCapacity;
        this.outboundQueueOfferTimeoutMillis = outboundQueueOfferTimeoutMillis;
        // Direct-value (test) callers get the production default; the file/.env constructor sets
        // this field itself from the server.tcp-nodelay property.
        this.serverTcpNoDelay = true;
        this.clusterEnabled = clusterEnabled;
        this.nodeId = nodeId;
        this.clusterPort = clusterPort;
        this.clusterPeers = clusterPeers != null ? List.copyOf(clusterPeers) : List.of();
        this.clusterSecret = clusterSecret != null ? clusterSecret : "";
        this.clusterHeartbeatIntervalMs = clusterHeartbeatIntervalMs;
        this.clusterTuning = clusterTuning != null ? clusterTuning : ClusterTuning.defaults();
        this.clientIdleTimeoutMillis = clientIdleTimeoutMillis;
        this.maxConnections = maxConnections;
        this.backpressurePolicy = backpressurePolicy != null
                ? backpressurePolicy : BackpressurePolicy.DISCONNECT_SLOW_CONSUMER;
        this.metricsLogIntervalSeconds = metricsLogIntervalSeconds;
        this.serverTlsEnabled = serverTlsEnabled;
        this.serverTlsKeystorePath = serverTlsKeystorePath != null ? serverTlsKeystorePath : "";
        this.serverTlsKeystorePassword = serverTlsKeystorePassword != null ? serverTlsKeystorePassword : "";
        this.clusterTlsEnabled = clusterTlsEnabled;
        this.clusterTlsKeystorePath = clusterTlsKeystorePath != null ? clusterTlsKeystorePath : "";
        this.clusterTlsKeystorePassword = clusterTlsKeystorePassword != null ? clusterTlsKeystorePassword : "";
        this.clusterTlsTruststorePath = clusterTlsTruststorePath != null ? clusterTlsTruststorePath : "";
        this.clusterTlsTruststorePassword = clusterTlsTruststorePassword != null ? clusterTlsTruststorePassword : "";
    }

    public AvenueConfig() throws IOException {
        Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
        Properties properties = loadProperties();

        packetSize = Integer.parseInt(
                dotenv.get("SERVER_PACKET_MAX_SIZE", properties.getProperty("server.packet.max-size"))
        );
        dropUnknownPackets = Boolean.parseBoolean(
                dotenv.get("SERVER_PACKET_DROP_UNKNOWN", properties.getProperty("server.packet.drop-unknown"))
        );
        authenticationSecret = dotenv.get(
                "AUTHENTICATION_SECRET", properties.getProperty("server.authentication.secret")
        );
        authenticationToken = dotenv.get(
                "AUTHENTICATION_TOKEN", properties.getProperty("server.authentication.token")
        );
        port = Integer.parseInt(
                dotenv.get("SERVER_PORT", properties.getProperty("server.port"))
        );
        outboundQueueCapacity = Integer.parseInt(
                dotenv.get("SERVER_OUTBOUND_QUEUE_CAPACITY",
                        properties.getProperty("server.outbound.queue.capacity", "1024"))
        );
        outboundQueueOfferTimeoutMillis = Long.parseLong(
                dotenv.get("SERVER_OUTBOUND_QUEUE_OFFER_TIMEOUT_MS",
                        properties.getProperty("server.outbound.queue.offer-timeout-ms", "100"))
        );
        serverTcpNoDelay = Boolean.parseBoolean(
                dotenv.get("SERVER_TCP_NODELAY",
                        properties.getProperty("server.tcp-nodelay", "true"))
        );

        // Cluster settings — all optional, clustering is off by default.
        clusterEnabled = Boolean.parseBoolean(
                dotenv.get("CLUSTER_ENABLED",
                        properties.getProperty("cluster.enabled", "false"))
        );
        nodeId = dotenv.get("CLUSTER_NODE_ID",
                properties.getProperty("cluster.node-id", UUID.randomUUID().toString()));
        clusterPort = Integer.parseInt(
                dotenv.get("CLUSTER_PORT",
                        properties.getProperty("cluster.port", "0"))
        );
        String peersRaw = dotenv.get("CLUSTER_PEERS",
                properties.getProperty("cluster.peers", ""));
        clusterPeers = peersRaw.isBlank()
                ? List.of()
                : Arrays.stream(peersRaw.split(","))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .toList();
        clusterSecret = dotenv.get("CLUSTER_SECRET",
                properties.getProperty("cluster.secret", ""));
        clusterHeartbeatIntervalMs = Long.parseLong(
                dotenv.get("CLUSTER_HEARTBEAT_INTERVAL_MS",
                        properties.getProperty("cluster.heartbeat-interval-ms", "5000"))
        );

        // Phase C — at-least-once replay/ack/ordering tuning, bundled into ClusterTuning.
        int replayCapacity = Integer.parseInt(
                dotenv.get("CLUSTER_REPLAY_CAPACITY",
                        properties.getProperty("cluster.replay.capacity",
                                Integer.toString(ClusterTuning.DEFAULT_REPLAY_CAPACITY)))
        );
        de.kyle.avenue.cluster.ReplayBuffer.BackpressurePolicy replayPolicy =
                de.kyle.avenue.cluster.ReplayBuffer.BackpressurePolicy.fromConfig(
                        dotenv.get("CLUSTER_REPLAY_BACKPRESSURE_POLICY",
                                properties.getProperty("cluster.replay.backpressure.policy", "BLOCK"))
                );
        long replayOfferTimeoutMs = Long.parseLong(
                dotenv.get("CLUSTER_REPLAY_OFFER_TIMEOUT_MS",
                        properties.getProperty("cluster.replay.offer-timeout-ms",
                                Long.toString(ClusterTuning.DEFAULT_REPLAY_OFFER_TIMEOUT_MS)))
        );
        long ackIntervalMs = Long.parseLong(
                dotenv.get("CLUSTER_ACK_INTERVAL_MS",
                        properties.getProperty("cluster.ack-interval-ms",
                                Long.toString(ClusterTuning.DEFAULT_ACK_INTERVAL_MS)))
        );
        boolean strictOrdering = Boolean.parseBoolean(
                dotenv.get("CLUSTER_STRICT_ORDERING",
                        properties.getProperty("cluster.strict-ordering",
                                Boolean.toString(ClusterTuning.DEFAULT_STRICT_ORDERING)))
        );
        long originExpiryMs = Long.parseLong(
                dotenv.get("CLUSTER_ORIGIN_EXPIRY_MS",
                        properties.getProperty("cluster.origin.expiry-ms",
                                Long.toString(ClusterTuning.DEFAULT_ORIGIN_EXPIRY_MS)))
        );
        // Phase D — interest-based routing tuning.
        long interestSyncIntervalMs = Long.parseLong(
                dotenv.get("CLUSTER_INTEREST_SYNC_INTERVAL_MS",
                        properties.getProperty("cluster.interest.sync-interval-ms",
                                Long.toString(ClusterTuning.DEFAULT_INTEREST_SYNC_INTERVAL_MS)))
        );
        long interestBroadcastGraceMs = Long.parseLong(
                dotenv.get("CLUSTER_INTEREST_BROADCAST_GRACE_MS",
                        properties.getProperty("cluster.interest.broadcast-grace-ms",
                                Long.toString(ClusterTuning.DEFAULT_INTEREST_BROADCAST_GRACE_MS)))
        );
        clusterTuning = new ClusterTuning(
                replayCapacity, replayPolicy, replayOfferTimeoutMs,
                ackIntervalMs, strictOrdering, originExpiryMs,
                interestSyncIntervalMs, interestBroadcastGraceMs);

        // Wave 5 — Security & Ops settings (all optional, safe defaults).
        clientIdleTimeoutMillis = Long.parseLong(
                dotenv.get("SERVER_CLIENT_IDLE_TIMEOUT_MS",
                        properties.getProperty("server.client.idle-timeout-ms", "60000"))
        );
        maxConnections = Integer.parseInt(
                dotenv.get("SERVER_MAX_CONNECTIONS",
                        properties.getProperty("server.max-connections", "10000"))
        );
        backpressurePolicy = BackpressurePolicy.fromConfig(
                dotenv.get("SERVER_BACKPRESSURE_POLICY",
                        properties.getProperty("server.backpressure.policy", "DISCONNECT_SLOW_CONSUMER"))
        );
        metricsLogIntervalSeconds = Long.parseLong(
                dotenv.get("SERVER_METRICS_LOG_INTERVAL_SECONDS",
                        properties.getProperty("server.metrics.log-interval-seconds", "30"))
        );

        serverTlsEnabled = Boolean.parseBoolean(
                dotenv.get("SERVER_TLS_ENABLED",
                        properties.getProperty("server.tls.enabled", "false"))
        );
        serverTlsKeystorePath = dotenv.get("SERVER_TLS_KEYSTORE_PATH",
                properties.getProperty("server.tls.keystore-path", ""));
        serverTlsKeystorePassword = dotenv.get("SERVER_TLS_KEYSTORE_PASSWORD",
                properties.getProperty("server.tls.keystore-password", ""));

        clusterTlsEnabled = Boolean.parseBoolean(
                dotenv.get("CLUSTER_TLS_ENABLED",
                        properties.getProperty("cluster.tls.enabled", "false"))
        );
        clusterTlsKeystorePath = dotenv.get("CLUSTER_TLS_KEYSTORE_PATH",
                properties.getProperty("cluster.tls.keystore-path", ""));
        clusterTlsKeystorePassword = dotenv.get("CLUSTER_TLS_KEYSTORE_PASSWORD",
                properties.getProperty("cluster.tls.keystore-password", ""));
        clusterTlsTruststorePath = dotenv.get("CLUSTER_TLS_TRUSTSTORE_PATH",
                properties.getProperty("cluster.tls.truststore-path", ""));
        clusterTlsTruststorePassword = dotenv.get("CLUSTER_TLS_TRUSTSTORE_PASSWORD",
                properties.getProperty("cluster.tls.truststore-password", ""));
    }

    /**
     * Loads the configuration properties using a clearly defined source precedence:
     * <ol>
     *   <li>If a {@code config/} folder exists and contains {@code *.properties} files, the
     *       first one in deterministic (alphabetical, case-insensitive) filename order is used.</li>
     *   <li>Otherwise the bundled classpath resource {@code /default.properties} is used.</li>
     * </ol>
     */
    private Properties loadProperties() throws IOException {
        Properties properties = new Properties();

        if (!Files.exists(CONFIG_FOLDER)) {
            Files.createDirectories(CONFIG_FOLDER);
        }

        Optional<Path> configFile = findFirstConfigFile();
        if (configFile.isPresent()) {
            Path path = configFile.get();
            log.info("Loading configuration from {}", path.toAbsolutePath());
            try (InputStream in = Files.newInputStream(path)) {
                properties.load(in);
            }
        } else {
            log.info("No external configuration found in {}, falling back to classpath {}",
                    CONFIG_FOLDER.toAbsolutePath(), DEFAULT_PROPERTIES_CLASSPATH);
            try (InputStream in = getClass().getResourceAsStream(DEFAULT_PROPERTIES_CLASSPATH)) {
                if (in == null) {
                    throw new IOException("Bundled default configuration " + DEFAULT_PROPERTIES_CLASSPATH + " is missing");
                }
                properties.load(in);
            }
        }
        return properties;
    }

    /**
     * Returns the first {@code *.properties} file in {@link #CONFIG_FOLDER} in deterministic
     * filename order, or empty if the folder holds no properties files.
     */
    private Optional<Path> findFirstConfigFile() throws IOException {
        try (Stream<Path> entries = Files.list(CONFIG_FOLDER)) {
            List<Path> propertyFiles = entries
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".properties"))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString().toLowerCase()))
                    .toList();
            return propertyFiles.isEmpty() ? Optional.empty() : Optional.of(propertyFiles.get(0));
        }
    }

    // -------------------------------------------------------------------------
    // Getters — existing fields
    // -------------------------------------------------------------------------

    public int getPacketSize() {
        return packetSize;
    }

    public boolean isDropUnknownPackets() {
        return dropUnknownPackets;
    }

    public String getAuthenticationSecret() {
        return authenticationSecret;
    }

    public String getAuthenticationToken() {
        return authenticationToken;
    }

    public int getPort() {
        return port;
    }

    public int getOutboundQueueCapacity() {
        return outboundQueueCapacity;
    }

    public long getOutboundQueueOfferTimeoutMillis() {
        return outboundQueueOfferTimeoutMillis;
    }

    /**
     * Whether accepted client sockets disable Nagle's algorithm ({@code TCP_NODELAY}).
     * Defaults to {@code true}.
     */
    public boolean isServerTcpNoDelay() {
        return serverTcpNoDelay;
    }

    // -------------------------------------------------------------------------
    // Getters — cluster fields
    // -------------------------------------------------------------------------

    /** Returns {@code true} if clustering is enabled for this node. */
    public boolean isClusterEnabled() {
        return clusterEnabled;
    }

    /** Returns this node's unique identifier. Auto-generated UUID if not configured. */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * Returns the TCP port for the cluster transport. {@code 0} means ephemeral (auto-assigned
     * by the OS). Defaults to {@code 0} when not configured, so tests always get a free port.
     */
    public int getClusterPort() {
        return clusterPort;
    }

    /**
     * Returns the list of static peer addresses in {@code host:port} format. Empty list when
     * not configured or when clustering is disabled.
     */
    public List<String> getClusterPeers() {
        return clusterPeers;
    }

    /** Returns the shared secret used to authenticate cluster peer links. */
    public String getClusterSecret() {
        return clusterSecret;
    }

    /** Returns the heartbeat interval in milliseconds. Default is 5 000 ms. */
    public long getClusterHeartbeatIntervalMs() {
        return clusterHeartbeatIntervalMs;
    }

    /**
     * Returns the Phase C cluster tuning bundle (replay capacity / backpressure policy / ack
     * interval / strict ordering / origin expiry). Never {@code null}; direct-value constructors
     * return {@link ClusterTuning#defaults()}.
     */
    public ClusterTuning getClusterTuning() {
        return clusterTuning;
    }

    // -------------------------------------------------------------------------
    // Getters — Wave 5 security/ops fields
    // -------------------------------------------------------------------------

    /** Socket read idle-timeout in ms for client connections. {@code 0} disables it. */
    public long getClientIdleTimeoutMillis() {
        return clientIdleTimeoutMillis;
    }

    /** Maximum number of concurrent client connections. {@code 0} means unlimited. */
    public int getMaxConnections() {
        return maxConnections;
    }

    /** The configured backpressure policy for slow consumers. */
    public BackpressurePolicy getBackpressurePolicy() {
        return backpressurePolicy;
    }

    /** Interval (seconds) for the periodic metrics log. {@code 0} disables periodic logging. */
    public long getMetricsLogIntervalSeconds() {
        return metricsLogIntervalSeconds;
    }

    /** Returns {@code true} if TLS is enabled on the client-facing port. */
    public boolean isServerTlsEnabled() {
        return serverTlsEnabled;
    }

    public String getServerTlsKeystorePath() {
        return serverTlsKeystorePath;
    }

    public String getServerTlsKeystorePassword() {
        return serverTlsKeystorePassword;
    }

    /** Returns {@code true} if TLS is enabled on the cluster transport. */
    public boolean isClusterTlsEnabled() {
        return clusterTlsEnabled;
    }

    public String getClusterTlsKeystorePath() {
        return clusterTlsKeystorePath;
    }

    public String getClusterTlsKeystorePassword() {
        return clusterTlsKeystorePassword;
    }

    public String getClusterTlsTruststorePath() {
        return clusterTlsTruststorePath;
    }

    public String getClusterTlsTruststorePassword() {
        return clusterTlsTruststorePassword;
    }
}
