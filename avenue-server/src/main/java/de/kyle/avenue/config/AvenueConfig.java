package de.kyle.avenue.config;

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
        this.packetSize = packetSize;
        this.dropUnknownPackets = dropUnknownPackets;
        this.authenticationSecret = authenticationSecret;
        this.authenticationToken = authenticationToken;
        this.port = port;
        this.outboundQueueCapacity = outboundQueueCapacity;
        this.outboundQueueOfferTimeoutMillis = outboundQueueOfferTimeoutMillis;
        this.clusterEnabled = clusterEnabled;
        this.nodeId = nodeId;
        this.clusterPort = clusterPort;
        this.clusterPeers = clusterPeers != null ? List.copyOf(clusterPeers) : List.of();
        this.clusterSecret = clusterSecret != null ? clusterSecret : "";
        this.clusterHeartbeatIntervalMs = clusterHeartbeatIntervalMs;
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
}
