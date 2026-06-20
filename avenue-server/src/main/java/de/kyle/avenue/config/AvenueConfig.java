package de.kyle.avenue.config;

import io.github.cdimascio.dotenv.Dotenv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
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
        // Per-client outbound queue tuning. Defaults are applied if neither env nor the
        // selected properties file provides a value, so existing configs keep working.
        outboundQueueCapacity = Integer.parseInt(
                dotenv.get("SERVER_OUTBOUND_QUEUE_CAPACITY",
                        properties.getProperty("server.outbound.queue.capacity", "1024"))
        );
        outboundQueueOfferTimeoutMillis = Long.parseLong(
                dotenv.get("SERVER_OUTBOUND_QUEUE_OFFER_TIMEOUT_MS",
                        properties.getProperty("server.outbound.queue.offer-timeout-ms", "100"))
        );
    }

    /**
     * Loads the configuration properties using a clearly defined source precedence:
     * <ol>
     *   <li>If a {@code config/} folder exists and contains {@code *.properties} files, the
     *       first one in deterministic (alphabetical, case-insensitive) filename order is used.</li>
     *   <li>Otherwise the bundled classpath resource {@code /default.properties} is used.</li>
     * </ol>
     * The {@code config/} folder is created if it does not yet exist (using the {@code Files}
     * API, so an already-existing folder is not treated as an error like {@code File.mkdir()}
     * would). Environment / {@code .env} values always override file values at field level.
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
}
