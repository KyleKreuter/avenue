package de.kyle.avenue.config;

import io.github.cdimascio.dotenv.Dotenv;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class AvenueClientConfig {
    private final String authenticationSecret;
    private final int port;
    private final String hostName;
    private final String clientName;
    private final int packetSize;

    public AvenueClientConfig() throws IOException {
        Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();

        Properties properties = new Properties();
        File specificationFolder = new File("config");
        if (specificationFolder.mkdir()) {
            properties.load(getClass().getResourceAsStream("/default.properties"));
        } else {
            File[] configs = specificationFolder.listFiles((dir, name) -> name.endsWith(".properties"));
            if (configs == null || configs.length == 0) {
                properties.load(getClass().getResourceAsStream("/default.properties"));
            } else {
                properties.load(new FileInputStream(configs[0]));
            }
        }

        authenticationSecret = dotenv.get("AUTHENTICATION_SECRET",
                properties.getProperty("client.authentication.secret"));

        port = parseInt(dotenv.get("SERVER_PORT", properties.getProperty("server.port")), 4180);

        packetSize = parseInt(dotenv.get("SERVER_PACKET_MAX_SIZE",
                properties.getProperty("server.packet.max-size")), 512);

        hostName = dotenv.get("SERVER_HOSTNAME", properties.getProperty("server.hostname", "localhost"));

        clientName = dotenv.get("CLIENT_NAME", properties.getProperty("client.name", "DefaultClient"));
    }

    private int parseInt(String value, int defaultValue) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public String getAuthenticationSecret() {
        return authenticationSecret;
    }

    public int getPort() {
        return port;
    }

    public String getHostName() {
        return hostName;
    }

    public int getPacketSize() {
        return packetSize;
    }

    public String getClientName() {
        return clientName;
    }
}
