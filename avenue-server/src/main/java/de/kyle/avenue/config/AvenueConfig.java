package de.kyle.avenue.config;

import io.github.cdimascio.dotenv.Dotenv;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class AvenueConfig {
    private final int packetSize;
    private final boolean dropUnknownPackets;
    private final String authenticationSecret;
    private final String authenticationToken;
    private final int port;

    public AvenueConfig() throws IOException {
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
}
