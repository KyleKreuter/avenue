package de.kyle.avenue.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class AvenueConfig {
    private final int packetSize;
    private final boolean dropUnknownPackets;
    private final String authenticationSecret;

    private final String authenticationToken;


    public AvenueConfig() throws IOException {
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

        packetSize = Integer.parseInt(properties.getProperty("server.packet.max-size"));
        dropUnknownPackets = Boolean.parseBoolean(properties.getProperty("server.packet.drop-unknown"));
        authenticationSecret = properties.getProperty("server.authentication.secret");
        authenticationToken = properties.getProperty("server.authentication.token");
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
}
