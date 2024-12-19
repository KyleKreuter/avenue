package de.kyle.avenue.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class AvenueClientConfig {
    private final String authenticationSecret;
    private final int port;
    private final String hostName;


    public AvenueClientConfig() throws IOException {
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

        authenticationSecret = properties.getProperty("client.authentication.secret");
        port = Integer.parseInt(properties.getProperty("server.port"));
        hostName = properties.getProperty("server.hostname");
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
}
