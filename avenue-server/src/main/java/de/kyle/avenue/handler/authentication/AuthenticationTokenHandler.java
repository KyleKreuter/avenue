package de.kyle.avenue.handler.authentication;

import de.kyle.avenue.config.AvenueConfig;

public class AuthenticationTokenHandler {
    private final AvenueConfig avenueConfig;

    public AuthenticationTokenHandler(AvenueConfig avenueConfig) {
        this.avenueConfig = avenueConfig;
    }

    public String getToken(String secret) {
        if (!this.avenueConfig.getAuthenticationSecret().equals(secret)) {
            throw new IllegalArgumentException("Provided secret mismatched local secret");
        }
        return this.avenueConfig.getAuthenticationToken();
    }

    public boolean isValidToken(String token) {
        return this.avenueConfig.getAuthenticationToken().equals(token);
    }
}
