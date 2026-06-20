package de.kyle.avenue.handler.authentication;

import de.kyle.avenue.config.AvenueConfig;

import java.security.MessageDigest;

import static java.nio.charset.StandardCharsets.UTF_8;

public class AuthenticationTokenHandler {
    private final AvenueConfig avenueConfig;

    public AuthenticationTokenHandler(AvenueConfig avenueConfig) {
        this.avenueConfig = avenueConfig;
    }

    public String getToken(String secret) {
        if (!constantTimeEquals(this.avenueConfig.getAuthenticationSecret(), secret)) {
            throw new IllegalArgumentException("Provided secret mismatched local secret");
        }
        return this.avenueConfig.getAuthenticationToken();
    }

    public boolean isValidToken(String token) {
        return constantTimeEquals(this.avenueConfig.getAuthenticationToken(), token);
    }

    /**
     * Compares two secrets in constant time to avoid leaking how many leading characters
     * matched via a timing side channel. Null-safe: a null on either side never matches.
     */
    private boolean constantTimeEquals(String expected, String actual) {
        if (expected == null || actual == null) {
            return false;
        }
        return MessageDigest.isEqual(expected.getBytes(UTF_8), actual.getBytes(UTF_8));
    }
}
