package de.kyle.avenue.config;

/**
 * Admin HTTP introspection settings (Phase F).
 * <p>
 * Bundled into a single record — like {@link ClusterTuning} — so it can be threaded through
 * {@link AvenueConfig} without adding yet more positional constructor parameters and breaking the
 * many existing callers. Every non-admin constructor delegates to {@link #disabled()}, so the
 * default behaviour is unchanged: the admin endpoint is off.
 * <p>
 * The server is read-only, binds to {@link #bindAddress} (default {@code 127.0.0.1}, i.e. loopback
 * only) and is disabled unless {@link #enabled} is {@code true}. An optional {@link #secret} gates
 * every request behind an {@code X-Admin-Secret} header (constant-time compared).
 *
 * @param enabled     whether the admin HTTP server is started ({@code admin.http.enabled}, default false)
 * @param port        TCP port to bind ({@code admin.http.port}; {@code 0} = ephemeral, useful for tests)
 * @param bindAddress interface to bind ({@code admin.http.bind-address}, default {@code 127.0.0.1})
 * @param secret      optional shared secret required in the {@code X-Admin-Secret} header
 *                    ({@code admin.http.secret}; empty = no auth)
 */
public record AdminConfig(
        boolean enabled,
        int port,
        String bindAddress,
        String secret
) {

    public static final String DEFAULT_BIND_ADDRESS = "127.0.0.1";

    /** Normalizes nulls so callers never have to null-check the string fields. */
    public AdminConfig {
        if (bindAddress == null || bindAddress.isBlank()) {
            bindAddress = DEFAULT_BIND_ADDRESS;
        }
        if (secret == null) {
            secret = "";
        }
    }

    /** The default: admin HTTP disabled. Used by every non-file {@link AvenueConfig} constructor. */
    public static AdminConfig disabled() {
        return new AdminConfig(false, 0, DEFAULT_BIND_ADDRESS, "");
    }

    /** True when an admin secret is configured and must be presented by callers. */
    public boolean requiresSecret() {
        return !secret.isEmpty();
    }
}
