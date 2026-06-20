package de.kyle.avenue.handler.client;

/**
 * Explicit, configurable backpressure policy applied by the per-client outbound queue when a
 * producer cannot enqueue a packet within the configured offer timeout (i.e. the client is a
 * slow consumer that cannot keep up with the fan-out rate).
 *
 * <p>The default policy {@link #DISCONNECT_SLOW_CONSUMER} preserves the pre-Wave-5 behaviour:
 * a client whose bounded outbound queue stays full beyond the offer timeout is considered too
 * slow and is disconnected, protecting the rest of the fan-out from head-of-line blocking.
 */
public enum BackpressurePolicy {

    /**
     * Disconnect a slow consumer whose outbound queue stays full beyond the offer timeout.
     * This is the default and matches the original server behaviour.
     */
    DISCONNECT_SLOW_CONSUMER,

    /**
     * Drop the individual message that could not be enqueued in time, but keep the client
     * connection open. Trades message loss for connection stability — useful for best-effort
     * topics where losing a single update is preferable to dropping the subscriber entirely.
     */
    DROP_MESSAGE;

    /**
     * Parses a policy from its configuration string. Falls back to
     * {@link #DISCONNECT_SLOW_CONSUMER} for {@code null}, blank or unrecognized values so a
     * misconfiguration never weakens the default protection.
     *
     * @param raw the configured value (case-insensitive)
     * @return the parsed policy, or the default if the value is unknown
     */
    public static BackpressurePolicy fromConfig(String raw) {
        if (raw == null || raw.isBlank()) {
            return DISCONNECT_SLOW_CONSUMER;
        }
        try {
            return BackpressurePolicy.valueOf(raw.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return DISCONNECT_SLOW_CONSUMER;
        }
    }
}
