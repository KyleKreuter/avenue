package de.kyle.avenue.handler.client;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BackpressurePolicy#fromConfig(String)} — the parsing must be lenient
 * and always fall back to the safe default {@code DISCONNECT_SLOW_CONSUMER}.
 */
class BackpressurePolicyTest {

    @Test
    void parses_known_policies_case_insensitively() {
        Assertions.assertEquals(BackpressurePolicy.DROP_MESSAGE,
                BackpressurePolicy.fromConfig("drop_message"));
        Assertions.assertEquals(BackpressurePolicy.DISCONNECT_SLOW_CONSUMER,
                BackpressurePolicy.fromConfig("Disconnect_Slow_Consumer"));
        Assertions.assertEquals(BackpressurePolicy.DROP_MESSAGE,
                BackpressurePolicy.fromConfig("  DROP_MESSAGE  "));
    }

    @Test
    void falls_back_to_default_for_null_blank_or_unknown() {
        Assertions.assertEquals(BackpressurePolicy.DISCONNECT_SLOW_CONSUMER,
                BackpressurePolicy.fromConfig(null));
        Assertions.assertEquals(BackpressurePolicy.DISCONNECT_SLOW_CONSUMER,
                BackpressurePolicy.fromConfig(""));
        Assertions.assertEquals(BackpressurePolicy.DISCONNECT_SLOW_CONSUMER,
                BackpressurePolicy.fromConfig("nonsense-value"));
    }
}
