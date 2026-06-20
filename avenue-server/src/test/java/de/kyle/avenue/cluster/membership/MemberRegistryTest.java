package de.kyle.avenue.cluster.membership;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for the SWIM incarnation merge rules and listener callbacks of {@link MemberRegistry}
 * (Phase E). All timing is driven by an explicit {@code now} argument, so the tests are deterministic
 * and do not sleep.
 */
class MemberRegistryTest {

    private static final String NODE = "node-x";
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 9000;

    private static final class RecordingListener implements MemberRegistryListener {
        final List<String> events = new ArrayList<>();

        @Override
        public void onAlive(Member m) {
            events.add("ALIVE:" + m.getNodeId() + ":" + m.getIncarnation());
        }

        @Override
        public void onSuspect(Member m) {
            events.add("SUSPECT:" + m.getNodeId() + ":" + m.getIncarnation());
        }

        @Override
        public void onDead(Member m) {
            events.add("DEAD:" + m.getNodeId() + ":" + m.getIncarnation());
        }

        @Override
        public void onLeft(Member m) {
            events.add("LEFT:" + m.getNodeId() + ":" + m.getIncarnation());
        }
    }

    @Test
    void first_sight_creates_member_and_fires_alive() {
        MemberRegistry registry = new MemberRegistry();
        RecordingListener listener = new RecordingListener();
        registry.addListener(listener);

        boolean changed = registry.apply(NODE, HOST, PORT, 100L, MemberState.ALIVE, 1L);

        Assertions.assertTrue(changed);
        Assertions.assertEquals(MemberState.ALIVE, registry.get(NODE).getState());
        Assertions.assertEquals(1, registry.aliveCount());
        Assertions.assertEquals(List.of("ALIVE:" + NODE + ":100"), listener.events);
    }

    @Test
    void alive_only_wins_with_higher_incarnation() {
        MemberRegistry registry = new MemberRegistry();
        registry.apply(NODE, HOST, PORT, 100L, MemberState.ALIVE, 1L);

        // Same incarnation, still ALIVE -> no change.
        Assertions.assertFalse(registry.apply(NODE, HOST, PORT, 100L, MemberState.ALIVE, 2L));
        // Lower incarnation -> ignored.
        Assertions.assertFalse(registry.apply(NODE, HOST, PORT, 50L, MemberState.ALIVE, 3L));
        Assertions.assertEquals(100L, registry.get(NODE).getIncarnation());

        // Higher incarnation -> wins (incarnation advances; state stays ALIVE so no transition event,
        // but the incarnation is updated).
        registry.apply(NODE, HOST, PORT, 200L, MemberState.ALIVE, 4L);
        Assertions.assertEquals(200L, registry.get(NODE).getIncarnation());
    }

    @Test
    void alive_refutes_suspect_at_equal_incarnation() {
        MemberRegistry registry = new MemberRegistry();
        RecordingListener listener = new RecordingListener();
        registry.addListener(listener);

        registry.apply(NODE, HOST, PORT, 100L, MemberState.ALIVE, 1L);
        registry.apply(NODE, HOST, PORT, 100L, MemberState.SUSPECT, 2L);
        Assertions.assertEquals(MemberState.SUSPECT, registry.get(NODE).getState());

        // ALIVE at the SAME incarnation must refute SUSPECT (current state != ALIVE rule).
        boolean changed = registry.apply(NODE, HOST, PORT, 100L, MemberState.ALIVE, 3L);
        Assertions.assertTrue(changed);
        Assertions.assertEquals(MemberState.ALIVE, registry.get(NODE).getState());
    }

    @Test
    void suspect_applies_when_incarnation_at_least_current() {
        MemberRegistry registry = new MemberRegistry();
        registry.apply(NODE, HOST, PORT, 100L, MemberState.ALIVE, 1L);

        // Stale SUSPECT (lower incarnation) is ignored.
        Assertions.assertFalse(registry.apply(NODE, HOST, PORT, 99L, MemberState.SUSPECT, 2L));
        Assertions.assertEquals(MemberState.ALIVE, registry.get(NODE).getState());

        // Equal incarnation SUSPECT applies.
        Assertions.assertTrue(registry.apply(NODE, HOST, PORT, 100L, MemberState.SUSPECT, 3L));
        Assertions.assertEquals(MemberState.SUSPECT, registry.get(NODE).getState());
    }

    @Test
    void dead_is_sticky() {
        MemberRegistry registry = new MemberRegistry();
        registry.apply(NODE, HOST, PORT, 100L, MemberState.ALIVE, 1L);
        registry.apply(NODE, HOST, PORT, 100L, MemberState.DEAD, 2L);
        Assertions.assertEquals(MemberState.DEAD, registry.get(NODE).getState());

        // ALIVE at the same incarnation must NOT revive a DEAD member (DEAD wins; ALIVE rule requires
        // strictly higher incarnation here because current state is DEAD and incarnation is equal).
        Assertions.assertFalse(registry.apply(NODE, HOST, PORT, 100L, MemberState.ALIVE, 3L));
        Assertions.assertEquals(MemberState.DEAD, registry.get(NODE).getState());

        // SUSPECT must never override DEAD.
        Assertions.assertFalse(registry.apply(NODE, HOST, PORT, 200L, MemberState.SUSPECT, 4L));
        Assertions.assertEquals(MemberState.DEAD, registry.get(NODE).getState());

        // A genuine restart with a STRICTLY higher incarnation ALIVE revives it.
        Assertions.assertTrue(registry.apply(NODE, HOST, PORT, 101L, MemberState.ALIVE, 5L));
        Assertions.assertEquals(MemberState.ALIVE, registry.get(NODE).getState());
    }

    @Test
    void left_always_applies() {
        MemberRegistry registry = new MemberRegistry();
        registry.apply(NODE, HOST, PORT, 100L, MemberState.ALIVE, 1L);
        Assertions.assertTrue(registry.apply(NODE, HOST, PORT, 100L, MemberState.LEFT, 2L));
        Assertions.assertEquals(MemberState.LEFT, registry.get(NODE).getState());
    }

    @Test
    void callbacks_fire_for_each_distinct_transition() {
        MemberRegistry registry = new MemberRegistry();
        RecordingListener listener = new RecordingListener();
        registry.addListener(listener);

        registry.apply(NODE, HOST, PORT, 100L, MemberState.ALIVE, 1L);     // ALIVE
        registry.apply(NODE, HOST, PORT, 100L, MemberState.SUSPECT, 2L);   // SUSPECT
        registry.apply(NODE, HOST, PORT, 100L, MemberState.DEAD, 3L);      // DEAD
        registry.apply(NODE, HOST, PORT, 101L, MemberState.ALIVE, 4L);     // ALIVE (revive)
        registry.apply(NODE, HOST, PORT, 102L, MemberState.LEFT, 5L);      // LEFT

        Assertions.assertEquals(List.of(
                "ALIVE:" + NODE + ":100",
                "SUSPECT:" + NODE + ":100",
                "DEAD:" + NODE + ":100",
                "ALIVE:" + NODE + ":101",
                "LEFT:" + NODE + ":102"
        ), listener.events);
    }

    @Test
    void reap_dead_removes_terminal_members_after_timeout() {
        MemberRegistry registry = new MemberRegistry();
        registry.apply("a", HOST, PORT, 1L, MemberState.DEAD, 1_000L);
        registry.apply("b", HOST, PORT, 1L, MemberState.LEFT, 1_000L);
        registry.apply("c", HOST, PORT, 1L, MemberState.ALIVE, 1_000L);

        // Not yet past the timeout (now=1_500, timeout=1_000 -> threshold 2_000).
        Assertions.assertTrue(registry.reapDead(1_000L, 1_500L).isEmpty());
        Assertions.assertNotNull(registry.get("a"));

        // Past the timeout: a and b are reaped, c (ALIVE) survives.
        List<String> reaped = registry.reapDead(1_000L, 2_500L);
        Assertions.assertEquals(2, reaped.size());
        Assertions.assertTrue(reaped.contains("a"));
        Assertions.assertTrue(reaped.contains("b"));
        Assertions.assertNull(registry.get("a"));
        Assertions.assertNull(registry.get("b"));
        Assertions.assertNotNull(registry.get("c"));
    }

    @Test
    void gossip_update_applies_via_merge_rules() {
        MemberRegistry registry = new MemberRegistry();
        GossipUpdate alive = new GossipUpdate(NODE, "ALIVE", 100L, HOST, PORT);
        Assertions.assertTrue(registry.applyGossip(alive, 1L));
        Assertions.assertEquals(MemberState.ALIVE, registry.get(NODE).getState());

        GossipUpdate staleSuspect = new GossipUpdate(NODE, "SUSPECT", 50L, HOST, PORT);
        Assertions.assertFalse(registry.applyGossip(staleSuspect, 2L));
        Assertions.assertEquals(MemberState.ALIVE, registry.get(NODE).getState());
    }
}
