package de.kyle.avenue.cluster;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

/**
 * Unit tests for {@link InterestRoutingTable}: version-based idempotency of deltas, full-state
 * replacement and node removal. Pure in-memory, no I/O.
 */
class InterestRoutingTableTest {

    @Test
    void delta_add_and_remove_updates_forward_index() {
        InterestRoutingTable table = new InterestRoutingTable();
        table.applyDelta("node-A", 1, InterestRoutingTable.Op.ADD, "orders");

        Assertions.assertTrue(table.peersFor("orders").contains("node-A"));
        Assertions.assertEquals(1, table.topicCount());

        table.applyDelta("node-A", 2, InterestRoutingTable.Op.REMOVE, "orders");
        Assertions.assertFalse(table.peersFor("orders").contains("node-A"));
        Assertions.assertEquals(0, table.topicCount(), "empty topic must be pruned");
    }

    @Test
    void stale_or_out_of_order_delta_is_ignored_by_version() {
        InterestRoutingTable table = new InterestRoutingTable();
        // Apply version 5 (ADD), then a stale REMOVE at version 3 must be ignored.
        table.applyDelta("node-A", 5, InterestRoutingTable.Op.ADD, "t");
        table.applyDelta("node-A", 3, InterestRoutingTable.Op.REMOVE, "t");
        Assertions.assertTrue(table.peersFor("t").contains("node-A"),
                "an out-of-order lower-version delta must not undo a newer one");

        // An equal version is also ignored (only strictly greater wins).
        table.applyDelta("node-A", 5, InterestRoutingTable.Op.REMOVE, "t");
        Assertions.assertTrue(table.peersFor("t").contains("node-A"));

        // A strictly newer version applies.
        table.applyDelta("node-A", 6, InterestRoutingTable.Op.REMOVE, "t");
        Assertions.assertFalse(table.peersFor("t").contains("node-A"));
    }

    @Test
    void replace_full_state_swaps_the_entire_interest_set() {
        InterestRoutingTable table = new InterestRoutingTable();
        table.replaceFullState("node-A", 1, Set.of("a", "b", "c"));
        Assertions.assertTrue(table.peersFor("a").contains("node-A"));
        Assertions.assertTrue(table.peersFor("b").contains("node-A"));
        Assertions.assertTrue(table.peersFor("c").contains("node-A"));

        // Newer snapshot keeps only "b" and adds "d"; "a" and "c" must drop.
        table.replaceFullState("node-A", 2, Set.of("b", "d"));
        Assertions.assertFalse(table.peersFor("a").contains("node-A"));
        Assertions.assertTrue(table.peersFor("b").contains("node-A"));
        Assertions.assertFalse(table.peersFor("c").contains("node-A"));
        Assertions.assertTrue(table.peersFor("d").contains("node-A"));
    }

    @Test
    void replace_full_state_ignores_older_version() {
        InterestRoutingTable table = new InterestRoutingTable();
        table.replaceFullState("node-A", 10, Set.of("x"));
        // An older full sync must not regress fresher state.
        table.replaceFullState("node-A", 4, Set.of("y", "z"));
        Assertions.assertTrue(table.peersFor("x").contains("node-A"));
        Assertions.assertFalse(table.peersFor("y").contains("node-A"));
    }

    @Test
    void remove_node_drops_all_interest_and_resets_version() {
        InterestRoutingTable table = new InterestRoutingTable();
        table.applyDelta("node-A", 1, InterestRoutingTable.Op.ADD, "a");
        table.applyDelta("node-A", 2, InterestRoutingTable.Op.ADD, "b");
        table.applyDelta("node-B", 1, InterestRoutingTable.Op.ADD, "a");

        table.removeNode("node-A");
        Assertions.assertFalse(table.peersFor("a").contains("node-A"));
        Assertions.assertFalse(table.peersFor("b").contains("node-A"));
        // node-B's interest in "a" must remain.
        Assertions.assertTrue(table.peersFor("a").contains("node-B"));

        // After removal the version watermark is forgotten, so a low-version reconnect applies.
        table.applyDelta("node-A", 1, InterestRoutingTable.Op.ADD, "c");
        Assertions.assertTrue(table.peersFor("c").contains("node-A"),
                "a reconnecting node id must be able to start its version space over");
    }

    @Test
    void peers_for_is_exact_match_no_wildcards() {
        InterestRoutingTable table = new InterestRoutingTable();
        table.applyDelta("node-A", 1, InterestRoutingTable.Op.ADD, "orders.eu");
        // No prefix/wildcard semantics: only the exact key matches.
        Assertions.assertTrue(table.peersFor("orders.eu").contains("node-A"));
        Assertions.assertTrue(table.peersFor("orders").isEmpty());
        Assertions.assertTrue(table.peersFor("orders.eu.west").isEmpty());
    }

    @Test
    void unknown_topic_returns_empty_set() {
        InterestRoutingTable table = new InterestRoutingTable();
        Assertions.assertTrue(table.peersFor("nope").isEmpty());
    }
}
