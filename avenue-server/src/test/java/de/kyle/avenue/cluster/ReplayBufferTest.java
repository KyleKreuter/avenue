package de.kyle.avenue.cluster;

import de.kyle.avenue.metrics.ClusterMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Unit tests for {@link ReplayBuffer}: append/ackUpTo/replayFrom, BLOCK backpressure with
 * ack-driven advance, DROP_GAP, and the gap signal on an evicted resume point.
 */
class ReplayBufferTest {

    private static byte[] payload(long seq) {
        return ("p" + seq).getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void append_then_replayFrom_returns_entries_in_order() {
        ClusterMetrics metrics = new ClusterMetrics();
        ReplayBuffer buf = new ReplayBuffer(16, ReplayBuffer.BackpressurePolicy.BLOCK, 1000, metrics);

        for (long s = 1; s <= 5; s++) {
            Assertions.assertTrue(buf.append(s, payload(s)));
        }
        Assertions.assertEquals(5, buf.size());

        ReplayBuffer.ReplayResult result = buf.replayFrom(2);
        Assertions.assertFalse(result.gap());
        Assertions.assertEquals(List.of(3L, 4L, 5L),
                result.entries().stream().map(ReplayBuffer.Entry::seq).toList());
    }

    @Test
    void ackUpTo_evicts_head_and_is_cumulative_and_monotonic() {
        ClusterMetrics metrics = new ClusterMetrics();
        ReplayBuffer buf = new ReplayBuffer(16, ReplayBuffer.BackpressurePolicy.BLOCK, 1000, metrics);
        for (long s = 1; s <= 5; s++) {
            buf.append(s, payload(s));
        }
        buf.ackUpTo(3);
        Assertions.assertEquals(2, buf.size());
        // Stale ack ignored (monotonic).
        buf.ackUpTo(1);
        Assertions.assertEquals(2, buf.size());

        ReplayBuffer.ReplayResult result = buf.replayFrom(3);
        Assertions.assertFalse(result.gap());
        Assertions.assertEquals(List.of(4L, 5L),
                result.entries().stream().map(ReplayBuffer.Entry::seq).toList());
    }

    @Test
    void replayFrom_caughtUp_returns_empty() {
        ClusterMetrics metrics = new ClusterMetrics();
        ReplayBuffer buf = new ReplayBuffer(16, ReplayBuffer.BackpressurePolicy.BLOCK, 1000, metrics);
        for (long s = 1; s <= 3; s++) {
            buf.append(s, payload(s));
        }
        ReplayBuffer.ReplayResult result = buf.replayFrom(3);
        Assertions.assertFalse(result.gap());
        Assertions.assertTrue(result.entries().isEmpty());
    }

    @Test
    void replayFrom_evicted_range_signals_gap() {
        ClusterMetrics metrics = new ClusterMetrics();
        ReplayBuffer buf = new ReplayBuffer(16, ReplayBuffer.BackpressurePolicy.BLOCK, 1000, metrics);
        for (long s = 1; s <= 5; s++) {
            buf.append(s, payload(s));
        }
        buf.ackUpTo(3); // entries 1..3 gone; ring head is seq=4
        // Peer asks to resume from 1 (i.e. wants 2..) but 2,3 are evicted -> gap at firstAvailable=4.
        ReplayBuffer.ReplayResult result = buf.replayFrom(1);
        Assertions.assertTrue(result.gap());
        Assertions.assertEquals(4L, result.firstAvailableSeq());
    }

    @Test
    void dropGap_policy_drops_when_full_and_records_gap() {
        ClusterMetrics metrics = new ClusterMetrics();
        ReplayBuffer buf = new ReplayBuffer(3, ReplayBuffer.BackpressurePolicy.DROP_GAP, 1000, metrics);
        Assertions.assertTrue(buf.append(1, payload(1)));
        Assertions.assertTrue(buf.append(2, payload(2)));
        Assertions.assertTrue(buf.append(3, payload(3)));
        // Ring full -> dropped, gap recorded, link not killed.
        Assertions.assertFalse(buf.append(4, payload(4)));
        Assertions.assertEquals(3, buf.size());
        Assertions.assertTrue(metrics.getClusterGapEvents() >= 1);
        // High-water mark still tracks the dropped seq.
        Assertions.assertEquals(4, buf.highestSeq());
    }

    @Test
    void block_policy_appender_unblocks_when_ack_frees_space() throws Exception {
        ClusterMetrics metrics = new ClusterMetrics();
        ReplayBuffer buf = new ReplayBuffer(2, ReplayBuffer.BackpressurePolicy.BLOCK, 5000, metrics);
        Assertions.assertTrue(buf.append(1, payload(1)));
        Assertions.assertTrue(buf.append(2, payload(2)));

        CountDownLatch started = new CountDownLatch(1);
        AtomicBoolean appended = new AtomicBoolean(false);
        Thread writer = new Thread(() -> {
            started.countDown();
            boolean ok = buf.append(3, payload(3)); // must block until ack frees space
            appended.set(ok);
        });
        writer.start();
        Assertions.assertTrue(started.await(1, TimeUnit.SECONDS));
        // Give the writer a beat to enter the blocked wait, then confirm it is still blocked.
        Thread.sleep(100);
        Assertions.assertFalse(appended.get(), "append(3) must block while the ring is full");

        // Ack frees the head -> the blocked appender proceeds.
        buf.ackUpTo(1);
        writer.join(2000);
        Assertions.assertTrue(appended.get(), "append(3) must succeed after an ack frees space");
        Assertions.assertEquals(2, buf.size()); // entry 1 acked away, 2 and 3 remain
        Assertions.assertTrue(metrics.getClusterSlowPeerStalls() >= 1);
        Assertions.assertEquals(0, metrics.getClusterGapEvents(), "no gap when space frees in time");
    }

    @Test
    void block_policy_times_out_to_gap_when_no_ack_arrives() {
        ClusterMetrics metrics = new ClusterMetrics();
        ReplayBuffer buf = new ReplayBuffer(2, ReplayBuffer.BackpressurePolicy.BLOCK, 50, metrics);
        Assertions.assertTrue(buf.append(1, payload(1)));
        Assertions.assertTrue(buf.append(2, payload(2)));
        // No ack ever comes; after the offer timeout the entry degrades to a gap (no link kill).
        Assertions.assertFalse(buf.append(3, payload(3)));
        Assertions.assertTrue(metrics.getClusterGapEvents() >= 1);
        Assertions.assertEquals(2, buf.size());
    }
}
