package de.kyle.avenue.integration;

import de.kyle.avenue.cluster.ClusterNode;
import de.kyle.avenue.cluster.ReplayBuffer;
import de.kyle.avenue.config.AdminConfig;
import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.config.ClusterTuning;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * Phase F — admin HTTP introspection integration test.
 * <p>
 * Starts a two-node cluster with the read-only admin endpoint enabled on an ephemeral port on node1,
 * waits for SWIM convergence, then scrapes {@code /health}, {@code /cluster/members}, {@code /metrics}
 * and {@code /cluster/peers} with the JDK {@link HttpClient} and asserts the returned fields agree
 * with the live {@link ClusterNode#getActivePeerCount()} and the {@code ClusterMetrics} getters.
 * Also covers the optional {@code X-Admin-Secret} auth (401 without, 200 with) and the Prometheus
 * text format. All synchronization is via bounded polling, never fixed sleeps.
 */
class AdminIntrospectionTest {

    private static final String HOST = "127.0.0.1";
    private static final String SECRET = "int-test-secret";
    private static final String TOKEN = "int-test-token";
    private static final String CLUSTER_SECRET = "cluster-secret-42";
    private static final String ADMIN_SECRET = "admin-secret-99";
    private static final int PACKET_SIZE = 65536;
    private static final long TIMEOUT = 20;
    private static final TimeUnit UNIT = TimeUnit.SECONDS;

    private final List<ClusterNode> nodes = new ArrayList<>();
    private final HttpClient http = HttpClient.newHttpClient();

    @AfterEach
    void stopNodes() {
        for (int i = nodes.size() - 1; i >= 0; i--) {
            try {
                nodes.get(i).stop();
            } catch (Exception ignored) {
            }
        }
        nodes.clear();
    }

    private static ClusterTuning fastSwimTuning() {
        return new ClusterTuning(
                ClusterTuning.DEFAULT_REPLAY_CAPACITY,
                ReplayBuffer.BackpressurePolicy.BLOCK,
                ClusterTuning.DEFAULT_REPLAY_OFFER_TIMEOUT_MS,
                ClusterTuning.DEFAULT_ACK_INTERVAL_MS,
                false,
                ClusterTuning.DEFAULT_ORIGIN_EXPIRY_MS,
                ClusterTuning.DEFAULT_INTEREST_SYNC_INTERVAL_MS,
                ClusterTuning.DEFAULT_INTEREST_BROADCAST_GRACE_MS,
                200L, 100L, 2, 500L, 2, 2_000L, "", 1_048_576);
    }

    private AvenueConfig config(String nodeId, List<String> peers, AdminConfig admin) {
        return new AvenueConfig(
                PACKET_SIZE, true, SECRET, TOKEN,
                0, 8192, 100,
                true, nodeId, 0, peers, CLUSTER_SECRET, 500L,
                0L, 10_000, null, 0L,
                false, "", "",
                false, "", "", "", "",
                fastSwimTuning(), admin);
    }

    private ClusterNode startNode(String nodeId, List<String> seeds, AdminConfig admin) {
        ClusterNode node = new ClusterNode(config(nodeId, seeds, admin));
        node.start();
        nodes.add(node);
        return node;
    }

    private static String addr(ClusterNode node) {
        return HOST + ":" + node.getClusterPort();
    }

    private void awaitTrue(BooleanSupplier condition, String message) throws InterruptedException {
        long deadline = System.nanoTime() + UNIT.toNanos(TIMEOUT);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        Assertions.assertTrue(condition.getAsBoolean(), message);
    }

    private HttpResponse<String> get(int port, String path, String secret) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(URI.create("http://" + HOST + ":" + port + path))
                .GET();
        if (secret != null) {
            b.header("X-Admin-Secret", secret);
        }
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void endpoints_reflect_live_cluster_state() throws Exception {
        // node1 exposes the admin endpoint (ephemeral port, no secret); node2 joins via node1.
        AdminConfig admin = new AdminConfig(true, 0, "127.0.0.1", "");
        ClusterNode node1 = startNode("node-1", List.of(), admin);
        ClusterNode node2 = startNode("node-2", List.of(addr(node1)), AdminConfig.disabled());

        Assertions.assertTrue(node1.awaitMembership(1, TIMEOUT, UNIT), "node1 must see node2");
        Assertions.assertTrue(node2.awaitMembership(1, TIMEOUT, UNIT), "node2 must see node1");
        awaitTrue(() -> node1.getActivePeerCount() == 1, "node1 must have one active peer");

        int adminPort = node1.getAdminHttpPort();
        Assertions.assertTrue(adminPort > 0, "admin port must be bound");

        // /health
        HttpResponse<String> health = get(adminPort, "/health", null);
        Assertions.assertEquals(200, health.statusCode());
        JSONObject healthBody = new JSONObject(health.body());
        Assertions.assertEquals("UP", healthBody.getString("status"));
        Assertions.assertEquals("node-1", healthBody.getString("nodeId"));
        Assertions.assertEquals(node1.getActivePeerCount(), healthBody.getInt("activePeers"));
        Assertions.assertEquals(node1.getMemberCount(), healthBody.getInt("membersAlive"));

        // /cluster/members — must include node-2 ALIVE and agree on the alive count.
        awaitTrue(() -> {
            try {
                JSONObject body = new JSONObject(get(adminPort, "/cluster/members", null).body());
                return body.getInt("membersAlive") == node1.getMemberCount()
                        && body.getJSONArray("members").length() >= 1;
            } catch (Exception e) {
                return false;
            }
        }, "members endpoint must reflect the alive membership");

        JSONObject members = new JSONObject(get(adminPort, "/cluster/members", null).body());
        boolean sawNode2Alive = false;
        var arr = members.getJSONArray("members");
        for (int i = 0; i < arr.length(); i++) {
            JSONObject m = arr.getJSONObject(i);
            if ("node-2".equals(m.getString("nodeId")) && "ALIVE".equals(m.getString("state"))) {
                sawNode2Alive = true;
                Assertions.assertTrue(m.has("incarnation"));
                Assertions.assertTrue(m.has("clusterPort"));
                Assertions.assertTrue(m.has("stateChangedAtMillis"));
            }
        }
        Assertions.assertTrue(sawNode2Alive, "members must list node-2 as ALIVE");

        // /metrics (JSON) — activePeerLinks must match the live gauge getter.
        JSONObject metrics = new JSONObject(get(adminPort, "/metrics", null).body());
        Assertions.assertEquals(
                node1.getClusterMetrics().getActivePeerLinks(),
                metrics.getLong("cluster_activePeerLinks"),
                "metrics activePeerLinks must equal the live ClusterMetrics getter");
        Assertions.assertTrue(metrics.has("messagesPublished"), "flat AvenueMetrics must be present");

        // /metrics/prometheus — text exposition format.
        HttpResponse<String> prom = get(adminPort, "/metrics/prometheus", null);
        Assertions.assertEquals(200, prom.statusCode());
        Assertions.assertTrue(prom.body().contains("avenue_cluster_activePeerLinks"),
                "prometheus output must carry the cluster gauges");

        // /cluster/peers — one running peer link with the expected node id.
        JSONObject peers = new JSONObject(get(adminPort, "/cluster/peers", null).body());
        Assertions.assertEquals(node1.getActivePeerCount(), peers.getInt("activePeers"));
        Assertions.assertTrue(peers.getJSONArray("peers").length() >= 1);
        Assertions.assertEquals("node-2", peers.getJSONArray("peers").getJSONObject(0).getString("nodeId"));
    }

    @Test
    void admin_secret_is_enforced() throws Exception {
        AdminConfig admin = new AdminConfig(true, 0, "127.0.0.1", ADMIN_SECRET);
        ClusterNode node1 = startNode("node-1", List.of(), admin);
        int adminPort = node1.getAdminHttpPort();
        Assertions.assertTrue(adminPort > 0);

        // No secret -> 401.
        Assertions.assertEquals(401, get(adminPort, "/health", null).statusCode());
        // Wrong secret -> 401.
        Assertions.assertEquals(401, get(adminPort, "/health", "wrong").statusCode());
        // Correct secret -> 200.
        HttpResponse<String> ok = get(adminPort, "/health", ADMIN_SECRET);
        Assertions.assertEquals(200, ok.statusCode());
        Assertions.assertEquals("node-1", new JSONObject(ok.body()).getString("nodeId"));
    }
}
