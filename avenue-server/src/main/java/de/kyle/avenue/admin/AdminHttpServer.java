package de.kyle.avenue.admin;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import de.kyle.avenue.admin.dto.MemberRegistrySnapshot;
import de.kyle.avenue.admin.dto.MemberSnapshot;
import de.kyle.avenue.admin.dto.PeerSnapshot;
import de.kyle.avenue.admin.dto.RoutingSnapshot;
import de.kyle.avenue.config.AdminConfig;
import de.kyle.avenue.metrics.AvenueMetrics;
import de.kyle.avenue.metrics.ClusterMetrics;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Read-only admin introspection HTTP server (Phase F).
 * <p>
 * Built entirely on the JDK's {@link com.sun.net.httpserver.HttpServer} — <b>no new Maven
 * dependency</b>. It is <b>disabled by default</b> and binds to loopback ({@code 127.0.0.1}) unless
 * configured otherwise. Every endpoint is a side-effect-free {@code GET} that pulls an immutable
 * snapshot DTO from the cluster, so the admin layer can never mutate or alias live state and is safe
 * to scrape continuously.
 * <h2>Endpoints</h2>
 * <ul>
 *   <li>{@code GET /health} — {@code {status,nodeId,activePeers,membersAlive,membersSuspect,
 *       membersDead}}; {@code 200} when healthy, {@code 503} when self-unhealthy.</li>
 *   <li>{@code GET /metrics} — flat JSON of every {@link AvenueMetrics} + {@link ClusterMetrics}
 *       getter. With {@code Accept: text/plain} (or via {@code /metrics/prometheus}) it returns the
 *       same numbers in dependency-free Prometheus text exposition format.</li>
 *   <li>{@code GET /cluster/members} — the SWIM member list from a {@link MemberRegistrySnapshot}.</li>
 *   <li>{@code GET /cluster/routing} — the interest {@link RoutingSnapshot}
 *       ({@code topic -> interested nodeIds}) plus {@code routingTableTopicCount}.</li>
 *   <li>{@code GET /cluster/peers} — per-peer-link details from {@link PeerSnapshot}s.</li>
 * </ul>
 * <h2>Optional auth</h2>
 * When {@code admin.http.secret} is set every request must carry a matching {@code X-Admin-Secret}
 * header; it is compared in constant time via {@link MessageDigest#isEqual}. A missing/wrong secret
 * yields {@code 401}.
 */
public final class AdminHttpServer {

    private static final Logger log = LoggerFactory.getLogger(AdminHttpServer.class);

    /**
     * Read-only data source the admin server scrapes. Implemented by the node wiring
     * ({@code ClusterNode} for the full cluster surface; a metrics-only adapter for the single-node
     * path). The cluster-specific methods may return {@code null} / empty when clustering is off so a
     * single-node server can still expose {@code /health} and {@code /metrics}.
     */
    public interface AdminDataSource {

        String nodeId();

        AvenueMetrics avenueMetrics();

        /** {@code null} when clustering is disabled. */
        ClusterMetrics clusterMetrics();

        int activePeerCount();

        /** {@code null} when clustering is disabled. */
        MemberRegistrySnapshot members();

        /** {@code null} when clustering is disabled. */
        RoutingSnapshot routing();

        /** Empty when clustering is disabled. */
        List<PeerSnapshot> peers();
    }

    private final AdminConfig config;
    private final AdminDataSource source;
    private volatile HttpServer server;

    public AdminHttpServer(AdminConfig config, AdminDataSource source) {
        this.config = config;
        this.source = source;
    }

    /**
     * Binds and starts the HTTP server on a small fixed-size virtual-thread executor. No-op when the
     * admin endpoint is disabled. Idempotent.
     */
    public synchronized void start() {
        if (!config.enabled() || server != null) {
            return;
        }
        try {
            HttpServer s = HttpServer.create(new InetSocketAddress(config.bindAddress(), config.port()), 0);
            s.createContext("/health", new GuardedHandler(this::handleHealth));
            s.createContext("/metrics/prometheus", new GuardedHandler(this::handlePrometheus));
            s.createContext("/metrics", new GuardedHandler(this::handleMetrics));
            s.createContext("/cluster/members", new GuardedHandler(this::handleMembers));
            s.createContext("/cluster/routing", new GuardedHandler(this::handleRouting));
            s.createContext("/cluster/peers", new GuardedHandler(this::handlePeers));
            s.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            s.start();
            this.server = s;
            log.info("Admin HTTP introspection bound on {}:{} (auth={})",
                    config.bindAddress(), s.getAddress().getPort(), config.requiresSecret());
        } catch (IOException e) {
            log.error("Cannot start admin HTTP server on {}:{}: {}",
                    config.bindAddress(), config.port(), e.getMessage());
        }
    }

    /** Stops the HTTP server (immediate). Safe to call when never started. */
    public synchronized void stop() {
        HttpServer s = this.server;
        if (s != null) {
            s.stop(0);
            this.server = null;
            log.info("Admin HTTP introspection stopped");
        }
    }

    /** Actual bound port, or {@code -1} if not running (resolves an ephemeral {@code 0} port). */
    public int getBoundPort() {
        HttpServer s = this.server;
        return s != null ? s.getAddress().getPort() : -1;
    }

    // ------------------------------------------------------------------
    // Endpoint handlers
    // ------------------------------------------------------------------

    private void handleHealth(HttpExchange exchange) throws IOException {
        MemberRegistrySnapshot members = source.members();
        int alive = members != null ? members.alivecount() : 0;
        int suspect = members != null ? members.suspectCount() : 0;
        int dead = members != null ? members.deadCount() : 0;

        // A node is "healthy" as long as it is up and serving; we do not have a self-failure signal
        // beyond process liveness, so report UP. The 503 path is wired for future self-unhealthy
        // signals (e.g. a degraded bind) without changing the contract.
        boolean healthy = isSelfHealthy();
        JSONObject body = new JSONObject();
        body.put("status", healthy ? "UP" : "DOWN");
        body.put("nodeId", source.nodeId());
        body.put("activePeers", source.activePeerCount());
        body.put("membersAlive", alive);
        body.put("membersSuspect", suspect);
        body.put("membersDead", dead);
        sendJson(exchange, healthy ? 200 : 503, body.toString());
    }

    /**
     * Self-health predicate. Currently always {@code true} (process is up if this handler runs); kept
     * as a seam so a future degraded-state signal can flip {@code /health} to {@code 503} without
     * touching the endpoint contract.
     */
    private boolean isSelfHealthy() {
        return true;
    }

    private void handleMetrics(HttpExchange exchange) throws IOException {
        String accept = exchange.getRequestHeaders().getFirst("Accept");
        if (accept != null && accept.contains("text/plain")) {
            sendText(exchange, 200, buildPrometheus());
            return;
        }
        sendJson(exchange, 200, buildMetricsJson().toString());
    }

    private void handlePrometheus(HttpExchange exchange) throws IOException {
        sendText(exchange, 200, buildPrometheus());
    }

    private void handleMembers(HttpExchange exchange) throws IOException {
        MemberRegistrySnapshot members = source.members();
        JSONObject body = new JSONObject();
        JSONArray arr = new JSONArray();
        if (members != null) {
            body.put("membersAlive", members.alivecount());
            body.put("membersSuspect", members.suspectCount());
            body.put("membersDead", members.deadCount());
            for (MemberSnapshot m : members.members()) {
                JSONObject jm = new JSONObject();
                jm.put("nodeId", m.nodeId());
                jm.put("state", m.state());
                jm.put("incarnation", m.incarnation());
                jm.put("host", m.host());
                jm.put("clusterPort", m.clusterPort());
                jm.put("stateChangedAtMillis", m.stateChangedAtMillis());
                arr.put(jm);
            }
        } else {
            body.put("membersAlive", 0);
            body.put("membersSuspect", 0);
            body.put("membersDead", 0);
        }
        body.put("members", arr);
        sendJson(exchange, 200, body.toString());
    }

    private void handleRouting(HttpExchange exchange) throws IOException {
        RoutingSnapshot routing = source.routing();
        JSONObject body = new JSONObject();
        JSONObject topics = new JSONObject();
        int topicCount = 0;
        if (routing != null) {
            topicCount = routing.topicCount();
            for (Map.Entry<String, List<String>> e : routing.topicToNodes().entrySet()) {
                topics.put(e.getKey(), new JSONArray(e.getValue()));
            }
        }
        body.put("routingTableTopicCount", topicCount);
        body.put("topics", topics);
        sendJson(exchange, 200, body.toString());
    }

    private void handlePeers(HttpExchange exchange) throws IOException {
        JSONArray arr = new JSONArray();
        for (PeerSnapshot p : source.peers()) {
            JSONObject jp = new JSONObject();
            jp.put("nodeId", p.nodeId());
            jp.put("running", p.running());
            jp.put("replayDepth", p.replayDepth());
            jp.put("ingestDepth", p.ingestDepth());
            jp.put("outboundLinkSeq", p.outboundLinkSeq());
            jp.put("contiguousLinkSeq", p.contiguousLinkSeq());
            arr.put(jp);
        }
        JSONObject body = new JSONObject();
        body.put("activePeers", source.activePeerCount());
        body.put("peers", arr);
        sendJson(exchange, 200, body.toString());
    }

    // ------------------------------------------------------------------
    // Metrics serialization (flat JSON + Prometheus text)
    // ------------------------------------------------------------------

    /** Flat map of every metric getter, name -> long. Single source of truth for both formats. */
    private Map<String, Long> collectMetrics() {
        java.util.LinkedHashMap<String, Long> m = new java.util.LinkedHashMap<>();
        AvenueMetrics a = source.avenueMetrics();
        m.put("messagesPublished", a.getMessagesPublished());
        m.put("messagesDelivered", a.getMessagesDelivered());
        m.put("droppedMessages", a.getDroppedMessages());
        m.put("slowConsumerDisconnects", a.getSlowConsumerDisconnects());
        m.put("totalConnectionsAccepted", a.getTotalConnectionsAccepted());
        m.put("connectionsRejected", a.getConnectionsRejected());
        m.put("activeConnections", a.getActiveConnections());
        m.put("subscriptionCount", a.getSubscriptionCount());
        m.put("maxOutboundQueueDepth", a.getMaxOutboundQueueDepth());

        ClusterMetrics c = source.clusterMetrics();
        if (c != null) {
            m.put("cluster_messagesForwarded", c.getMessagesForwarded());
            m.put("cluster_messagesReceived", c.getMessagesReceived());
            m.put("cluster_messagesDeduped", c.getMessagesDeduped());
            m.put("cluster_messagesDropped", c.getMessagesDropped());
            m.put("cluster_handshakeAuthFailures", c.getHandshakeAuthFailures());
            m.put("cluster_activePeerLinks", c.getActivePeerLinks());
            m.put("cluster_replayBufferDepth", c.getReplayBufferDepth());
            m.put("cluster_backfillMessages", c.getClusterBackfillMessages());
            m.put("cluster_gapEvents", c.getClusterGapEvents());
            m.put("cluster_slowPeerStalls", c.getClusterSlowPeerStalls());
            m.put("cluster_acksSent", c.getAcksSent());
            m.put("cluster_acksReceived", c.getAcksReceived());
            m.put("cluster_interestUpdatesSent", c.getInterestUpdatesSent());
            m.put("cluster_interestUpdatesReceived", c.getInterestUpdatesReceived());
            m.put("cluster_interestRoutedSkipped", c.getInterestRoutedSkipped());
            m.put("cluster_routingTableTopicCount", c.getRoutingTableTopicCount());
            m.put("cluster_membersAlive", c.getMembersAlive());
            m.put("cluster_membersSuspect", c.getMembersSuspect());
            m.put("cluster_membersDead", c.getMembersDead());
            m.put("cluster_joinEvents", c.getJoinEvents());
            m.put("cluster_leaveEvents", c.getLeaveEvents());
            m.put("cluster_suspectEvents", c.getSuspectEvents());
            m.put("cluster_deadEvents", c.getDeadEvents());
            m.put("cluster_indirectProbesSent", c.getIndirectProbesSent());
            m.put("cluster_gossipMessagesSent", c.getGossipMessagesSent());
            m.put("cluster_gossipMessagesReceived", c.getGossipMessagesReceived());
            m.put("cluster_incarnationConflicts", c.getIncarnationConflicts());
        }
        return m;
    }

    private JSONObject buildMetricsJson() {
        JSONObject body = new JSONObject();
        for (Map.Entry<String, Long> e : collectMetrics().entrySet()) {
            body.put(e.getKey(), e.getValue());
        }
        return body;
    }

    private String buildPrometheus() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Long> e : collectMetrics().entrySet()) {
            String metric = "avenue_" + e.getKey();
            sb.append("# TYPE ").append(metric).append(" gauge\n");
            sb.append(metric).append(' ').append(e.getValue()).append('\n');
        }
        return sb.toString();
    }

    // ------------------------------------------------------------------
    // HTTP plumbing
    // ------------------------------------------------------------------

    /** Wraps a handler with method-, auth- and error-guards so each endpoint stays focused. */
    private final class GuardedHandler implements HttpHandler {
        private final ThrowingHandler delegate;

        GuardedHandler(ThrowingHandler delegate) {
            this.delegate = delegate;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                    sendText(exchange, 405, "method not allowed\n");
                    return;
                }
                if (!authorized(exchange)) {
                    sendText(exchange, 401, "unauthorized\n");
                    return;
                }
                delegate.handle(exchange);
            } catch (Exception e) {
                log.warn("Admin endpoint error on {}: {}", exchange.getRequestURI(), e.getMessage());
                try {
                    sendText(exchange, 500, "internal error\n");
                } catch (IOException ignored) {
                    // client gone
                }
            } finally {
                exchange.close();
            }
        }
    }

    @FunctionalInterface
    private interface ThrowingHandler {
        void handle(HttpExchange exchange) throws IOException;
    }

    /** Constant-time secret check; returns {@code true} when no secret is configured. */
    private boolean authorized(HttpExchange exchange) {
        if (!config.requiresSecret()) {
            return true;
        }
        String provided = exchange.getRequestHeaders().getFirst("X-Admin-Secret");
        if (provided == null) {
            return false;
        }
        return MessageDigest.isEqual(
                provided.getBytes(StandardCharsets.UTF_8),
                config.secret().getBytes(StandardCharsets.UTF_8));
    }

    private void sendJson(HttpExchange exchange, int status, String body) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        write(exchange, status, body.getBytes(StandardCharsets.UTF_8));
    }

    private void sendText(HttpExchange exchange, int status, String body) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4");
        write(exchange, status, body.getBytes(StandardCharsets.UTF_8));
    }

    private void write(HttpExchange exchange, int status, byte[] payload) throws IOException {
        exchange.sendResponseHeaders(status, payload.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(payload);
        }
    }
}
