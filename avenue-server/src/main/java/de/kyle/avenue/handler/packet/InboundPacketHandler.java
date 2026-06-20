package de.kyle.avenue.handler.packet;

import de.kyle.avenue.annotation.Secured;
import de.kyle.avenue.annotation.TopicHandler;
import de.kyle.avenue.cluster.ClusterForwarder;
import de.kyle.avenue.handler.authentication.AuthenticationTokenHandler;
import de.kyle.avenue.handler.client.ClientConnectionHandler;
import de.kyle.avenue.handler.packet.auth.AuthTokenRequestInboundPacketHandler;
import de.kyle.avenue.handler.packet.publish.PublishMessageInboundPacketHandler;
import de.kyle.avenue.handler.packet.subscribe.SubscribeInboundPacketHandler;
import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
import de.kyle.avenue.metrics.AvenueMetrics;
import org.json.JSONObject;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * Routes inbound packets to their registered {@link PacketHandler}.
 * <p>
 * The {@link Secured @Secured} / {@link TopicHandler @TopicHandler} annotations on each
 * handler's {@code handle} method are resolved exactly once at registration time and cached
 * as boolean flags inside a {@link RegisteredHandler}. The hot path therefore performs no
 * reflection or annotation scanning per packet.
 * <p>
 * An optional {@link ClusterForwarder} is injected into
 * {@link PublishMessageInboundPacketHandler} to forward local publishes to cluster peers.
 * When clustering is disabled, {@link ClusterForwarder#NOOP} is used automatically.
 */
public class InboundPacketHandler {
    private final Map<String, RegisteredHandler> packethandlerMap = new ConcurrentHashMap<>();
    private final AuthenticationTokenHandler authenticationTokenHandler;

    /**
     * Single-node constructor (no clustering). Fully backwards-compatible with existing callers.
     */
    public InboundPacketHandler(
            AuthenticationTokenHandler authenticationTokenHandler,
            TopicSubscriptionHandler topicSubscriptionHandler,
            ExecutorService executorService
    ) {
        this(authenticationTokenHandler, topicSubscriptionHandler, executorService,
                ClusterForwarder.NOOP, new AvenueMetrics());
    }

    /**
     * Cluster-aware constructor without explicit metrics. Uses a standalone metrics registry.
     */
    public InboundPacketHandler(
            AuthenticationTokenHandler authenticationTokenHandler,
            TopicSubscriptionHandler topicSubscriptionHandler,
            ExecutorService executorService,
            ClusterForwarder clusterForwarder
    ) {
        this(authenticationTokenHandler, topicSubscriptionHandler, executorService,
                clusterForwarder, new AvenueMetrics());
    }

    /**
     * Full constructor. The supplied {@code clusterForwarder} and shared {@code metrics} are
     * injected into the {@link PublishMessageInboundPacketHandler}.
     */
    public InboundPacketHandler(
            AuthenticationTokenHandler authenticationTokenHandler,
            TopicSubscriptionHandler topicSubscriptionHandler,
            ExecutorService executorService,
            ClusterForwarder clusterForwarder,
            AvenueMetrics metrics
    ) {
        this.authenticationTokenHandler = authenticationTokenHandler;
        register("AuthTokenRequestInboundPacket", new AuthTokenRequestInboundPacketHandler(authenticationTokenHandler));
        register("PublishMessageInboundPacket",
                new PublishMessageInboundPacketHandler(topicSubscriptionHandler, executorService, clusterForwarder, metrics));
        register("SubscribeInboundPacket", new SubscribeInboundPacketHandler(topicSubscriptionHandler));
    }

    /**
     * Registers a handler and resolves its annotation flags a single time. Any failure to
     * locate the {@code handle} method is a programming error and fails fast at startup.
     */
    private void register(String packetName, PacketHandler handler) {
        try {
            Method handle = handler.getClass()
                    .getDeclaredMethod("handle", JSONObject.class, ClientConnectionHandler.class);
            boolean secured = Arrays.stream(handle.getAnnotations())
                    .anyMatch(annotation -> annotation.annotationType().equals(Secured.class));
            boolean topicHandler = Arrays.stream(handle.getAnnotations())
                    .anyMatch(annotation -> annotation.annotationType().equals(TopicHandler.class));
            packethandlerMap.put(packetName, new RegisteredHandler(handler, secured, topicHandler));
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("Packet handler " + packetName + " has no handle method", e);
        }
    }

    public void handleInboundPacket(JSONObject packet, ClientConnectionHandler clientConnectionHandler) throws IOException {
        if (!packet.has("header")) {
            throw new IOException("Packet received does not contain a header field");
        }
        Object headerO = packet.get("header");
        if (!(headerO instanceof JSONObject header)) {
            throw new IOException("Packet has header field but was not parsed correctly");
        }
        if (!header.has("name")) {
            throw new IOException("Packet has no name field in the header");
        }
        String packetName = header.getString("name");
        RegisteredHandler registeredHandler = packethandlerMap.get(packetName);
        if (registeredHandler == null) {
            throw new IllegalArgumentException("Packet with the provided name not found");
        }

        // Hot path: only check cached flags, no reflection.
        if (registeredHandler.secured()) {
            verifyToken(header);
        }
        if (registeredHandler.topicHandler() && !header.has("topic")) {
            throw new IllegalArgumentException("Packet does not contain a topic");
        }

        registeredHandler.handler().handle(packet, clientConnectionHandler);
    }

    private void verifyToken(JSONObject header) {
        if (!header.has("token")) {
            throw new IllegalArgumentException("Packet does not contain a token");
        }
        String clientToken = header.getString("token");
        if (!this.authenticationTokenHandler.isValidToken(clientToken)) {
            throw new IllegalArgumentException("Provided token mismatched local token");
        }
    }

    /**
     * Caches a handler together with its annotation-derived flags so the dispatch path never
     * has to touch reflection.
     */
    private record RegisteredHandler(PacketHandler handler, boolean secured, boolean topicHandler) {
    }
}
