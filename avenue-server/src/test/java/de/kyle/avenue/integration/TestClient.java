package de.kyle.avenue.integration;

import de.kyle.avenue.proto.AuthTokenRequest;
import de.kyle.avenue.proto.ClientEnvelope;
import de.kyle.avenue.proto.PublishInbound;
import de.kyle.avenue.proto.PublishOutbound;
import de.kyle.avenue.proto.Subscribe;
import de.kyle.avenue.serialization.PacketFraming;
import de.kyle.avenue.serialization.WireCodec;
import org.json.JSONObject;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Minimal raw-socket test client. It speaks the real Avenue wire protocol
 * ({@link PacketFraming length-prefix framing} + a real {@link ClientEnvelope} protobuf payload)
 * over a real TCP socket, so the integration tests exercise the full server end to end (framing,
 * (de)serialization, auth, routing, fan-out) rather than mocks.
 * <p>
 * The {@link de.kyle.avenue.AvenueClient} singleton is intentionally NOT reused here: being a
 * singleton it cannot represent two independent clients in the same JVM, which the fan-out
 * tests require.
 * <p>
 * After the protobuf cutover the inbound/outbound payloads are {@code ClientEnvelope}s, but the
 * test-facing API is unchanged: server-to-client envelopes are converted into the historic
 * {@code {"header":{"name",...},"body":{...}}} {@link JSONObject} shape so the existing test
 * assertions (which read {@code header.name}, {@code header.topic}, {@code body.data}, ...) keep
 * working verbatim. {@code SubscribeAck} envelopes are turned into per-topic signals (so
 * {@link #subscribe} can block until the server confirms the subscription is registered), while
 * every other packet goes into a {@link BlockingQueue} that tests can await with a timeout. This
 * makes the subscribe-then-publish path deterministic: a publish only happens after the
 * subscription is acknowledged.
 */
final class TestClient implements Closeable {

    private final Socket socket;
    private final DataOutputStream out;
    private final DataInputStream in;
    private final int maxPacketSize;
    private final BlockingQueue<JSONObject> inbound = new LinkedBlockingQueue<>();
    /**
     * One latch per (normalized) topic that the client has subscribed to. The reader counts
     * the latch down when the matching {@code SubscribeAck} arrives, releasing a {@link #subscribe}
     * call that is blocking on it.
     */
    private final Map<String, CountDownLatch> subscribeAckLatches = new ConcurrentHashMap<>();
    private final Thread readerThread;
    private volatile boolean running = true;

    TestClient(String host, int port, int maxPacketSize) throws IOException {
        this(new Socket(host, port), maxPacketSize);
    }

    /**
     * Constructs a client around an already-connected socket. Used by the TLS test, which
     * supplies an {@link javax.net.ssl.SSLSocket} so the exact same wire protocol runs over
     * an encrypted transport.
     */
    TestClient(Socket socket, int maxPacketSize) throws IOException {
        this.socket = socket;
        this.out = new DataOutputStream(socket.getOutputStream());
        this.in = new DataInputStream(socket.getInputStream());
        this.maxPacketSize = maxPacketSize;
        this.readerThread = new Thread(this::readLoop, "test-client-reader");
        this.readerThread.setDaemon(true);
        this.readerThread.start();
    }

    private void readLoop() {
        try {
            while (running) {
                byte[] frame = PacketFraming.readFrame(in, maxPacketSize);
                ClientEnvelope envelope = WireCodec.decodeClient(frame, maxPacketSize);
                switch (envelope.getMsgCase()) {
                    case SUBSCRIBE_ACK -> {
                        // Route subscribe acks to their per-topic latch instead of the generic
                        // queue, so they can release a blocking subscribe() without being consumed
                        // by an unrelated awaitPacket() call.
                        signalSubscribeAck(normalize(envelope.getSubscribeAck().getTopic()));
                    }
                    case AUTH_RESPONSE -> inbound.offer(authResponseJson(envelope.getAuthResponse().getToken()));
                    case PUBLISH_OUTBOUND -> inbound.offer(publishOutboundJson(envelope.getPublishOutbound()));
                    default -> {
                        // No other server-to-client cases exist; ignore anything unexpected.
                    }
                }
            }
        } catch (IOException e) {
            // Expected on socket close / server shutdown; stop reading silently.
        }
    }

    /**
     * Rebuilds the historic {@code {"header":{"name":"AuthTokenResponseOutboundPacket"},"body":{"token":...}}}
     * shape so existing assertions remain unchanged.
     */
    private static JSONObject authResponseJson(String token) {
        JSONObject header = new JSONObject().put("name", "AuthTokenResponseOutboundPacket");
        JSONObject body = new JSONObject().put("token", token);
        return new JSONObject().put("header", header).put("body", body);
    }

    /**
     * Rebuilds the historic {@code {"header":{"name":"PublishMessageOutboundPacket","topic","source"},"body":{"data"}}}
     * shape so existing assertions remain unchanged.
     */
    private static JSONObject publishOutboundJson(PublishOutbound publish) {
        JSONObject header = new JSONObject()
                .put("name", "PublishMessageOutboundPacket")
                .put("topic", publish.getTopic())
                .put("source", publish.getSource());
        JSONObject body = new JSONObject().put("data", publish.getData());
        return new JSONObject().put("header", header).put("body", body);
    }

    /** Counts down (creating if necessary) the latch for the given normalized topic. */
    private void signalSubscribeAck(String topic) {
        subscribeAckLatches.computeIfAbsent(topic, key -> new CountDownLatch(1)).countDown();
    }

    /** Normalizes a topic the same way the server does, so ack correlation always agrees. */
    private static String normalize(String topic) {
        return topic.toLowerCase(Locale.ROOT).strip();
    }

    private void writeEnvelope(ClientEnvelope envelope) throws IOException {
        byte[] payload = WireCodec.encodeClient(envelope, maxPacketSize);
        synchronized (out) {
            PacketFraming.writeFrame(out, payload);
        }
    }

    /**
     * Performs the auth handshake: sends the secret and blocks until the token response arrives.
     *
     * @return the issued token
     */
    String authenticate(String secret, long timeout, TimeUnit unit) throws IOException, InterruptedException {
        writeEnvelope(ClientEnvelope.newBuilder()
                .setAuthRequest(AuthTokenRequest.newBuilder().setSecret(secret).build())
                .build());
        JSONObject response = awaitPacket("AuthTokenResponseOutboundPacket", timeout, unit);
        if (response == null) {
            throw new IOException("No auth token response received within timeout");
        }
        return response.getJSONObject("body").getString("token");
    }

    /**
     * Subscribes to a topic and blocks until the server acknowledges the subscription with a
     * {@code SubscribeAck} envelope, or the timeout elapses. Blocking on the ack guarantees the
     * subscription is registered server-side before the call returns, so a subsequent publish can
     * never race ahead of it.
     *
     * @throws IOException if the ack does not arrive within the timeout
     */
    void subscribe(String topic, String token, long timeout, TimeUnit unit) throws IOException, InterruptedException {
        String normalized = normalize(topic);
        // Register the latch BEFORE sending so a fast ack cannot arrive before we are ready
        // to observe it.
        CountDownLatch latch = subscribeAckLatches.computeIfAbsent(normalized, key -> new CountDownLatch(1));
        writeEnvelope(subscribeEnvelope(topic, token));
        if (!latch.await(timeout, unit)) {
            throw new IOException("No subscribe acknowledgment received for topic '" + topic + "' within timeout");
        }
    }

    /**
     * Fire-and-forget subscribe used by negative tests that expect the server to drop the
     * connection (and therefore never send an ack).
     */
    void subscribe(String topic, String token) throws IOException {
        writeEnvelope(subscribeEnvelope(topic, token));
    }

    private static ClientEnvelope subscribeEnvelope(String topic, String token) {
        return ClientEnvelope.newBuilder()
                .setSubscribe(Subscribe.newBuilder().setTopic(topic).setToken(token).build())
                .build();
    }

    void publish(String topic, String data, String source, String token) throws IOException {
        writeEnvelope(ClientEnvelope.newBuilder()
                .setPublishInbound(PublishInbound.newBuilder()
                        .setTopic(topic)
                        .setData(data)
                        .setSource(source)
                        .setToken(token)
                        .build())
                .build());
    }

    /**
     * Waits for the next inbound packet with the given header {@code name}, ignoring others.
     *
     * @return the matching envelope, or {@code null} if none arrived within the timeout
     */
    JSONObject awaitPacket(String name, long timeout, TimeUnit unit) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
        while (true) {
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0) {
                return null;
            }
            JSONObject envelope = inbound.poll(remaining, TimeUnit.NANOSECONDS);
            if (envelope == null) {
                return null;
            }
            if (name.equals(envelope.getJSONObject("header").optString("name"))) {
                return envelope;
            }
        }
    }

    /** Returns true if NO packet with the given name arrives within the timeout (negative assert). */
    boolean expectNoPacket(String name, long timeout, TimeUnit unit) throws InterruptedException {
        return awaitPacket(name, timeout, unit) == null;
    }

    boolean isClosed() {
        return socket.isClosed() || !socket.isConnected();
    }

    @Override
    public void close() {
        running = false;
        try {
            socket.close();
        } catch (IOException ignored) {
            // best-effort teardown
        }
    }
}
