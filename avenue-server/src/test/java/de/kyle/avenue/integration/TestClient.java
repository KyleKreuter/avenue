package de.kyle.avenue.integration;

import de.kyle.avenue.packet.OutboundPacket;
import de.kyle.avenue.packet.Packet;
import de.kyle.avenue.packet.auth.AuthTokenRequestInboundPacket;
import de.kyle.avenue.packet.publish.PublishMessageInboundPacket;
import de.kyle.avenue.packet.subscribe.SubscribeInboundPacket;
import de.kyle.avenue.serialization.PacketFraming;
import org.json.JSONObject;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Minimal raw-socket test client. It speaks the real Avenue wire protocol
 * ({@link PacketFraming length-prefix framing} + the real packet classes) over a real TCP
 * socket, so the integration tests exercise the full server end to end (framing, serialization,
 * auth, routing, fan-out) rather than mocks.
 * <p>
 * The {@link de.kyle.avenue.AvenueClient} singleton is intentionally NOT reused here: being a
 * singleton it cannot represent two independent clients in the same JVM, which the fan-out
 * tests require.
 * <p>
 * A background reader thread drains inbound frames into a {@link BlockingQueue} so tests can
 * await delivery with a timeout instead of sleeping.
 */
final class TestClient implements Closeable {

    private final Socket socket;
    private final DataOutputStream out;
    private final DataInputStream in;
    private final int maxPacketSize;
    private final BlockingQueue<JSONObject> inbound = new LinkedBlockingQueue<>();
    private final Thread readerThread;
    private volatile boolean running = true;

    TestClient(String host, int port, int maxPacketSize) throws IOException {
        this.socket = new Socket(host, port);
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
                JSONObject envelope = new JSONObject(new String(frame, StandardCharsets.UTF_8));
                inbound.offer(envelope);
            }
        } catch (IOException e) {
            // Expected on socket close / server shutdown; stop reading silently.
        }
    }

    private void writeFrame(Packet packet) throws IOException {
        JSONObject envelope = new JSONObject();
        envelope.put("header", new JSONObject(new String(packet.getHeader(), StandardCharsets.UTF_8)));
        envelope.put("body", new JSONObject(new String(packet.getBody(), StandardCharsets.UTF_8)));
        byte[] payload = envelope.toString().getBytes(StandardCharsets.UTF_8);
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
        writeFrame(new AuthTokenRequestInboundPacket(secret));
        JSONObject response = awaitPacket("AuthTokenResponseOutboundPacket", timeout, unit);
        if (response == null) {
            throw new IOException("No auth token response received within timeout");
        }
        return response.getJSONObject("body").getString("token");
    }

    void subscribe(String topic, String token) throws IOException {
        writeFrame(new SubscribeInboundPacket(topic, token));
    }

    void publish(String topic, String data, String source, String token) throws IOException {
        writeFrame(new PublishMessageInboundPacket(topic, data, source, token));
    }

    /** Sends an arbitrary outbound packet (used for negative tests of malformed/raw frames). */
    void sendRaw(OutboundPacket packet) throws IOException {
        writeFrame(packet);
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
