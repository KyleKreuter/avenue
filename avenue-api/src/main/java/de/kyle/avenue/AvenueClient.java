package de.kyle.avenue;

import de.kyle.avenue.config.AvenueClientConfig;
import de.kyle.avenue.message.Message;
import de.kyle.avenue.packet.auth.AuthTokenRequestInboundPacket;
import de.kyle.avenue.packet.publish.PublishMessageInboundPacket;
import de.kyle.avenue.packet.subscribe.SubscribeInboundPacket;
import de.kyle.avenue.serialization.PacketDeserializer;
import de.kyle.avenue.serialization.PacketFraming;
import de.kyle.avenue.serialization.PacketSerializer;
import de.kyle.avenue.topic.Topic;
import de.kyle.avenue.topic.TopicListener;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class AvenueClient {
    private static final Logger log = LoggerFactory.getLogger(AvenueClient.class);

    /**
     * Maximum time a caller will block waiting for the connection to become ready
     * (socket connected and auth token received) before an exception is raised.
     */
    private static final long READINESS_TIMEOUT_SECONDS = 10L;

    private final Socket socket;
    private InputStream inputStream;
    private OutputStream outputStream;
    private DataOutputStream dataOutputStream;
    private final PacketDeserializer packetDeserializer;
    private final PacketSerializer packetSerializer;
    private final int packetSize;

    /**
     * Maps the normalized topic name to its registered listener. Accessed from both the
     * caller thread (registration) and the listen virtual thread (dispatch), hence the
     * concurrent map.
     */
    private final Map<String, TopicListener> topicListenerMap;

    /**
     * Topics that have been registered locally but not yet confirmed as subscribed on the
     * server (e.g. registered before the connection became ready). They are flushed once
     * readiness is reached. Thread-safe set backed by a concurrent map.
     */
    private final Set<String> pendingSubscriptions;

    private final String clientName;
    private volatile boolean running;
    private volatile String authToken;

    /**
     * Reaches zero exactly when the client is ready (connected and authenticated). Callers
     * of {@link #sendMessage(String, String)} and subscribe sends wait on this latch.
     */
    private final CountDownLatch readyLatch = new CountDownLatch(1);

    /**
     * Guards writes to the shared output stream so that frames from concurrent senders are
     * never interleaved on the wire.
     */
    private final Lock writeLock = new ReentrantLock();

    private static AvenueClient avenueClient;
    private static final Lock constructorLock = new ReentrantLock();

    private AvenueClient() throws IOException {
        AvenueClientConfig config = new AvenueClientConfig();
        this.socket = new Socket();
        this.running = true;
        this.packetDeserializer = new PacketDeserializer(config.getPacketSize());
        this.packetSerializer = new PacketSerializer(config.getPacketSize());
        this.packetSize = config.getPacketSize();
        this.topicListenerMap = new ConcurrentHashMap<>();
        this.pendingSubscriptions = ConcurrentHashMap.newKeySet();
        this.clientName = config.getClientName();
        Thread.ofVirtual().start(() -> {
            try {
                socket.connect(new InetSocketAddress(config.getHostName(), config.getPort()));
                this.inputStream = this.socket.getInputStream();
                this.outputStream = this.socket.getOutputStream();
                this.dataOutputStream = new DataOutputStream(this.outputStream);
                AuthTokenRequestInboundPacket authTokenRequestInboundPacket =
                        new AuthTokenRequestInboundPacket(config.getAuthenticationSecret());
                byte[] serialized = packetSerializer.serialize(authTokenRequestInboundPacket);
                // The auth request is sent before readiness, so it bypasses the readiness
                // gate and writes directly under the write lock.
                writeFrameLocked(serialized);
                listen();
            } catch (IOException e) {
                log.error("An error occurred while connecting to the server", e);
                throw new RuntimeException(e);
            }
        });
    }

    public static AvenueClient getInstance() throws IOException, InterruptedException {
        constructorLock.lock();
        try {
            if (avenueClient == null) {
                avenueClient = new AvenueClient();
            }
            return avenueClient;
        } finally {
            constructorLock.unlock();
        }
    }

    private void listen() throws IOException {
        try (DataInputStream dataInputStream = new DataInputStream(this.inputStream)) {
            // Length-prefix framing: read exactly one frame per iteration. The loop must run
            // for the entire lifetime of the connection so that topic messages keep flowing
            // after authentication.
            while (this.running) {
                byte[] packetBytes = PacketFraming.readFrame(dataInputStream, packetSize);
                JSONObject packet = packetDeserializer.deserialize(packetBytes);
                JSONObject header = packet.getJSONObject("header");
                JSONObject body = packet.getJSONObject("body");
                String name = header.getString("name");

                if (name.equals("AuthTokenResponseOutboundPacket")) {
                    this.authToken = body.getString("token");
                    // Signal readiness and flush any subscriptions registered before auth.
                    // We must NOT return here: the loop has to keep reading topic messages.
                    readyLatch.countDown();
                    flushPendingSubscriptions();
                    continue;
                }

                if (name.equals("PublishMessageOutboundPacket")) {
                    String topic = normalizeTopic(header.getString("topic"));
                    TopicListener topicListener = topicListenerMap.get(topic);
                    if (topicListener == null) {
                        // Unknown topic: nothing is registered for it locally. Ignore the
                        // message and keep reading instead of tearing the loop down.
                        log.debug("Received a message for an unregistered topic '{}', ignoring", topic);
                        continue;
                    }
                    topicListener.onMessage(new Message(header.getString("source"), body.getString("data")));
                    continue;
                }

                log.debug("Received a packet of unhandled type '{}', ignoring", name);
            }
        } catch (IOException e) {
            if (this.running) {
                // Only surface the failure if we did not shut down deliberately.
                throw e;
            }
            log.debug("Listen loop stopped after shutdown");
        }
    }

    public void sendMessage(String topic, String data) throws IOException {
        awaitReady();
        String normalizedTopic = normalizeTopic(topic);
        PublishMessageInboundPacket publishMessageInboundPacket =
                new PublishMessageInboundPacket(normalizedTopic, data, this.clientName, this.authToken);
        byte[] serialized = packetSerializer.serialize(publishMessageInboundPacket);
        writeFrameLocked(serialized);
    }

    public void shutdown() {
        try {
            this.running = false;
            if (this.inputStream != null) {
                this.inputStream.close();
            }
            if (this.outputStream != null) {
                this.outputStream.close();
            }
            this.socket.close();
        } catch (IOException e) {
            log.warn("An error occurred while closing connection", e);
        }
    }

    public void registerTopicListener(TopicListener topicListener) {
        try {
            Method method = topicListener.getClass().getMethod("onMessage", Message.class);
            if (Arrays.stream(method.getAnnotations())
                    .noneMatch(annotation -> annotation.annotationType().equals(Topic.class))) {
                throw new RuntimeException("Could not find a topic for the provided Topiclistener");
            }
            Topic annotation = method.getAnnotation(Topic.class);
            String topic = normalizeTopic(annotation.value());
            topicListenerMap.put(topic, topicListener);
            // Subscribe on the server. If the connection is not ready yet, buffer the topic
            // and let it be flushed once authentication completes.
            subscribeOrBuffer(topic);
        } catch (NoSuchMethodException e) {
            log.error("Could not register topic listener", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Sends a subscribe packet for the given normalized topic if the client is already
     * ready; otherwise records it for a later flush after authentication.
     *
     * @param topic the already-normalized topic name
     */
    private void subscribeOrBuffer(String topic) {
        if (readyLatch.getCount() == 0) {
            try {
                sendSubscribe(topic);
            } catch (IOException e) {
                // Fall back to buffering so the subscription is retried on the next flush.
                log.warn("Failed to send subscribe for topic '{}', buffering for retry", topic, e);
                pendingSubscriptions.add(topic);
            }
            return;
        }
        pendingSubscriptions.add(topic);
    }

    /**
     * Flushes all buffered subscriptions to the server. Invoked from the listen thread once
     * the auth token has been received.
     */
    private void flushPendingSubscriptions() {
        for (String topic : pendingSubscriptions) {
            try {
                sendSubscribe(topic);
                pendingSubscriptions.remove(topic);
            } catch (IOException e) {
                log.warn("Failed to flush subscribe for topic '{}', will retry later", topic, e);
            }
        }
    }

    /**
     * Sends a single {@link SubscribeInboundPacket} for the given normalized topic. Must only
     * be called once an auth token is available.
     *
     * @param topic the already-normalized topic name
     * @throws IOException if writing to the stream fails
     */
    private void sendSubscribe(String topic) throws IOException {
        SubscribeInboundPacket subscribeInboundPacket = new SubscribeInboundPacket(topic, this.authToken);
        byte[] serialized = packetSerializer.serialize(subscribeInboundPacket);
        writeFrameLocked(serialized);
    }

    /**
     * Writes a single framed payload to the shared output stream while holding the write lock
     * to prevent interleaving with other senders.
     *
     * @param payload the serialized packet payload (without length prefix)
     * @throws IOException if writing fails
     */
    private void writeFrameLocked(byte[] payload) throws IOException {
        writeLock.lock();
        try {
            PacketFraming.writeFrame(this.dataOutputStream, payload);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Blocks until the client is ready (connected and authenticated) or the readiness timeout
     * elapses.
     *
     * @throws IOException if readiness is not reached within {@link #READINESS_TIMEOUT_SECONDS}
     *                     or the wait is interrupted
     */
    private void awaitReady() throws IOException {
        try {
            if (!readyLatch.await(READINESS_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                throw new IOException("Timed out waiting for the client to become ready (auth not completed)");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for the client to become ready", e);
        }
    }

    /**
     * Normalizes a topic the same way the server does so that registration, subscription,
     * publishing and dispatch all agree on the key.
     *
     * @param topic the raw topic
     * @return the normalized topic (lower-cased and stripped)
     */
    private static String normalizeTopic(String topic) {
        return topic.toLowerCase(Locale.ROOT).strip();
    }
}
