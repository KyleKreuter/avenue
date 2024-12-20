package de.kyle.avenue;

import de.kyle.avenue.config.AvenueClientConfig;
import de.kyle.avenue.message.Message;
import de.kyle.avenue.packet.auth.AuthTokenRequestInboundPacket;
import de.kyle.avenue.packet.publish.PublishMessageInboundPacket;
import de.kyle.avenue.serialization.PacketDeserializer;
import de.kyle.avenue.serialization.PacketSerializer;
import de.kyle.avenue.topic.Topic;
import de.kyle.avenue.topic.TopicListener;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class AvenueClient {
    private static final Logger log = LoggerFactory.getLogger(AvenueClient.class);
    private final Socket socket;
    private final InputStream inputStream;
    private final OutputStream outputStream;
    private final PacketDeserializer packetDeserializer;
    private final PacketSerializer packetSerializer;
    private final Map<String, TopicListener> topicListenerMap;
    private final String clientName;
    private boolean running;
    private String authToken;
    private static AvenueClient avenueClient;
    private static final Lock constructorLock = new ReentrantLock();

    private AvenueClient() throws IOException {
        AvenueClientConfig config = new AvenueClientConfig();
        this.socket = new Socket();
        this.inputStream = this.socket.getInputStream();
        this.outputStream = this.socket.getOutputStream();
        this.running = true;
        this.packetDeserializer = new PacketDeserializer(config.getPacketSize());
        this.packetSerializer = new PacketSerializer(config.getPacketSize());
        this.topicListenerMap = new HashMap<>();
        this.clientName = config.getClientName();
        Thread.ofVirtual().start(() -> {
            try {
                socket.connect(new InetSocketAddress(config.getHostName(), config.getPort()));
                AuthTokenRequestInboundPacket authTokenRequestInboundPacket = new AuthTokenRequestInboundPacket(config.getAuthenticationSecret());
                byte[] serialized = packetSerializer.serialize(authTokenRequestInboundPacket);
                outputStream.write(serialized);
                outputStream.flush();
                listen();
            } catch (IOException e) {
                log.error("An error occurred while connecting to the server", e);
                throw new RuntimeException(e);
            }
        });
    }

    public static AvenueClient getInstance() throws IOException {
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
            while (this.running) {
                byte[] packetBytes = dataInputStream.readAllBytes();
                JSONObject packet = packetDeserializer.deserialize(packetBytes);
                JSONObject header = packet.getJSONObject("header");
                JSONObject body = packet.getJSONObject("body");
                if (header.getString("name").equals("AuthTokenResponseOutboundPacket")) {
                    this.authToken = body.getString("token");
                    return;
                }
                if (!topicListenerMap.containsKey(header.getString("topic"))) {
                    return;
                }
                TopicListener topicListener = topicListenerMap.get(header.getString("topic"));
                topicListener.onMessage(new Message(header.getString("source"), body.getString("data")));
            }
        }
    }

    public void sendMessage(String topic, String data) throws IOException {
        PublishMessageInboundPacket publishMessageInboundPacket = new PublishMessageInboundPacket(topic, data, this.clientName, this.authToken);
        byte[] serialized = packetSerializer.serialize(publishMessageInboundPacket);
        outputStream.write(serialized);
        outputStream.flush();
    }

    public void shutdown() {
        try {
            this.running = false;
            this.inputStream.close();
            this.outputStream.close();
            this.socket.close();
        } catch (IOException e) {
            log.warn("An error occurred while closing connection", e);
        }
    }

    public void registerTopicListener(TopicListener topicListener) {
        try {
            Method method = topicListener.getClass().getMethod("onMessage", Message.class);
            if (Arrays.stream(method.getAnnotations()).noneMatch(annotation -> annotation.annotationType().equals(Topic.class))) {
                throw new RuntimeException("Could not find a topic for the provided Topiclistener");
            }
            Topic annotation = method.getAnnotation(Topic.class);
            String value = annotation.value();
            topicListenerMap.put(value, topicListener);
        } catch (NoSuchMethodException e) {
            log.error("Could not register topic listener", e);
            throw new RuntimeException(e);
        }
    }
}
