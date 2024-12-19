package de.kyle.avenue;

import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.handler.authentication.AuthenticationTokenHandler;
import de.kyle.avenue.handler.client.ClientConnectionHandler;
import de.kyle.avenue.handler.packet.InboundPacketHandler;
import de.kyle.avenue.handler.subscription.TopicSubscriptionHandler;
import de.kyle.avenue.serialization.PacketDeserializer;
import de.kyle.avenue.serialization.PacketSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SingleNodeServer {
    private static final Logger log = LoggerFactory.getLogger(SingleNodeServer.class);
    private final PacketDeserializer packetDeserializer;
    private final PacketSerializer packetSerializer;
    private final InboundPacketHandler inboundPacketHandler;
    private final ExecutorService executorService;
    private final AvenueConfig avenueConfig;
    private boolean running;

    public SingleNodeServer() {
        try {
            this.avenueConfig = new AvenueConfig();
        } catch (IOException e) {
            log.error("Could not load configuration file", e);
            throw new RuntimeException(e);
        }
        AuthenticationTokenHandler authenticationTokenHandler = new AuthenticationTokenHandler(this.avenueConfig);
        TopicSubscriptionHandler topicSubscriptionHandler = new TopicSubscriptionHandler();

        this.executorService = Executors.newVirtualThreadPerTaskExecutor();
        this.packetDeserializer = new PacketDeserializer(this.avenueConfig);
        this.packetSerializer = new PacketSerializer(this.avenueConfig);
        this.inboundPacketHandler = new InboundPacketHandler(
                authenticationTokenHandler,
                this.avenueConfig,
                topicSubscriptionHandler
        );
    }

    public void start(int port) {
        log.info("Starting server on port {}", port);
        this.running = true;
        try (ServerSocket server = new ServerSocket(port)) {
            while (running) {
                try {
                    Socket client = server.accept();
                    ClientConnectionHandler clientConnectionHandler = new ClientConnectionHandler(
                            client,
                            packetDeserializer,
                            packetSerializer,
                            inboundPacketHandler,
                            avenueConfig
                    );
                    this.executorService.execute(clientConnectionHandler);
                } catch (IOException e) {
                    log.warn("Exception while trying to accept connection: {}", e.getMessage());
                }
            }
        } catch (IOException e) {
            log.error(e.getMessage());
        } finally {
            stop();
        }
    }

    public void stop() {
        log.info("Stopping server");
        this.running = false;
        this.executorService.shutdown();
    }
}
