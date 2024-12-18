package de.kyle.avenue;

import de.kyle.avenue.handler.client.ClientConnectionHandler;
import de.kyle.avenue.registry.InboundPacketRegistry;
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
    private final InboundPacketRegistry inboundPacketRegistry;
    private final ExecutorService executorService;
    private boolean running;

    public SingleNodeServer(int port) {
        this.executorService = Executors.newVirtualThreadPerTaskExecutor();
        this.packetDeserializer = new PacketDeserializer();
        this.packetSerializer = new PacketSerializer();
        this.inboundPacketRegistry = new InboundPacketRegistry();

    }

    public void start(int port) {
        log.info("Starting server...");
        this.running = true;
        try (ServerSocket server = new ServerSocket(port)) {
            while (running) {
                try {
                    Socket client = server.accept();
                    ClientConnectionHandler clientConnectionHandler = new ClientConnectionHandler(
                            client,
                            packetDeserializer,
                            packetSerializer,
                            inboundPacketRegistry
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
        log.info("Stopping server...");
        this.running = false;
        this.executorService.shutdown();
    }
}
