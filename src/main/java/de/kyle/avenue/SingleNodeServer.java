package de.kyle.avenue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class SingleNodeServer {
    private static final Logger log = LoggerFactory.getLogger(SingleNodeServer.class);
    private final Executor executor;
    private boolean running;

    public SingleNodeServer(int port) {
        executor = Executors.newVirtualThreadPerTaskExecutor();

    }

    public void start(int port) {
        try (ServerSocket server = new ServerSocket(port)) {
            while (running) {
                Socket client = server.accept();

            }
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }
}
