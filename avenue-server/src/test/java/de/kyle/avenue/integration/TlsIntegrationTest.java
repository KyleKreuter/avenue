package de.kyle.avenue.integration;

import de.kyle.avenue.SingleNodeServer;
import de.kyle.avenue.config.AvenueConfig;
import de.kyle.avenue.handler.client.BackpressurePolicy;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * TLS integration test (E14).
 * <p>
 * Starts a {@link SingleNodeServer} with {@code server.tls.enabled=true} using a self-signed
 * PKCS12 keystore generated at runtime (no checked-in certificate). A real {@link SSLSocket}
 * client then runs the full auth -&gt; subscribe -&gt; publish -&gt; receive flow over the
 * encrypted transport, proving the SSL server socket and the {@code SocketFactoryProvider}
 * abstraction work end to end.
 */
class TlsIntegrationTest {

    private static final String HOST = "127.0.0.1";
    private static final String SECRET = "tls-secret";
    private static final String TOKEN = "tls-token";
    private static final int PACKET_SIZE = 65536;
    private static final long TIMEOUT = 10;
    private static final TimeUnit UNIT = TimeUnit.SECONDS;

    @TempDir
    Path tempDir;

    private SingleNodeServer server;
    private int port;
    private Path keystorePath;

    @BeforeEach
    void startTlsServer() throws Exception {
        keystorePath = TlsTestKeystore.generate(tempDir.resolve("avenue-tls.p12"));

        AvenueConfig config = new AvenueConfig(
                PACKET_SIZE,
                true,
                SECRET, TOKEN,
                0,            // ephemeral client port
                1024, 100,
                false, UUID.randomUUID().toString(), 0, List.of(), "", 5_000L,
                0L,                                  // idle-timeout off for the test
                10_000,                              // max-connections
                BackpressurePolicy.DISCONNECT_SLOW_CONSUMER,
                0L,                                  // metrics logging off in tests
                true,                                // server TLS ENABLED
                keystorePath.toString(),
                TlsTestKeystore.PASSWORD,
                false, "", "", "", ""                // cluster TLS off
        );
        server = new SingleNodeServer(config);
        server.start();
        port = server.getPort();
        Assertions.assertTrue(port > 0, "TLS server must report a bound ephemeral port");
    }

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void full_pubsub_flow_works_over_tls() throws Exception {
        SSLSocketFactory factory = trustingSocketFactory(keystorePath);

        try (TestClient subscriber = new TestClient(connect(factory), PACKET_SIZE);
             TestClient publisher = new TestClient(connect(factory), PACKET_SIZE)) {

            Assertions.assertEquals(TOKEN, subscriber.authenticate(SECRET, TIMEOUT, UNIT),
                    "Auth over TLS must yield the configured token");
            publisher.authenticate(SECRET, TIMEOUT, UNIT);

            subscriber.subscribe("secure-news", TOKEN, TIMEOUT, UNIT);
            publisher.publish("secure-news", "encrypted-hello", "tls-publisher", TOKEN);

            JSONObject received = subscriber.awaitPacket("PublishMessageOutboundPacket", TIMEOUT, UNIT);
            Assertions.assertNotNull(received, "Subscriber must receive the published message over TLS");
            Assertions.assertEquals("secure-news", received.getJSONObject("header").getString("topic"));
            Assertions.assertEquals("tls-publisher", received.getJSONObject("header").getString("source"));
            Assertions.assertEquals("encrypted-hello", received.getJSONObject("body").getString("data"));
        }
    }

    private SSLSocket connect(SSLSocketFactory factory) throws Exception {
        SSLSocket socket = (SSLSocket) factory.createSocket(HOST, port);
        socket.startHandshake();
        return socket;
    }

    /**
     * Builds an {@link SSLSocketFactory} that trusts the self-signed certificate in the given
     * keystore (used here as a truststore) so the client verifies the server cert.
     */
    private static SSLSocketFactory trustingSocketFactory(Path keystorePath) throws Exception {
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(keystorePath)) {
            keyStore.load(in, TlsTestKeystore.PASSWORD.toCharArray());
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(keyStore);

        // Key managers are not needed (no mutual TLS), but kept symmetric for clarity.
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, TlsTestKeystore.PASSWORD.toCharArray());

        SSLContext context = SSLContext.getInstance("TLS");
        context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        return context.getSocketFactory();
    }
}
