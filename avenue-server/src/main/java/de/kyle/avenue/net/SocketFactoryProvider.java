package de.kyle.avenue.net;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

/**
 * Small abstraction that yields either plain {@link ServerSocket}/{@link Socket} instances or
 * their TLS-enabled {@link SSLServerSocket}/{@link SSLSocket} counterparts depending on
 * configuration (E14).
 *
 * <p>The rest of the codebase keeps working purely against {@link Socket}/{@link ServerSocket};
 * only the factory knows about TLS. The plain path (TLS disabled) is the default and must stay
 * behaviourally identical to the pre-Wave-5 server so existing tests are unaffected.
 *
 * <p>An instance is built once via {@link #plain()}, {@link #forServer} or {@link #forCluster}
 * and then reused. TLS instances eagerly initialize an {@link SSLContext} from the configured
 * keystore (server side) and/or truststore (client side), failing fast on a misconfiguration.
 */
public final class SocketFactoryProvider {

    private static final String TLS_PROTOCOL = "TLS";

    private final boolean tlsEnabled;
    private final SSLContext sslContext;

    private SocketFactoryProvider(boolean tlsEnabled, SSLContext sslContext) {
        this.tlsEnabled = tlsEnabled;
        this.sslContext = sslContext;
    }

    /** A plain (non-TLS) provider. */
    public static SocketFactoryProvider plain() {
        return new SocketFactoryProvider(false, null);
    }

    /**
     * Builds a provider for the client-facing server port. When {@code tlsEnabled} is false a
     * plain provider is returned and the keystore arguments are ignored.
     *
     * @param tlsEnabled       whether TLS is active
     * @param keystorePath     path to the JKS/PKCS12 keystore holding the server certificate
     * @param keystorePassword keystore (and key) password
     */
    public static SocketFactoryProvider forServer(boolean tlsEnabled, String keystorePath, String keystorePassword) {
        if (!tlsEnabled) {
            return plain();
        }
        SSLContext context = buildContext(keystorePath, keystorePassword, null, null);
        return new SocketFactoryProvider(true, context);
    }

    /**
     * Builds a provider for the cluster transport. The keystore is used by the acceptor side
     * (server socket); the truststore is used by the initiator side (client socket) to verify
     * the peer certificate. Either may be blank if only one direction is needed, but for a
     * mutual full-mesh both are typically configured.
     */
    public static SocketFactoryProvider forCluster(
            boolean tlsEnabled,
            String keystorePath,
            String keystorePassword,
            String truststorePath,
            String truststorePassword
    ) {
        if (!tlsEnabled) {
            return plain();
        }
        SSLContext context = buildContext(keystorePath, keystorePassword, truststorePath, truststorePassword);
        return new SocketFactoryProvider(true, context);
    }

    /** Returns {@code true} if this provider produces TLS sockets. */
    public boolean isTlsEnabled() {
        return tlsEnabled;
    }

    /**
     * Creates a server socket bound to the given port. For TLS the returned socket is an
     * {@link SSLServerSocket} configured to require a TLS handshake on accept.
     *
     * @param port the port to bind ({@code 0} = ephemeral)
     */
    public ServerSocket createServerSocket(int port) throws IOException {
        if (!tlsEnabled) {
            return ServerSocketFactory.getDefault().createServerSocket(port);
        }
        SSLServerSocketFactory factory = sslContext.getServerSocketFactory();
        SSLServerSocket serverSocket = (SSLServerSocket) factory.createServerSocket(port);
        // Server presents its certificate; clients are not required to present one (no mTLS).
        serverSocket.setNeedClientAuth(false);
        serverSocket.setWantClientAuth(false);
        return serverSocket;
    }

    /**
     * Creates a client socket connected to {@code host:port}. For TLS the returned socket is an
     * {@link SSLSocket} whose handshake is started eagerly so connection failures surface here.
     */
    public Socket createSocket(String host, int port) throws IOException {
        if (!tlsEnabled) {
            return SocketFactory.getDefault().createSocket(host, port);
        }
        SSLSocketFactory factory = sslContext.getSocketFactory();
        SSLSocket socket = (SSLSocket) factory.createSocket(host, port);
        // Trigger the handshake now so handshake/trust failures are reported at connect time.
        socket.startHandshake();
        return socket;
    }

    // ------------------------------------------------------------------
    // SSLContext construction
    // ------------------------------------------------------------------

    private static SSLContext buildContext(
            String keystorePath, String keystorePassword,
            String truststorePath, String truststorePassword
    ) {
        try {
            KeyManagerFactory kmf = null;
            if (keystorePath != null && !keystorePath.isBlank()) {
                KeyStore keyStore = loadKeyStore(keystorePath, keystorePassword);
                kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(keyStore, passwordChars(keystorePassword));
            }

            TrustManagerFactory tmf = null;
            if (truststorePath != null && !truststorePath.isBlank()) {
                KeyStore trustStore = loadKeyStore(truststorePath, truststorePassword);
                tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(trustStore);
            }

            SSLContext context = SSLContext.getInstance(TLS_PROTOCOL);
            context.init(
                    kmf != null ? kmf.getKeyManagers() : null,
                    tmf != null ? tmf.getTrustManagers() : null,
                    null
            );
            return context;
        } catch (IOException | GeneralSecurityException e) {
            throw new IllegalStateException("Failed to initialize TLS context: " + e.getMessage(), e);
        }
    }

    private static KeyStore loadKeyStore(String path, String password) throws IOException, GeneralSecurityException {
        Path storePath = Path.of(path);
        // PKCS12 is the modern default keystore type and what keytool produces by default on JDK 21.
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(storePath)) {
            keyStore.load(in, passwordChars(password));
        }
        return keyStore;
    }

    private static char[] passwordChars(String password) {
        return password != null ? password.toCharArray() : new char[0];
    }
}
