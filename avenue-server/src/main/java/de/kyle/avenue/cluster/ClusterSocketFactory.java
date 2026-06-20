package de.kyle.avenue.cluster;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Narrow seam over the socket-creation surface that {@link ClusterManager} needs.
 * <p>
 * The production implementation is {@link de.kyle.avenue.net.SocketFactoryProvider} (plain or
 * TLS). Tests can supply a fake implementation to inject loopback sockets, simulate connection
 * failures, or intercept the cluster wire without standing up real TLS. The production code path
 * is unchanged: the default {@link ClusterManager} constructor still builds a
 * {@code SocketFactoryProvider} exactly as before.
 */
public interface ClusterSocketFactory {

    /**
     * Creates a client socket connected to {@code host:port}.
     *
     * @param host target host
     * @param port target port
     * @return a connected socket
     * @throws IOException on connection failure
     */
    Socket createSocket(String host, int port) throws IOException;

    /**
     * Creates a server socket bound to the given port ({@code 0} = ephemeral).
     *
     * @param port the port to bind
     * @return a bound server socket
     * @throws IOException on bind failure
     */
    ServerSocket createServerSocket(int port) throws IOException;

    /** Returns {@code true} if this factory produces TLS sockets. */
    boolean isTlsEnabled();
}
