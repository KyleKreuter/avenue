package de.kyle.avenue.handler.packet.auth;

import de.kyle.avenue.handler.authentication.AuthenticationTokenHandler;
import de.kyle.avenue.handler.client.ClientConnectionHandler;
import de.kyle.avenue.handler.packet.PacketHandler;
import de.kyle.avenue.proto.AuthTokenRequest;
import de.kyle.avenue.proto.ClientEnvelope;
import de.kyle.avenue.serialization.ClientEnvelopes;

/**
 * Handles an {@link AuthTokenRequest} from a client: exchanges the provided secret for a token
 * and answers with an {@link AuthTokenResponse} envelope. This message is unsecured (no token
 * is required to authenticate), mirroring the pre-protobuf behaviour.
 */
public class AuthTokenRequestInboundPacketHandler implements PacketHandler {
    private final AuthenticationTokenHandler authenticationTokenHandler;

    public AuthTokenRequestInboundPacketHandler(
            AuthenticationTokenHandler authenticationTokenHandler
    ) {
        this.authenticationTokenHandler = authenticationTokenHandler;
    }

    @Override
    public void handle(ClientEnvelope envelope, ClientConnectionHandler clientConnectionHandler) {
        AuthTokenRequest request = envelope.getAuthRequest();
        // proto3 defaults an unset string to "", which is exactly the empty-secret case; the
        // token handler validates the secret and yields the configured token (or an error token).
        String token = authenticationTokenHandler.getToken(request.getSecret());
        // Asynchronous enqueue; the dedicated writer thread performs the actual socket write.
        clientConnectionHandler.send(ClientEnvelopes.authResponse(token));
    }
}
