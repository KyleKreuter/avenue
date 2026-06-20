package de.kyle.avenue.handler.packet;

import de.kyle.avenue.handler.client.ClientConnection;
import de.kyle.avenue.proto.ClientEnvelope;

/**
 * Handles a single inbound {@link ClientEnvelope} message that the dispatcher has already
 * decoded and security-gated.
 * <p>
 * After the protobuf cutover the handler receives the typed, fully-parsed envelope instead of a
 * loosely-typed {@code JSONObject}. Each implementation reads the relevant oneof case
 * ({@code getAuthRequest()}, {@code getSubscribe()}, {@code getPublishInbound()}) — the
 * {@link InboundPacketHandler} guarantees the correct case is set before dispatch.
 */
public interface PacketHandler {
    void handle(ClientEnvelope envelope, ClientConnection clientConnection);
}
