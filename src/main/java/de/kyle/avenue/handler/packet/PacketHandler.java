package de.kyle.avenue.handler.packet;

import de.kyle.avenue.handler.client.ClientConnectionHandler;
import org.json.JSONObject;

public interface PacketHandler {
    void handle(JSONObject packet, ClientConnectionHandler clientConnectionHandler);
}