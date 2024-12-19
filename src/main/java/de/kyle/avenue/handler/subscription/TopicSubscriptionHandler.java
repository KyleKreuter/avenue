package de.kyle.avenue.handler.subscription;

import de.kyle.avenue.handler.client.ClientConnectionHandler;
import de.kyle.avenue.packet.OutboundPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TopicSubscriptionHandler {
    private static final Logger log = LoggerFactory.getLogger(TopicSubscriptionHandler.class);
    private final Map<String, List<ClientConnectionHandler>> topicSubscriptions = new ConcurrentHashMap<>();

    public void deliverPacketToSubscribers(String topic, OutboundPacket packet) {
        if (!topicSubscriptions.containsKey(topic.toLowerCase())) {
            log.warn("Packet was not delivered to other clients because no subscriptions are registered");
            return;
        }
        List<ClientConnectionHandler> clientConnectionHandlers = topicSubscriptions.get(topic.toLowerCase());
        for (ClientConnectionHandler clientConnectionHandler : clientConnectionHandlers) {
            try {
                clientConnectionHandler.send(packet);
            } catch (IOException e) {
                log.error("An error occurred while trying to deliver a packet", e);
            }
        }
    }

    public void subscribeToTopic(String topic, ClientConnectionHandler clientConnectionHandler) {
        if (topicSubscriptions.containsKey(topic)) {
            List<ClientConnectionHandler> clientConnectionHandlers = topicSubscriptions.get(topic);
            clientConnectionHandlers.add(clientConnectionHandler);
            topicSubscriptions.put(topic, clientConnectionHandlers);
            return;
        }
        List<ClientConnectionHandler> clientConnectionHandlers = new ArrayList<>();
        clientConnectionHandlers.add(clientConnectionHandler);
        topicSubscriptions.put(topic, clientConnectionHandlers);
    }

    public void unsubscribeFromAllTopics(ClientConnectionHandler clientConnectionHandler) {
        for (Map.Entry<String, List<ClientConnectionHandler>> topics : topicSubscriptions.entrySet()) {
            if (!topics.getValue().contains(clientConnectionHandler)) {
                continue;
            }
            List<ClientConnectionHandler> clients = topics.getValue();
            clients.remove(clientConnectionHandler);
            topics.setValue(clients);
        }
    }
}
