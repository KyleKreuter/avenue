package de.kyle.avenue.topic;

import de.kyle.avenue.message.Message;

public interface TopicListener {
    void onMessage(Message message);
}
