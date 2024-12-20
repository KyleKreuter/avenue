# Avenue

Avenue is a high-performance TCP Pub-Sub server designed to handle thousands of requests per second,
leveraging the power of Java Virtual Threads. Virtual Threads, introduced in Java 21, provide lightweight JVM-managed
threads that consume significantly fewer resources than traditional platform threads.


## API Overview

Although this project is not production-ready, you can test the API by following these steps. To get started,
create an instance of `AvenueClient` and register your `TopicListeners`.

### Example Usage

```java
public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        AvenueClient avenueClient = AvenueClient.getInstance();
        avenueClient.registerTopicListener(new TopicListener() {
            @Override
            @Topic("test-topic")
            public void onMessage(Message message) {
                System.out.println("A message was received: " + message.data());
            }
        });
    }
}
```

### Key Notes
- Add the `@Topic` annotation to the `onMessage` method to specify the topic the listener is subscribed to.
- Ensure proper exception handling for IO and interruptions


