import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class Publisher {

    private static final ExecutorService executorService = Executors.newCachedThreadPool();
    private static final AtomicInteger currentInstanceCount = new AtomicInteger(0);
    private static final Map<String, String> sharedSettings = new HashMap<>();  // Shared settings for delay and qos

    public static void main(String[] args) {
        try {
            MqttClient client = new MqttClient("tcp://localhost:1883", MqttClient.generateClientId(), new MemoryPersistence());
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);
            client.connect(options);

            client.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    // Handle connection lost
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    if ("request/instancecount".equals(topic)) {
                        int requestedCount = Integer.parseInt(new String(message.getPayload()));
                        int currentCount = currentInstanceCount.get();
                        while (currentCount < requestedCount) {
//                            System.out.println("Create New Instance");
                            int newInstanceId = currentCount + 1;
                            executorService.submit(() -> runPublisher(newInstanceId));
                            currentInstanceCount.incrementAndGet();
                            currentCount++;
                        }
                    } else if (topic.startsWith("request/")) {
                        String key = topic.split("/")[1];
                        sharedSettings.put(key, new String(message.getPayload()));
//                        System.out.println("Updated shared settings with " + key + ": " + new String(message.getPayload()));
                    }
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    // Message delivery complete
                }
            });

            client.subscribe("request/#");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void runPublisher(int instanceId) {
        try {
//            System.out.println("Enter runPublisher Method");
            MqttClient client = new MqttClient("tcp://localhost:1883", MqttClient.generateClientId(), new MemoryPersistence());
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);
            client.connect(options);
            client.subscribe("request/#");

            while (true) {
                String delay = sharedSettings.getOrDefault("delay", "1000");
                String qos = sharedSettings.getOrDefault("qos", "0");
//                System.out.println("Ready to publish with delay: " + delay + ", qos: " + qos);
                publishMessages(client, Integer.parseInt(delay), Integer.parseInt(qos), instanceId);
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void publishMessages(MqttClient client, int delay, int qos, int instanceId) throws Exception {
        long endTime = System.currentTimeMillis() + 60000;
        int counter = 0;
        while (System.currentTimeMillis() < endTime) {
            String topic = String.format("counter/%d/%d/%d", instanceId, qos, delay);
            MqttMessage message = new MqttMessage(Integer.toString(counter).getBytes());
            message.setQos(qos);
            client.publish(topic, message);
            Thread.sleep(delay);
            counter++;
        }
//        System.out.println("finally" + counter);
        MqttMessage message = new MqttMessage(Integer.toString(counter).getBytes());
        message.setQos(qos);
        client.publish(String.format("published_count/%d/%d/%d", instanceId, qos, delay), message);
    }
}
