import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class defines a MQTT Publisher that dynamically adjusts the number of publisher instances and
 * updates settings such as 'qos' and 'delay' based on the messages received from a specified MQTT topic.
 */
public class Publisher {

    private static final ExecutorService executorService = Executors.newCachedThreadPool();
    private static final AtomicInteger currentInstanceCount = new AtomicInteger(0);
    // Stores settings for delay and qos
    private static final Map<String, String> sharedSettings = new HashMap<>();

    private static final Object lock = new Object();
    // Track the number of settings updated
    private static int updateCount = 0;
    // Expected number of updates (qos and delay)
    private static final int expectedUpdates = 2;
    // Flag to indicate if all settings have been updated

    private static boolean settingsUpdated = false;

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
                        // Start the corresponding number of instances to publish messages
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
//                        System.out.println("Updated shared settings with " + key + ": " + new String(message.getPayload())+"-----------------");

                        synchronized (lock) {
                            updateCount++;
                            if (updateCount == expectedUpdates) {
                                settingsUpdated = true;
                                // Notify that all settings have been updated
                                lock.notifyAll();
                                // Reset the update count for next updates
                                updateCount = 0;
                            }
                        }

                    }
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {

                }
            });

            client.subscribe("request/#");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * It is the preparation method for publishing message. It will set the qos and delay for each message
     * @param instanceId It is the index of the instance
     */
    private static void runPublisher(int instanceId) {
        try {
//            System.out.println("Enter runPublisher Method");
            MqttClient client = new MqttClient("tcp://localhost:1883", MqttClient.generateClientId(), new MemoryPersistence());
            // Connection setting
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);
            client.connect(options);
            client.subscribe("request/#");

            while (true) {
                synchronized (lock) {
                    while (!settingsUpdated) {
                        // Use while loop to avoid spurious wakeup
                        // Wait until all settings are updated
                        lock.wait();
                    }
                    // Reset flag after settings are confirmed to be updated
                    settingsUpdated = false;
                }
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

    /**
     * Publishes messages to a specific MQTT topic based on the given delay and quality of service (QoS) settings.
     * It continuously publishes messages for one minute (60000 milliseconds).
     *
     * @param client The MQTT client used to publish messages.
     * @param delay The time in milliseconds to wait between consecutive message publications.
     * @param qos The quality of service level at which the messages should be published.
     *           QoS 0: The broker/client will deliver the message once, with no confirmation.
     *           QoS 1: The broker/client will deliver the message at least once, with confirmation required.
     *           QoS 2: The broker/client will deliver the message exactly once by using a four-step handshake.
     * @param instanceId An identifier for the instance of the publisher that is sending the messages.
     *                   This is used to create a unique topic for each instance.
     * @throws Exception Handles exceptions related to MQTT connections and thread interruptions.
     */
    private static void publishMessages(MqttClient client, int delay, int qos, int instanceId) throws Exception {
        long endTime = System.currentTimeMillis() + 60000;
        // Counter to keep track of the number of messages sent.
        int counter = 0;

        // Loop until the current time is less than the end time.
        while (System.currentTimeMillis() < endTime) {
            // Construct the topic string.
            String topic = String.format("counter/%d/%d/%d", instanceId, qos, delay);
            // Create a new MQTT message with the counter value.
            MqttMessage message = new MqttMessage(Integer.toString(counter).getBytes());
            // Set the quality of service for the message.
            message.setQos(qos);
            // Publish the message to the MQTT broker on the constructed topic.
            client.publish(topic, message);
            // Wait for the specified delay before sending the next message.
            Thread.sleep(delay);
            // Increment the message counter after each publication.
            counter++;
        }
//        System.out.println("finally" + counter);
        // After exiting the loop, send a final message with the total count of messages sent.
        MqttMessage message = new MqttMessage(Integer.toString(counter).getBytes());
        // Create a final message with the total count.
        message.setQos(qos);
        // Publish the final count to a specific topic.
        client.publish(String.format("published_count/%d/%d/%d", instanceId, qos, delay), message);
    }
}
