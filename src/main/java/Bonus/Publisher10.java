package Bonus;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Hu
 */
public class Publisher10 {
    private static final ExecutorService executorService = Executors.newCachedThreadPool();
    private static final int NUM_PUBLISHERS = 10;  // Number of publishers to start

    public static void main(String[] args) {
        try {
            for (int i = 1; i <= NUM_PUBLISHERS; i++) {
                final int instanceId = i;
                executorService.submit(() -> runPublisher(instanceId));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Prepares and starts the publishing process with fixed delay and QoS.
     *
     * @param instanceId Index of the publisher instance
     */
    private static void runPublisher(int instanceId) {
        try {
            MqttClient client = new MqttClient("tcp://localhost:1883", MqttClient.generateClientId(), new MemoryPersistence());
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);
            client.connect(options);

            int delay = 0;  // Fixed delay
            int qos = 0;    // Fixed QoS
            publishMessages(client, delay, qos, instanceId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Publishes messages to a specific MQTT topic based on the given delay and quality of service (QoS) settings.
     * Continuously publishes messages for one minute (60000 milliseconds).
     *
     * @param client      The MQTT client used to publish messages.
     * @param delay       Time in milliseconds to wait between consecutive message publications.
     * @param qos         Quality of service level at which the messages should be published.
     * @param instanceId  Identifier for the instance of the publisher sending the messages.
     * @throws Exception  Handles exceptions related to MQTT connections and thread interruptions.
     */
    private static void publishMessages(MqttClient client, int delay, int qos, int instanceId) throws Exception {
        long endTime = System.currentTimeMillis() + 60000;  // One minute from now
        int counter = 0;  // Message counter

        while (System.currentTimeMillis() < endTime) {
            String topic = String.format("counter/%d/%d/%d", instanceId, qos, delay);
            MqttMessage message = new MqttMessage(Integer.toString(counter).getBytes());
            message.setQos(qos);
            client.publish(topic, message);
            Thread.sleep(delay);  // Wait for the specified delay, if any
            counter++;
        }

        // Publish the final count of messages sent
        MqttMessage message = new MqttMessage(Integer.toString(counter).getBytes());
        message.setQos(qos);
        client.publish(String.format("published_count/%d/%d/%d", instanceId, qos, delay), message);
    }
}
