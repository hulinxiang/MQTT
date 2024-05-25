package WireShark;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/**
 * This class subscribes to an MQTT topic and is used to analyze how
 * different QoS levels impact the communication process.
 */
public class WiresharkSubscriber {
    public static void main(String[] args) {
        // MQTT broker URL and port
        String broker = "tcp://localhost:1883";
        // Client identifier used to communicate with the MQTT broker
        String clientId = "Subscriber";
        // MQTT topic to subscribe to messages
        String topic = "test/wireshark";

        try {
            // Create an instance of the MQTT client
            MqttClient client = new MqttClient(broker, clientId, new MemoryPersistence());
            // Set callback methods to handle incoming messages
            client.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    // Called when the client lost the connection to the broker
                    System.out.println("Connection lost: " + cause.getMessage());
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    // Called when a message arrives from the server.
                    System.out.println("Message arrived. Topic: " + topic + " Message: " + new String(message.getPayload()));
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    // Called when a message has been delivered to the server
                    System.out.println("Delivery complete");
                }
            });

            // Create connection options, set clean session flag to true to start a new clean session
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            System.out.println("Connecting to broker: " + broker);
            // Connect to the MQTT broker
            client.connect(connOpts);
            System.out.println("Connected");

            // Subscribe to the specified topic with QoS level 0
            System.out.println("Subscribing to topic: " + topic);
            client.subscribe(topic, 0);
            System.out.println("Subscribed to topic: " + topic);

            // Keep the client running to listen for messages
            System.out.println("Waiting for messages...");
            while (true) {
                // Wait to keep the main thread alive. This is usually not recommended in production code
                Thread.sleep(1000);
            }

        } catch (MqttException | InterruptedException me) {
            // Catch and handle possible exceptions

        }
    }
}