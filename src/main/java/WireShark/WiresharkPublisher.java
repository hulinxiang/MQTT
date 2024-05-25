package WireShark;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/**
 * @author Hu
 * It is a method for sending one message to analyse how different
 * Qos levels will impact the communication process.
 */
public class WiresharkPublisher {
    public static void main(String[] args) {
        // MQTT broker URL and port
        String broker = "tcp://localhost:1883";
        // Client identifier used to communicate with the MQTT broker
        String clientId = "Hello MQTT";
        // MQTT topic to publish messages
        String topic = "test/wireshark";
        // Message content to send
        String content = "Hello MQTT";

        try {
            // Create an instance of the MQTT client
            MqttClient sampleClient = new MqttClient(broker, clientId, new MemoryPersistence());
            // Create connection options, set clean session flag to true to start a new clean session
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            System.out.println("Connecting to broker: " + broker);
            // Connect to the MQTT broker
            sampleClient.connect(connOpts);
            System.out.println("Connected");

            // Create a new MQTT message, set the quality of service level to 0 (at most once delivery)
            MqttMessage message = new MqttMessage(content.getBytes());
            message.setQos(2);
            System.out.println("Publishing message: " + content);
            // Publish the message to the specified topic
            sampleClient.publish(topic, message);
            System.out.println("Message published");

            // Disconnect from the MQTT broker
            sampleClient.disconnect();
            System.out.println("Disconnected");
            // Safely exit the program
            System.exit(0);
        } catch (MqttException me) {
            // Catch and handle possible exceptions
            System.err.println("reason " + me.getReasonCode());
            System.err.println("msg " + me.getMessage());
            System.err.println("loc " + me.getLocalizedMessage());
            System.err.println("cause " + me.getCause());
            System.err.println("excep " + me);
            me.printStackTrace();
        }
    }
}
