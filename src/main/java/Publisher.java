import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class Publisher {
    private static final String BROKER_URL = "tcp://localhost:1883";
    private static final String CLIENT_ID = "pub_client";
    private MqttClient client;
    private int[] qosOptions = {0, 1, 2};
    private int[] delayOptions = {0, 1, 2, 4};
    private int INSTANCE_ID = 0;
    private static final long PUBLISH_DURATION_MS = 1; // 60 seconds

    public Publisher() throws MqttException {
        this.client = new MqttClient(BROKER_URL, CLIENT_ID, new MemoryPersistence());
        connect();
    }

    private void connect() throws MqttException {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setConnectionTimeout(60);
        options.setKeepAliveInterval(60);
        client.connect(options);
        client.setCallback(new MqttCallbackExtended() {
            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                System.out.println("Connected to broker: " + serverURI);
            }

            @Override
            public void connectionLost(Throwable cause) {
                System.out.println("Connection lost: " + cause.getMessage());
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                System.out.println("Message arrived: " + new String(message.getPayload()));
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
                System.out.println("Delivery complete for token: " + token.getResponse());
            }
        });
    }

    private void disconnect() {
        try {
            if (client != null) {
                client.disconnect();
                System.out.println("Disconnected safely.");
                client.close(); // Additionally ensure the client is closed after disconnect
            }
        } catch (MqttException e) {
            System.out.println("Error while disconnecting: " + e.getMessage());
        }
    }

    public void startPublishing() throws MqttException, InterruptedException {
        try {
            for (int qos : qosOptions) {
                for (int delay : delayOptions) {
                    long startTime = System.currentTimeMillis();
                    int counter = 0;
                    while (System.currentTimeMillis() - startTime < PUBLISH_DURATION_MS) {
                        String content = "Counter: " + counter++;
                        String topic = String.format("counter/%d/%d/%d", INSTANCE_ID, qos, delay);
                        MqttMessage message = new MqttMessage(content.getBytes());
                        message.setQos(qos);
                        client.publish(topic, message);
                        System.out.println("Published to " + topic + ": " + content);
                        Thread.sleep(delay);
                    }
                }
            }
        }finally {
            disconnect();
        }
    }

    public static void main(String[] args) {
        try {
            for (int i = 1; i <= 5; i++) {
                Publisher publisher = new Publisher();
                publisher.INSTANCE_ID = i;
                publisher.startPublishing();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}