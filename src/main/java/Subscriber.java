import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class Subscriber {
    private static final String BROKER_URL = "tcp://localhost:1883";
    private static final String CLIENT_ID = "subscribe_client";
    private MqttClient client;
    private static final int[] QOS_OPTIONS = {0, 1, 2};  // QoS 级别数组
    private static final int[] DELAY_OPTIONS = {0, 1, 2, 4};  // 延迟选项数组
    private static final int INSTANCE_COUNT = 5;  // 实例计数

    public Subscriber() {
        connect();
    }

    public void connect() {
        try {
            client = new MqttClient(BROKER_URL, CLIENT_ID, new MemoryPersistence());
            MqttConnectOptions options = new MqttConnectOptions();
            options.setConnectionTimeout(60);
            options.setKeepAliveInterval(60);
            client.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    System.out.println("Connection lost: " + cause.getMessage());
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) {
                    System.out.println("Topic: " + topic);
                    System.out.println("Publisher QoS: " + message.getQos());
                    System.out.println("Message content: " + new String(message.getPayload()));
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    System.out.println("Message delivery complete: " + token.isComplete());
                }
            });
            client.connect(options);
            subscribeToTopics();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void subscribeToTopics() {
        // 构造并订阅每一个主题
        try {
            for (int instance = 1; instance <= INSTANCE_COUNT; instance++) {
                for (int pubQos : QOS_OPTIONS) {
                    for (int delay : DELAY_OPTIONS) {
                        String topic = String.format("counter/%d/%d/%d", instance, pubQos, delay);
                        for (int subQos : QOS_OPTIONS) {
                            client.subscribe(topic, subQos);
                            System.out.println("Subscribed to topic: " + topic + " with QoS " + subQos);
                        }
                    }
                }
            }
        } catch (MqttException e) {
            System.out.println("Failed to subscribe");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new Subscriber();
    }
}
