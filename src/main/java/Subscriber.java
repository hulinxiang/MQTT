import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.json.JSONObject;

public class Subscriber {
    private static final String BROKER_URL = "tcp://localhost:1883";
    private static final String CLIENT_ID = "subscribe_client";
    private MqttClient client;
    private static final int[] QOS_OPTIONS = {0, 1, 2};
    private static final int[] DELAY_OPTIONS = {0, 1, 2, 4};
    private static final int INSTANCE_COUNT = 5;
    private static final int PUBLISH_DURATION = 60000;


    public Subscriber() {
        try {
            connect();
            subscribeToTopics();
            sendAllConfigurationUpdates(); // 发送所有可能的配置更新
        } catch (Exception e) {
            System.out.println("Error in connection or subscription: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void connect() throws MqttException {
        client = new MqttClient(BROKER_URL, CLIENT_ID, new MemoryPersistence());
        MqttConnectOptions options = new MqttConnectOptions();
        options.setConnectionTimeout(60);
        options.setKeepAliveInterval(60);
        client.connect(options);
        client.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable cause) {
                System.out.println("Connection lost: " + cause.getMessage());
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
                System.out.println("Message arrived. Topic: " + topic + ", QoS: " + message.getQos() + ", Content: " + new String(message.getPayload()));
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
                System.out.println("Delivery complete. Token: " + token.getResponse());
            }
        });
    }

    private void subscribeToTopics() throws MqttException {
        for (int instance = 1; instance <= INSTANCE_COUNT; instance++) {
            for (int pubQos : QOS_OPTIONS) {
                for (int delay : DELAY_OPTIONS) {
                    String topic = String.format("counter/%d/%d/%d", instance, pubQos, delay);
                    for (int subQos : QOS_OPTIONS) {
                        client.subscribe(topic, subQos);
                        System.out.println("Subscribed to topic: " + topic + " with Subscription QoS " + subQos);
                    }
                }
            }
        }
    }


    private void sendAllConfigurationUpdates() throws MqttException, InterruptedException {
        // 准备所有配置更新
        for (int instance = 1; instance <= INSTANCE_COUNT; instance++) {
            for (int qos : QOS_OPTIONS) {
                for (int delay : DELAY_OPTIONS) {
                    JSONObject config = new JSONObject();
                    config.put("qos", qos);
                    config.put("delay", delay);
                    config.put("instanceId", instance);
                    updateConfiguration(config.toString());
                }
            }
        }
    }

    private void updateConfiguration(String config) throws MqttException, InterruptedException {
        client.publish("request/config", new MqttMessage(config.getBytes()));
        Thread.sleep(PUBLISH_DURATION);
    }

    public static void main(String[] args) {
        new Subscriber();
    }
}