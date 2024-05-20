import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class Subscriber {
    String broker = "tcp://localhost:1883";
    String topic = "mqtt/test";
    String clientid = "subscribe_client";
    int[] qos = new int[]{0, 1, 2};

    public Subscriber() {
        connect();
    }

    public void connect() {
        try {
            MqttClient client = new MqttClient(broker, clientid, new MemoryPersistence());
            MqttConnectOptions options = new MqttConnectOptions();
            options.setConnectionTimeout(60);
            options.setKeepAliveInterval(60);
            client.setCallback(new MqttCallback() {

                @Override
                public void connectionLost(Throwable cause) {
                    System.out.println("连接丢失: " + cause.getMessage());
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) {
                    System.out.println("主题: " + topic);
                    System.out.println("Publisher QoS: " + message.getQos());
                    System.out.println("Subscriber QoS: " + 2);
                    System.out.println("消息内容: " + new String(message.getPayload()));
//                        Analyser.analyseMessage(message);
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    System.out.println("消息送达完毕: " + token.isComplete());
                }
            });
            client.connect(options);
            for (int qos = 0; qos <= 2; qos++) {
                client.subscribe(topic, qos);
                System.out.println("订阅主题: " + topic + " 使用QoS: " + qos);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Subscriber subscriber = new Subscriber();
    }
}
