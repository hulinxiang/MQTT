import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class Publisher {
    String broker = "tcp://localhost:1883";
    String topic = "mqtt/test";
    String clientid = "publish_client";
    String content = "Hello MQTT";
    int qos = 0;

    public Publisher() {
        connect();
    }


    public void connect() {
        try {
            MqttClient client = new MqttClient(broker, clientid, new MemoryPersistence());
            // 连接参数
            MqttConnectOptions options = new MqttConnectOptions();
            // 设置用户名和密码
            options.setConnectionTimeout(60);
            options.setKeepAliveInterval(60);
            client.setCallback(new MqttCallback(){

                @Override
                public void connectionLost(Throwable cause) {

                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {

                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    System.out.println("deliveryComplete---------" + token.isComplete());
                }
            });


            // 连接
            client.connect(options);
            // 创建消息并设置 QoS
            MqttMessage message = new MqttMessage(content.getBytes());
            message.setQos(qos);
            // 发布消息
            client.publish(topic, message);
            System.out.println("Message published");
            System.out.println("topic: " + topic);
            System.out.println("message content: " + content);
            // 关闭连接
            client.disconnect();
            // 关闭客户端
            client.close();
        } catch (MqttException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        Publisher publisher = new Publisher();
    }
}