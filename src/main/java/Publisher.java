import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.json.JSONObject;

public class Publisher {
    private static final String BROKER_URL = "tcp://localhost:1883";
    private static final String CLIENT_ID = "publish_client";
    private MqttClient client;
    private int currentQos = 0;
    private int currentDelay = 1000;
    private static final int DURATION = 60000;

    public Publisher() {
        try {
            connect();
            subscribeToRequestTopics();
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
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                String payload = new String(message.getPayload());
                if ("request/config".equals(topic)) {
                    JSONObject config = new JSONObject(payload);
                    currentQos = config.getInt("qos");
                    currentDelay = config.getInt("delay");
                    String instanceId = config.getString("instanceId");
                    System.out.println("publishdata前");
                    publishData(instanceId); // 根据最新的配置开始发送数据
                }
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
                System.out.println("Delivery complete. Token: " + token.getResponse());
            }
        });
    }

    private void subscribeToRequestTopics() throws MqttException {
        client.subscribe("request/config"); // 只订阅一个用于配置更新的主题
    }

    private void publishData(String instanceId) throws MqttException, InterruptedException {
        long startTime = System.currentTimeMillis();
        int count = 0;
        while (System.currentTimeMillis() - startTime < DURATION) {
            String topic = String.format("counter/%s/%d/%d", instanceId, currentQos, currentDelay);
            MqttMessage message = new MqttMessage(String.valueOf(count).getBytes());
            message.setQos(currentQos);
            client.publish(topic, message);
            System.out.println("Published message: " + count + " to topic: " + topic);
            count++;
            if (currentDelay!=0){
                Thread.sleep(currentDelay);
            }
        }
    }

    public static void main(String[] args) {
        new Publisher();
    }
}
