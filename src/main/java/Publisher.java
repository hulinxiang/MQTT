import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.json.JSONObject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class Publisher {
    private static final String BROKER_URL = "tcp://localhost:1883";
    private static final String CLIENT_ID = "publish_client";
    private MqttClient client;
    private int currentQos = 0;
    private int currentDelay = 1000;
    private static final int DURATION = 20000;
    private MqttMessage message;
//    private ExecutorService executor = Executors.newCachedThreadPool();  // 创建一个线程池


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

            // 这个只被触发了一次
            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                String payload = new String(message.getPayload());
                if ("request/config".equals(topic)) {
                    JSONObject config = new JSONObject(payload);
                    currentQos = config.getInt("qos");
                    currentDelay = config.getInt("delay");
                    int instanceId = config.getInt("instanceId");
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

    private void publishData(int instanceId) throws MqttException, InterruptedException {
        long startTime = System.currentTimeMillis();
        int count = 0;
        System.out.println("我要进去了哦");
        while (System.currentTimeMillis() - startTime < DURATION) {
            System.out.println("正在执行 count: " + count);
            String topic = String.format("counter/%d/%d/%d", instanceId, currentQos, currentDelay);
            message = new MqttMessage(String.valueOf(count).getBytes());
            message.setQos(currentQos);
            System.out.println(topic);
            System.out.println(message);
            try {
                if (client.isConnected()) {
                    client.publish(topic, message);
                    System.out.println("Finish publishing");
                } else {
                    System.out.println("Client is not connected.");
                }
            } catch (MqttException e) {
                System.out.println("Failed to publish message: " + e.getMessage());
                e.printStackTrace();
            }
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
