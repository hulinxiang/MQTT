import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Publisher {

    public static void main(String[] args) {
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        for (int i = 1; i <= 5; i++) {
            int finalI = i;
            executorService.submit(() -> runPublisher(finalI));
        }
    }

    private static void runPublisher(int instanceId) {
        try {
            Map<String, String> userdata = new HashMap<>();
            MqttClient client = new MqttClient("tcp://localhost:1883", MqttClient.generateClientId(), new MemoryPersistence());
            client.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    // Handle connection lost
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    String[] topicParts = topic.split("/");
                    if (topicParts.length == 2) {
                        String param = topicParts[1];
                        userdata.put(param, new String(message.getPayload()));
                        userdata.put("updated", "true");
                    }
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    // Message delivery complete
                }
            });

            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);
            client.connect(options);
            client.subscribe("request/#");

            while (true) {
                if ("true".equals(userdata.get("updated")) && userdata.containsKey("delay") && userdata.containsKey("qos")) {
                    userdata.put("updated", "false");
                    int delay = Integer.parseInt(userdata.getOrDefault("delay", "1000"));  // Default delay 1000 ms
                    int qos = Integer.parseInt(userdata.getOrDefault("qos", "0"));  // Default QoS 0
                    publishMessages(client, delay, qos, instanceId);
                }
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void publishMessages(MqttClient client, int delay, int qos, int instanceId) throws Exception {
        long endTime = System.currentTimeMillis() + 60000;
        int counter = 0;
        while (System.currentTimeMillis() < endTime) {
            String topic = String.format("counter/%d/%d/%d", instanceId, qos, delay);
            MqttMessage message = new MqttMessage(Integer.toString(counter).getBytes());
            message.setQos(qos);
            client.publish(topic, message);
            Thread.sleep(delay);
            counter++;
        }
        System.out.println("finally" + counter);
        MqttMessage message = new MqttMessage(Integer.toString(counter).getBytes());
        message.setQos(qos); // 设置QoS
        client.publish(String.format("published_count/%d/%d/%d", instanceId, qos, delay), message);
    }
}
