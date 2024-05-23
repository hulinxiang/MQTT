import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Analyser {

    private static final String BROKER_URL = "tcp://localhost:1883";
    private MqttClient client;
    private Map<String, InstanceData> dataMap = new ConcurrentHashMap<>();
    private int currentSubQos = 0;

    public static void main(String[] args) {
        new Analyser().runAnalyser(5);
    }

    public void runAnalyser(int numInstances) {
        try {
            client = new MqttClient(BROKER_URL, MqttClient.generateClientId(), new MemoryPersistence());
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            client.connect(connOpts);

            for (int instanceId = 1; instanceId <= numInstances; instanceId++) {
                for (int publisherQos = 0; publisherQos < 3; publisherQos++) {
                    for (int analyzerQos = 0; analyzerQos < 3; analyzerQos++) {
                        updateSubscription(analyzerQos);
                        for (int delay : new int[]{0, 1, 2, 4}) {
                            sendConfigurationRequests(instanceId, publisherQos, delay);
                            Thread.sleep(70000);  // Wait for messages to accumulate
                            calculateStatistics();
                        }
                    }
                }
            }
        } catch (MqttException | InterruptedException | IOException e) {
            e.printStackTrace();
        } finally {
            try {
                client.disconnect();
            } catch (MqttException e) {
                e.printStackTrace();
            }
        }
    }


    private void updateSubscription(int qos) throws MqttException {
        // 取消订阅现有的主题
        this.currentSubQos = qos;
        client.unsubscribe("counter/#");
        client.unsubscribe("published_count/#");
        client.unsubscribe("$SYS/#");

        // 断开连接
        client.disconnect();

        // 重新连接
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        client.connect(connOpts);

        // 根据新的QoS重新订阅主题
        client.subscribe("counter/#", qos);
        client.subscribe("published_count/#", qos);
        client.subscribe("$SYS/#", qos);
    }


    private void subscribeTopics() {
        try {
            client.subscribe("counter/#");
            client.subscribe("published_count/#");
            client.subscribe("$SYS/#");
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    private void sendConfigurationRequests(int instanceId, int qos, int delay) throws MqttException {
        client.publish("request/instancecount", new MqttMessage(String.valueOf(instanceId).getBytes()));
        client.publish("request/qos", new MqttMessage(String.valueOf(qos).getBytes()));
        client.publish("request/delay", new MqttMessage(String.valueOf(delay).getBytes()));
    }

    private void handleMessages(String topic, MqttMessage message) {
        String payload = new String(message.getPayload());
        String[] topicParts = topic.split("/");
        if (topic.startsWith("counter")) {
            handleCounterMessage(topicParts, payload, currentSubQos);
        } else if (topic.startsWith("published_count")) {
            handlePublishedCountMessage(topicParts, payload);
        } else if (topic.startsWith("$SYS")) {
            handleSysMetrics(topic, payload);
        }
    }

    private void handleCounterMessage(String[] topicParts, String payload, int currentSubQos) {
        if (topicParts.length != 4) {
            return;
        }
        String key = String.join("/", Arrays.copyOf(topicParts, 3));
        dataMap.putIfAbsent(key, new InstanceData());
        InstanceData data = dataMap.get(key);
        synchronized (data) {
            data.messagesByQos.get(currentSubQos).add(new Message(Integer.parseInt(payload), System.currentTimeMillis()));
        }
    }


    private void handlePublishedCountMessage(String[] topicParts, String payload) {
        if (topicParts.length != 4) {
            return;
        }
        String key = String.join("/", Arrays.copyOf(topicParts, 3));
        dataMap.putIfAbsent(key, new InstanceData());
        InstanceData data = dataMap.get(key);
        synchronized (data) {
            data.publishedCount = Integer.parseInt(payload);
        }
    }

    private void handleSysMetrics(String topic, String payload) {
        System.out.println(topic + ": " + payload);
    }

    private void calculateStatistics() throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter("statistics_results.txt"));
        dataMap.forEach((key, data) -> {
            synchronized (data) {
                for (Map.Entry<Integer, List<Message>> entry : data.messagesByQos.entrySet()) {
                    int qos = entry.getKey();
                    List<Message> messages = entry.getValue();
                    messages.sort(Comparator.comparingLong(m -> m.timestamp));

                    // 计算统计数据
                    double totalRate = messages.size() / 60.0;
                    double messageLossRate = 100.0 * (1 - (messages.size() / (double) data.publishedCount));
                    double outOfOrderRate = calculateOutOfOrderRate(messages);
                    Double medianGap = calculateMedianGap(messages);

                    try {
                        writer.write(String.format("Key: %s, QoS: %d\n", key, qos));
                        writer.write(String.format("Total average rate: %.2f messages/sec\n", totalRate));
                        writer.write(String.format("Message loss rate: %.2f%%\n", messageLossRate));
                        writer.write(String.format("Out of order rate: %.2f%%\n", outOfOrderRate));
                        writer.write(medianGap != null ? String.format("Median inter-message gap: %.2f ms\n", medianGap) : "Median inter-message gap: N/A\n");
                        writer.write("-------------------------------\n");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        writer.close();
    }

    private double calculateOutOfOrderRate(List<Message> messages) {
        if (messages.size() < 2) {
            return 0.0;
        }
        int outOfOrderCount = 0;
        Message previousMessage = messages.get(0);

        for (int i = 1; i < messages.size(); i++) {
            Message currentMessage = messages.get(i);
            if (currentMessage.value < previousMessage.value) {
                outOfOrderCount++;
            }
            previousMessage = currentMessage;
        }

        return 100.0 * outOfOrderCount / messages.size();
    }

    private Double calculateMedianGap(List<Message> messages) {
        if (messages.size() < 2) {
            return null;
        }
        List<Double> gaps = new ArrayList<>();

        for (int i = 1; i < messages.size(); i++) {
            double gap = messages.get(i).timestamp - messages.get(i - 1).timestamp;
            gaps.add(gap);
        }

        Collections.sort(gaps);
        if (gaps.size() % 2 == 1) {
            return gaps.get(gaps.size() / 2);
        } else {
            int midIndex = gaps.size() / 2;
            return (gaps.get(midIndex - 1) + gaps.get(midIndex)) / 2.0;
        }
    }

    static class InstanceData {
        int publishedCount;
        Map<Integer, List<Message>> messagesByQos = new HashMap<>();

        public InstanceData() {
            this.publishedCount = 0;
            for (int i = 0; i < 3; i++) {
                messagesByQos.put(i, new ArrayList<>());
            }
        }
    }


    static class Message {
        int value;
        long timestamp;

        public Message(int value, long timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }
    }
}