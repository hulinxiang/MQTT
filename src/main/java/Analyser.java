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
    private Map<String, List<String>> systemMetricsData = new HashMap<>();
    private int currentInstanceId = 0;
    private int currentPublisherQoS = 0;
    private int currentAnalyzerQoS = 0;
    private int currentDelay = 0;


    public static void main(String[] args) {
        new Analyser().runAnalyser(5);
    }

    public void runAnalyser(int numInstances) {
        try {
            client = new MqttClient(BROKER_URL, MqttClient.generateClientId(), new MemoryPersistence());
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            client.connect(connOpts);

            client.setCallback(new MqttCallback() {

                @Override
                public void connectionLost(Throwable cause) {
                    System.out.println("Connection lost: " + cause.getMessage());
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws IOException {
                    handleMessages(topic, message);
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {

                }
            });

            subscribeTopics();

            for (int instanceId = 1; instanceId <= numInstances; instanceId++) {
                for (int publisherQos = 0; publisherQos < 3; publisherQos++) {
                    for (int analyzerQos = 0; analyzerQos < 3; analyzerQos++) {
                        currentInstanceId = instanceId;
                        currentPublisherQoS = publisherQos;
                        currentAnalyzerQoS = analyzerQos;

                        updateSubscription(analyzerQos);
                        for (int delay : new int[]{0, 1, 2, 4}) {
                            currentDelay = delay;
                            sendConfigurationRequests(instanceId, publisherQos, delay);
                            Thread.sleep(70000);  // Wait for messages to accumulate
                            writeSysMetricsToFile();
                            calculateStatistics(instanceId, publisherQos, analyzerQos);
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
        this.currentAnalyzerQoS = qos;
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

    private void handleMessages(String topic, MqttMessage message) throws IOException {
        String payload = new String(message.getPayload());
        String[] topicParts = topic.split("/");
        if (topic.startsWith("counter")) {
            handleCounterMessage(topicParts, payload, currentAnalyzerQoS);
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
        String key = String.join("/", Arrays.copyOf(topicParts, 4));
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
        String[] cur=Arrays.copyOf(topicParts,4);
        cur[0]="counter";
        String key = String.join("/", cur);
        InstanceData data = dataMap.get(key);
        synchronized (data) {
            data.publishedCount = Integer.parseInt(payload);
        }
    }

    private void handleSysMetrics(String topic, String payload) {
        if (!systemMetricsData.containsKey(topic)) {
            systemMetricsData.put(topic, new ArrayList<>());
        }
        String logEntry = String.format("System metric [%s]: %s", topic, payload);
        systemMetricsData.get(topic).add(logEntry);
    }

    private void calculateStatistics(int numPublishers, int publisherQos, int analyzerQos) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter("statistics_results.txt", true));  // Append mode
        writer.write(String.format("Number of publishers: %d\n", numPublishers));
        writer.write(String.format("Publisher QoS: %d\n", publisherQos));
        writer.write(String.format("Analyzer QoS: %d\n", analyzerQos));
        System.out.println(dataMap.size());
        dataMap.forEach((key, data) -> {
            synchronized (data) {
                List<Message> messages = data.messagesByQos.get(analyzerQos);
                if (messages == null) {
                    return;
                }

                messages.sort(Comparator.comparingLong(m -> m.timestamp));

                long receivedMessages = messages.size();
                double totalRate = receivedMessages / 60.0;
                long expectedMessages = data.publishedCount;
                double messageLossRate = (expectedMessages > 0) ? 100.0 * (1 - ((double) receivedMessages / expectedMessages)) : 0;

                long outOfOrderCount = 0;
                for (int i = 1; i < messages.size(); i++) {
                    if (messages.get(i).value < messages.get(i - 1).value) {
                        outOfOrderCount++;
                    }
                }
                double outOfOrderRate = (receivedMessages > 1) ? 100.0 * outOfOrderCount / receivedMessages : 0;

                List<Double> gaps = new ArrayList<>();
                for (int i = 1; i < messages.size(); i++) {
                    if (messages.get(i).value == messages.get(i - 1).value + 1) {
                        gaps.add((double) (messages.get(i).timestamp - messages.get(i - 1).timestamp));
                    }
                }
                Double medianGap = gaps.isEmpty() ? null : median(gaps);

                try {
                    writer.write(String.format("Configuration: %s\n", key));
                    writer.write(String.format("Received messages: %d\n", receivedMessages));
                    writer.write(String.format("Expected messages: %d\n", expectedMessages));
                    writer.write(String.format("Current number of publishers: %d\n", currentInstanceId));
                    writer.write(String.format("Total average rate: %.2f messages/sec\n", totalRate));
                    writer.write(String.format("Message loss rate: %.2f%%\n", messageLossRate));
                    writer.write(String.format("Out of order rate: %.2f%%\n", outOfOrderRate));
                    writer.write(medianGap != null ? String.format("Median inter-message gap: %.2f ms\n", medianGap) : "Median inter-message gap: N/A\n");
                    writer.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        writer.write("--------------------------------------------------\n");
        dataMap.clear();
        writer.close();
    }

    private void writeSysMetricsToFile() throws IOException {
        String sysMetricsFilename = "sys_metrics.txt";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(sysMetricsFilename, true))) {
            for (Map.Entry<String, List<String>> entry : systemMetricsData.entrySet()) {
                for (String logEntry : entry.getValue()) {
                    writer.write(logEntry);
                    writer.newLine();
                }
            }
            writer.write(String.format("Current number of publishers: %d\n", currentInstanceId));
            writer.write(String.format("Current Publisher QoS: %d\n", currentPublisherQoS));
            writer.write(String.format("Current Analyzer QoS: %d\n", currentAnalyzerQoS));
            writer.write(String.format("Current Delay: %d\n", currentDelay));
            writer.write("--------------------------------------------------\n");
            writer.flush();
            systemMetricsData.clear();  // Clear data after writing
        } catch (IOException e) {
            System.err.println("Error writing system metrics: " + e.getMessage());
            throw e;
        }
    }

    // Helper method to calculate median
    private Double median(List<Double> values) {
        Collections.sort(values);
        int middle = values.size() / 2;
        if (values.size() % 2 == 1) {
            return values.get(middle);
        } else {
            return (values.get(middle - 1) + values.get(middle)) / 2.0;
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