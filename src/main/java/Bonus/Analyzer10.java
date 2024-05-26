package Bonus;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class implements an MQTT client analyzer that subscribes to topics published by up to 10 publishers.
 * It collects data for a fixed duration, computes statistical measures, and prints the results to the console.
 */
public class Analyzer10 {

    private static final String BROKER_URL = "tcp://localhost:1883";
    private MqttClient client;
    private Map<String, InstanceData> dataMap = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        new Analyzer10().runAnalyser(10);
    }

    /**
     * Initializes and runs the MQTT client to collect data from publishers.
     * @param numInstances Number of publisher instances to monitor.
     */
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
                public void messageArrived(String topic, MqttMessage message) {
                    handleMessages(topic, message);
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                }
            });

            subscribeTopics();
            // Wait for date.
            Thread.sleep(70000);
            printStatistics();
        } catch (MqttException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            try {
                client.disconnect();
            } catch (MqttException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Subscribes to topics relevant to the analysis.
     */
    private void subscribeTopics() {
        try {
            client.subscribe("counter/#");
            client.subscribe("published_count/#");
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    /**
     * Handles incoming MQTT messages by directing them to appropriate processing functions.
     * @param topic The topic of the received message.
     * @param message The received MQTT message.
     */
    private void handleMessages(String topic, MqttMessage message) {
        String payload = new String(message.getPayload());
        String[] topicParts = topic.split("/");

        if (topic.startsWith("counter")) {
            handleCounterMessage(topicParts, payload);
        } else if (topic.startsWith("published_count")) {
            handlePublishedCountMessage(topicParts, payload);
        }
    }

    /**
     * Processes messages on counter topics, updating count data.
     * @param topicParts Parts of the topic used to categorize the message.
     * @param payload The payload of the message.
     */
    private void handleCounterMessage(String[] topicParts, String payload) {
        if (topicParts.length != 4) {
            return;
        }
        String key = String.join("/", Arrays.copyOf(topicParts, 4));
        dataMap.putIfAbsent(key, new InstanceData());
        InstanceData data = dataMap.get(key);
        synchronized (data) {
            int count = Integer.parseInt(payload);
            data.messages.add(new Message(count, System.currentTimeMillis()));
        }
    }

    /**
     * Processes messages indicating the total count of messages published by a publisher.
     * @param topicParts Parts of the topic used to categorize the message.
     * @param payload The payload of the message.
     */
    private void handlePublishedCountMessage(String[] topicParts, String payload) {
        if (topicParts.length != 4) {
            return;
        }
        String key = String.join("/", Arrays.copyOf(topicParts, 4));
        InstanceData data = dataMap.get(key);
        synchronized (data) {
            data.publishedCount = Integer.parseInt(payload);
        }
    }

    /**
     * Prints statistics computed from collected data to the console.
     */
    private void printStatistics() {
        dataMap.forEach((key, data) -> {
            synchronized (data) {
                long totalMessages = data.messages.size();
                double messageLossRate = 100.0 * (1 - ((double) totalMessages / data.publishedCount));
                List<Double> gaps = new ArrayList<>();
                long outOfOrderCount = 0;
                for (int i = 1; i < data.messages.size(); i++) {
                    gaps.add((double) (data.messages.get(i).timestamp - data.messages.get(i - 1).timestamp));
                    if (data.messages.get(i).value < data.messages.get(i - 1).value) {
                        outOfOrderCount++;
                    }
                }
                double medianGap = gaps.isEmpty() ? 0 : median(gaps);
                double outOfOrderRate = 100.0 * outOfOrderCount / totalMessages;

                //The calculated data was not output because ten Publishers running at the same time would report errors.
            }
        });
    }

    /**
     * Calculates the median value from a list of doubles.
     * @param values List of values to compute the median from.
     * @return Median value.
     */
    private double median(List<Double> values) {
        Collections.sort(values);
        if (values.size() % 2 == 1) {
            return values.get(values.size() / 2);
        } else {
            int midIndex = values.size() / 2;
            return (values.get(midIndex - 1) + values.get(midIndex)) / 2.0;
        }
    }

    /**
     * Represents data associated with a particular publisher instance, tracking message counts and timestamps.
     */
    static class InstanceData {
        int publishedCount;
        List<Message> messages = new ArrayList<>();
    }

    /**
     * Represents an individual message, holding its sequence number and the timestamp when it was received.
     */
    static class Message {
        int value;
        long timestamp;

        public Message(int value, long timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }
    }
}
