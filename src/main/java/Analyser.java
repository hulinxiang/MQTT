import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Analyser class sets up an MQTT client to subscribe to specific topics,
 * collect messages, and perform analysis based on the received data. It supports
 * dynamic configuration of MQTT message publishers based on received settings.
 */
public class Analyser {

    // MQTT broker URL
    private static final String BROKER_URL = "tcp://localhost:1883";
    // MQTT client instance
    private MqttClient client;
    // Stores data received from publishers
    private Map<String, InstanceData> dataMap = new ConcurrentHashMap<>();
    // Stores system metrics received
    private Map<String, List<String>> systemMetricsData = new HashMap<>();
    private int currentInstanceId = 0;
    private int currentPublisherQoS = 0;
    private int currentAnalyzerQoS = 0;
    private int currentDelay = 0;


    public static void main(String[] args) {
        new Analyser().runAnalyser(5);
    }

    /**
     * Initializes the MQTT client and sets up the message processing loop.
     * @param numInstances The number of publishing instances to manage.
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
                public void messageArrived(String topic, MqttMessage message) throws IOException {
                    handleMessages(topic, message);
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {

                }
            });

            subscribeTopics();

            // Loop to handle configurations and message accumulation
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
                            Thread.sleep(70000);
                            // Wait for messages to accumulate
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


    /**
     * Updates the MQTT subscription to specific topics with a new Quality of Service (QoS) level.
     * This method first unsubscribes from the existing topics, disconnects the client, reconnects it,
     * and subscribes again with the new QoS setting. This ensures that messages are received
     * at the specified QoS level for subsequent publications.
     *
     * @param qos The new QoS level to set for the subscription. QoS levels are:
     *            0 - At most once: The message is delivered at most once, or it may not be delivered at all.
     *            1 - At least once: The message is delivered at least once, or possibly more than once if network failures occur.
     *            2 - Exactly once: The message is delivered exactly once by using a four-step handshake.
     * @throws MqttException if any MQTT related errors occur during the subscription update process.
     */
    private void updateSubscription(int qos) throws MqttException {
        // Unsubscribe from the existing topics to clear previous subscriptions
        this.currentAnalyzerQoS = qos;
        client.unsubscribe("counter/#");
        client.unsubscribe("published_count/#");
        client.unsubscribe("$SYS/#");

        // Disconnect the client to ensure clean session setup for new subscriptions
        client.disconnect();

        // Reconnect with a clean session to avoid any stale subscriptions
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        client.connect(connOpts);

        // Resubscribe to the topics with the new QoS level to start receiving messages at the specified QoS
        client.subscribe("counter/#", qos);
        client.subscribe("published_count/#", qos);
        client.subscribe("$SYS/#", qos);
    }


    /**
     * Subscribes to essential MQTT topics to receive messages related to counters,
     * published message counts, and system metrics. This method ensures the MQTT client
     * is actively listening for messages on these topics to process and analyze them accordingly.
     */
    private void subscribeTopics() {
        try {
            // Subscribe to the counter topic to receive updates on message counts from publishers.
            // This is typically used to monitor message flow rates and verify message delivery.
            client.subscribe("counter/#");
            // Subscribe to the published count topic to receive the final count of messages published
            // by each publisher. This helps in determining message loss or duplication.
            client.subscribe("published_count/#");
            client.subscribe("$SYS/#");
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    /**
     * Sends configuration requests to publishers by publishing the instance count, QoS, and delay
     * settings to specific MQTT topics. These settings dictate how many publishers should be active,
     * the quality of service at which messages should be delivered, and the delay between messages.
     *
     * @param instanceId The number of publisher instances that should be active. This allows
     *                    dynamic scaling of the number of publishers based on the workload.
     * @param qos The quality of service level at which messages should be delivered. It determines
     *            the guarantee of message delivery:
     *            0 - At most once (fire and forget),
     *            1 - At least once (acknowledged delivery),
     *            2 - Exactly once (assured delivery).
     * @param delay The delay in milliseconds between each message sent by the publishers. This
     *              can help in controlling the flow of messages and adjusting the load on the network.
     * @throws MqttException if there is an error during the publish operation, such as connection issues
     *                       or problems with the MQTT broker.
     */
    private void sendConfigurationRequests(int instanceId, int qos, int delay) throws MqttException {
        // Publish the desired number of active publisher instances to a specific topic.
        // This allows dynamic scaling of publishers based on the provided instance count.
        client.publish("request/instancecount", new MqttMessage(String.valueOf(instanceId).getBytes()));
        // Publish the desired Quality of Service level to a specific topic.
        // Publishers will use this QoS level for sending messages, affecting delivery certainty and overhead.
        client.publish("request/qos", new MqttMessage(String.valueOf(qos).getBytes()));
        // Publish the desired delay between messages to a specific topic.
        // This controls the rate at which messages are sent, helping manage network congestion and message handling.
        client.publish("request/delay", new MqttMessage(String.valueOf(delay).getBytes()));
    }


    /**
     * Processes incoming MQTT messages by dispatching them to specific handler functions
     * based on the topic they were published to. This method acts as a router, directing
     * messages related to message counts, published counts, and system metrics to their
     * respective handling methods.
     *
     * @param topic The MQTT topic on which the message was received, which determines the type of data
     *              and the appropriate handler method.
     * @param message The actual MQTT message received, which contains the payload and other message-specific parameters.
     * @throws IOException If there are any IO exceptions during message handling, particularly when handling system metrics.
     */
    private void handleMessages(String topic, MqttMessage message) throws IOException {
        // Convert the binary payload to a string for processing.
        String payload = new String(message.getPayload());
        // Split the topic string by '/' to analyze its components.
        String[] topicParts = topic.split("/");

        // Split the topic string by '/' to analyze its components.
        if (topic.startsWith("counter")) {
            // Handle counter messages, which typically involve updating or tracking message counts from publishers.
            handleCounterMessage(topicParts, payload, currentAnalyzerQoS);
        } else if (topic.startsWith("published_count")) {
            // Handle published count messages, which provide the total count of messages sent by a publisher.
            handlePublishedCountMessage(topicParts, payload);
        } else if (topic.startsWith("$SYS")) {
            // Handle system metrics messages, which provide diagnostic and health information from the MQTT broker or system.
            handleSysMetrics(topic, payload);
        }

    }

    /**
     * Handles messages on counter topics by updating data structures to track message counts
     * for each publisher and Quality of Service (QoS) level. It ensures that messages are
     * stored correctly to facilitate later analysis.
     *
     * @param topicParts An array of strings derived from the topic the message was published on,
     *                   which includes segments indicating the specific instance, QoS, and other
     *                   details used to categorize the message.
     * @pam payload The message content, which typically includes a count value indicating how
     *              many messages have been sent or received.
     * @param currentSubQos The current QoS level at which the analyzer is subscribing to messages,
     *                      used to differentiate data storage by QoS level in the data map.
     */
    private void handleCounterMessage(String[] topicParts, String payload, int currentSubQos) {
        // Check if the topic has the correct format and expected number of parts.
        // Typically, this would be in the format "counter/instanceId/qos/delay".
        if (topicParts.length != 4) {
            // If the topic does not have the expected segments, do nothing.
            return;
        }
        // Create a unique key from the topic parts to use for storing and retrieving data
        String key = String.join("/", Arrays.copyOf(topicParts, 4));
        // Ensure there is an entry for this key in the data map. If not, initialize it.
        dataMap.putIfAbsent(key, new InstanceData());
        InstanceData data = dataMap.get(key);
        synchronized (data) {
            data.messagesByQos.get(currentSubQos).add(new Message(Integer.parseInt(payload), System.currentTimeMillis()));
        }
    }


    /**
     * Handles messages on the published_count topics by updating the corresponding instance data
     * with the total count of messages published. This data is crucial for comparing expected message
     * counts to actual received message counts, aiding in the analysis of message loss or duplication.
     *
     * @param topicParts An array of strings derived from the topic the message was published on,
     *                   which includes segments indicating the specific instance, QoS, and delay
     *                   used to uniquely identify the message source.
     * @param payload The message content, which is expected to be the total count of messages
     *                that have been published by a specific instance.
     */
    private void handlePublishedCountMessage(String[] topicParts, String payload) {
        // Check if the topic has the correct format and expected number of parts.
        // Typically, this would be in the format "published_count/instanceId/qos/delay".
        if (topicParts.length != 4) {
            // If the topic does not have the expected segments, exit the method.
            return;
        }
        // Modify the first part of the topic parts to "counter" to match the keys used for storing
        // message count data, as the data for published counts should correlate with the data
        // for received messages.
        String[] cur=Arrays.copyOf(topicParts,4);
        cur[0]="counter";
        String key = String.join("/", cur);
        // Retrieve the instance data object associated with the key from the data map.
        InstanceData data = dataMap.get(key);
        // Synchronize on the data object to prevent concurrent modification issues.
        synchronized (data) {
            // Parse the payload as an integer and update the published count in the InstanceData.
            data.publishedCount = Integer.parseInt(payload);
        }
    }


    /**
     * Processes and records system metrics messages from the MQTT broker or other system components.
     * These messages are typically published on system-specific topics and contain valuable
     * diagnostic information. The method logs each message with its topic and payload to facilitate
     * detailed system monitoring and troubleshooting.
     *
     * @param topic The MQTT topic on which the system metric was published, indicating the type of metric.
     * @param payload The content of the system metric message, typically representing a value or status
     *                related to the system's performance or configuration.
     */
    private void handleSysMetrics(String topic, String payload) {
        // Check if there is already a list for this topic in the systemMetricsData map. If not, create one.
        // This ensures that all system metrics for a specific topic are grouped together.
        if (!systemMetricsData.containsKey(topic)) {
            systemMetricsData.put(topic, new ArrayList<>());
        }
        // Format the log entry to include the topic and its payload. This formatting makes it easier to
        // understand the context of the metric when reviewing logs or performing analysis.
        String logEntry = String.format("System metric [%s]: %s", topic, payload);

        // Add the formatted log entry to the list associated with the topic in the systemMetricsData map.
        // This aggregation allows for sequential monitoring and analysis of system metrics over time.
        systemMetricsData.get(topic).add(logEntry);
    }

    /**
     * Calculates and writes statistics based on received messages for each publisher configuration.
     * This includes the total number of messages received, message loss rate, order of messages, and
     * the time gaps between consecutive messages.
     *
     * @param numPublishers The number of publishers that are currently active.
     * @param publisherQos The Quality of Service level at which the publishers are sending messages.
     * @param analyzerQos The Quality of Service level at which the analyzer is subscribing to messages.
     * @throws IOException If there is an error writing to the file.
     */
    private void calculateStatistics(int numPublishers, int publisherQos, int analyzerQos) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter("statistics_results.txt", true));  // Append mode

        // Write headers for the current configuration to the file.
        writer.write(String.format("Number of publishers: %d\n", numPublishers));
        writer.write(String.format("Publisher QoS: %d\n", publisherQos));
        writer.write(String.format("Analyzer QoS: %d\n", analyzerQos));
//        System.out.println(dataMap.size());

        // Iterate through each data entry in the map.
        dataMap.forEach((key, data) -> {
            synchronized (data) {

                // Get the list of messages for the current analyzer QoS.
                List<Message> messages = data.messagesByQos.get(analyzerQos);
                // If there are no messages, skip to the next entry.
                if (messages == null) {
                    return;
                }

                // Sort messages by timestamp to ensure the calculations for gaps and order are accurate.
                messages.sort(Comparator.comparingLong(m -> m.timestamp));

                // Calculate statistics based on the messages.
                long receivedMessages = messages.size();
                double totalRate = receivedMessages / 60.0;
                long expectedMessages = data.publishedCount;
                double messageLossRate = (expectedMessages > 0) ? 100.0 * (1 - ((double) receivedMessages / expectedMessages)) : 0;


                // Calculate the number of out-of-order messages.
                long outOfOrderCount = 0;
                for (int i = 1; i < messages.size(); i++) {
                    if (messages.get(i).value < messages.get(i - 1).value) {
                        outOfOrderCount++;
                    }
                }
                double outOfOrderRate = (receivedMessages > 1) ? 100.0 * outOfOrderCount / receivedMessages : 0;

                // Calculate the median time gap between consecutive messages.
                List<Double> gaps = new ArrayList<>();
                for (int i = 1; i < messages.size(); i++) {
                    if (messages.get(i).value == messages.get(i - 1).value + 1) {
                        gaps.add((double) (messages.get(i).timestamp - messages.get(i - 1).timestamp));
                    }
                }
                Double medianGap = gaps.isEmpty() ? null : median(gaps);


                // Write the calculated statistics to the file.
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

    /**
     * Writes system metrics collected from MQTT messages to a text file. This includes metrics
     * published on system-specific MQTT topics, which are crucial for monitoring the health and
     * performance of the MQTT system. The method ensures that each entry is timestamped and
     * categorized, making it easier to trace back and analyze the system's behavior over time.
     *
     * @throws IOException If there is an error writing to the file, the exception is thrown to be
     *                     handled by the caller, ensuring that file I/O failures are properly managed.
     */
    private void writeSysMetricsToFile() throws IOException {
        // Define the filename for system metrics.
        String sysMetricsFilename = "sys_metrics.txt";

        // Use try-with-resources to ensure that resources are properly closed after the operation is done.
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(sysMetricsFilename, true))) {
            // Iterate over each entry in the system metrics data map.
            for (Map.Entry<String, List<String>> entry : systemMetricsData.entrySet()) {
                // Write each log entry for the specific system metric.
                for (String logEntry : entry.getValue()) {
                    // Write the log entry to the file.
                    writer.write(logEntry);
                    writer.newLine();
                }
            }
            // Write the current configuration settings for reference.
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


    /**
     * This class represents the data structure for storing information about the messages received for each topic.
     * It tracks the count of published messages and organizes received messages by their Quality of Service (QoS) level.
     */
    static class InstanceData {
        // The expected number of messages
        int publishedCount;
        // Maps each QoS level to a list of received messages.
        Map<Integer, List<Message>> messagesByQos = new HashMap<>();

        /**
         * Constructor for the InstanceData class. Initializes the published count to zero
         * and prepares the map to store lists of messages for each QoS level from 0 to 2.
         */
        public InstanceData() {
            this.publishedCount = 0;
            for (int i = 0; i < 3; i++) {
                messagesByQos.put(i, new ArrayList<>());
            }
        }
    }


    /**
     * Represents a single message with its value and the timestamp when it was received.
     * This simple structure is crucial for analyzing the order and timing of MQTT messages.
     */
    static class Message {
        // The numerical value or identifier of the message.
        int value;
        // The timestamp when the message was received, used for timing analysis.
        long timestamp;

        /**
         * Constructs a new Message instance with specified value and timestamp.
         * @param value The numerical value of the message, often used as a sequence number.
         * @param timestamp The exact time the message was received, measured in milliseconds since epoch.
         */
        public Message(int value, long timestamp) {
            // Set the message value, typically used as an identifier or counter.
            this.value = value;
            // Set the timestamp of when the message was received.
            this.timestamp = timestamp;
        }
    }
}