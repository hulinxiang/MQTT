import org.eclipse.paho.client.mqttv3.MqttMessage;

public class Analyser {
    public static void analyseMessage(MqttMessage message) {
        // 在这里实现您的分析逻辑
        System.out.println("分析消息: " + new String(message.getPayload()));
        // 示例：检查重复消息、消息顺序等。
    }
}
