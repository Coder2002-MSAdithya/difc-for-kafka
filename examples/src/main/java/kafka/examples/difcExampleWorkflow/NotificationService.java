package kafka.examples.difcExampleWorkflow;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class NotificationService {

    public static void main(String[] args) {
        Properties props = KafkaConfig.consumerProps(
                "notification-group",
                "notification-service"
        );

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.registerClient();

        consumer.subscribe(Collections.singletonList(KafkaConfig.TOPIC));

        System.out.println("Notification Service started...");

        while(true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));

            consumer.addTag("user-events");
            consumer.addTag("user-events-s");

            for (ConsumerRecord<String, String> record : records) {
                String value = record.value();

                if(value == null)
                {
                    System.out.println("Client is unauthorized to access record...");
                    continue;
                }

                if (value.startsWith("USER_CREATED")) {
                    String username = extract(value, "username");
                    String email = extract(value, "email");

                    System.out.println(
                            "[NotificationService] Sending welcome email to " +
                                    username + " (" + email + ")"
                    );
                }
            }
        }
    }

    private static String extract(String event, String key) {
        String[] parts = event.split("\\|");
        for (String p : parts) {
            if (p.startsWith(key + "=")) {
                return p.substring(key.length() + 1);
            }
        }
        return "UNKNOWN";
    }
}

