package kafka.examples.difcExampleWorkflow;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class UserAddressService {

    private static final Map<String, String> ADDRESS_STORE = new HashMap<>();

    public static void main(String[] args) {
        Properties props = KafkaConfig.consumerProps(
                "user-address-group",
                "user-address-service"
        );

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.registerClient();

        consumer.subscribe(Collections.singletonList(KafkaConfig.TOPIC));

        System.out.println("User Address Service started...");

        while(true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));

            consumer.addTag("user-events");

            for (ConsumerRecord<String, String> record : records) {
                String value = record.value();

                if(value == null)
                {
                    System.out.println("Client is unauthorized to access record...");
                    continue;
                }
                System.out.println(value);
                if (value.startsWith("USER_CREATED"))
                {
                    String userId = extract(value, "userId");
                    String username = extract(value, "username");

                    String fakeAddress = username + " Street, City-X";
                    ADDRESS_STORE.put(userId, fakeAddress);

                    System.out.println(
                            "[AddressService] Stored address for " +
                                    username + " => " + fakeAddress
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
