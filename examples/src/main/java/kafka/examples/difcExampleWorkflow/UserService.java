package kafka.examples.difcExampleWorkflow;

import org.apache.kafka.clients.Capability;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

public class UserService {

    private static final String CLIENT_ID = "user-service";

    public static void main(String[] args) throws Exception {

        Properties props = KafkaConfig.producerProps(CLIENT_ID);
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // === DIFC BOOTSTRAP ===
        try {
            System.out.println(
                    producer.registerClient().errorMessage()
            );

            // Create tags (idempotent)
            producer.createTag("user-events");
            producer.createTag("user-events-s");

            // Label self
            producer.addTag("user-events");

            // Grant downstream privileges
            producer.addClientPrivs(
                    "user-address-service", "user-events", Capability.CAN_ADD
            );

            producer.addClientPrivs(
                    "notification-service", "user-events", Capability.CAN_ADD
            );

            producer.addClientPrivs(
                    "notification-service", "user-events-s", Capability.CAN_ADD
            );
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("User Service started (interactive DIFC mode).");
        System.out.println("Commands:");
        System.out.println("  create <username> <email>");
        System.out.println("  exit");

        BufferedReader reader =
                new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));

        String line;

        while ((line = reader.readLine()) != null) {
            line = line.trim();

            if (line.equalsIgnoreCase("exit")) {
                break;
            }

            if (!line.startsWith("create")) {
                System.out.println("Unknown command. Use: create <username> <email>");
                continue;
            }

            String[] parts = line.split("\\s+");
            if (parts.length != 3) {
                System.out.println("Usage: create <username> <email>");
                continue;
            }

            String username = parts[1];
            String email = parts[2];
            String userId = UUID.randomUUID().toString();

            String event = String.format(
                    "USER_CREATED|userId=%s|username=%s|email=%s",
                    userId, username, email
            );

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(KafkaConfig.TOPIC, userId, event);

            try {
                if (email.startsWith("admin")) {
                    producer.sendWithTags(record, Set.of("user-events-s"));
                    System.out.println(
                            "[INTERACTIVE] Published ADMIN event: " + event
                    );
                } else {
                    producer.sendWithTags(record, Set.of("user-events"));
                    System.out.println(
                            "[INTERACTIVE] Published event: " + event
                    );
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

        producer.close();
        System.out.println("User Service stopped.");
    }
}
