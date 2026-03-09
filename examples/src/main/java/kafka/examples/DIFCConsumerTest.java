package kafka.examples;

import org.apache.kafka.clients.Capability;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * End-to-end test program for DIFC APIs from a Kafka consumer.
 *
 * This program:
 *  1. Registers the consumer as a DIFC client
 *  2. Adds / removes tags
 *  3. Grants privileges (if authorized)
 *  4. Tests negative paths
 *  5. Consumes messages normally
 */
public class DIFCConsumerTest {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "deliverySvc");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "delivery-group");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(props)) {
            System.out.println("========================================");
            System.out.println(" DIFC CONSUMER API TEST");
            System.out.println(" ClientId = deliverySvc");
            System.out.println("========================================");

            // --------------------------------------------------
            // 1. CREATE NEW TAG
            // --------------------------------------------------
            System.out.println("\n=== 1. Register client ===");
            try {
                RegisterClientResponseData r = consumer.registerClient();
                System.out.println("RegisterClient -> " + r.errorMessage());
            } catch (Exception e) {
                System.out.println("RegisterClient skipped: " + e.getMessage());
            }

            // --------------------------------------------------
            // 2. ADD TAG
            // --------------------------------------------------
            System.out.println("\n=== 2. Add tag OUT_FOR_DELIVERY ===");
            try {
                AddTagResponseData r =
                        consumer.addTag("OUT_FOR_DELIVERY");
                System.out.println("AddTag -> " + r.errorMessage());
            } catch (Exception e) {
                System.out.println("AddTag failed: " + e.getMessage());
            }

            // --------------------------------------------------
            // 3. REMOVE TAG (may fail if unauthorized)
            // --------------------------------------------------
            System.out.println("\n=== 3. Remove tag OUT_FOR_DELIVERY ===");
            try {
                RemoveTagResponseData r =
                        consumer.removeTag("OUT_FOR_DELIVERY");
                System.out.println("RemoveTag -> " + r.errorMessage());
            } catch (Exception e) {
                System.out.println("RemoveTag failed (expected maybe): " + e.getMessage());
            }

            // --------------------------------------------------
            // 4. CREATE TAG (only if allowed)
            // --------------------------------------------------
            System.out.println("\n=== 4. Create tag TEMP_TEST_TAG ===");
            try {
                CreateTagResponseData r =
                        consumer.createTag("TEMP_TEST_TAG");
                System.out.println("CreateTag -> " + r.errorMessage()
                        + " (id=" + r.tagId() + ")");
            } catch (Exception e) {
                System.out.println("CreateTag failed: " + e.getMessage());
            }

            // --------------------------------------------------
            // 5. GRANT PRIVILEGES (only if this client owns tag)
            // --------------------------------------------------
            System.out.println("\n=== 5. Grant CAN_ADD on TEMP_TEST_TAG to supportSvc ===");
            try {
                AddClientPrivsResponseData r =
                        consumer.addClientPrivs(
                                "supportSvc",
                                "TEMP_TEST_TAG",
                                Capability.CAN_ADD   // CAN_ADD
                        );
                System.out.println("AddClientPrivs -> " + r.errorMessage());
            } catch (Exception e) {
                System.out.println("AddClientPrivs failed (expected maybe): " + e.getMessage());
            }

            // --------------------------------------------------
            // 6. OWNERSHIP TRANSFER (only if authorized)
            // --------------------------------------------------
            System.out.println("\n=== 6. Transfer ownership of TEMP_TEST_TAG to supportSvc ===");
            try {
                GrantOwnerPrivilegesResponseData r =
                        consumer.grantOwnerPrivs(
                                "supportSvc",
                                "TEMP_TEST_TAG"
                        );
                System.out.println("GrantOwnerPrivileges -> " + r.errorMessage());
            } catch (Exception e) {
                System.out.println("GrantOwnerPrivileges failed (expected maybe): " + e.getMessage());
            }

            // --------------------------------------------------
            // 7. NORMAL CONSUME TEST
            // --------------------------------------------------
            System.out.println("\n=== 7. Normal consume test ===");
            consumer.subscribe(Collections.singletonList("orders-topic"));

            for (int i = 0; i < 5; i++) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, String> r : records) {
                    System.out.printf("Consumed record: key=%s value=%s%n",
                            r.key(), r.value());
                }
            }

            System.out.println("\n========================================");
            System.out.println(" DIFC CONSUMER TEST COMPLETED");
            System.out.println("========================================");
        } catch (Exception e) {
            System.err.println("❌ TEST FAILED");
            e.printStackTrace();
        }
    }
}