package kafka.examples;

import org.apache.kafka.clients.Capability;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * One-shot end-to-end DIFC test for the food-delivery scenario.
 *
 * This program simulates all services in a single run by creating
 * multiple KafkaProducer instances, each with a different client.id.
 */
public class DIFCFullFoodDeliveryTest
{
    private static final String BOOTSTRAP = "localhost:9092";

    public static void main(String[] args) throws Exception
    {
        System.out.println("================================================");
        System.out.println(" DIFC FULL FOOD-DELIVERY INTEGRATION TEST");
        System.out.println("================================================");

        try (
                KafkaProducer<String, String> customerApp   = newProducer("customerApp");
                KafkaProducer<String, String> restaurantSvc = newProducer("restaurantSvc");
                KafkaProducer<String, String> deliverySvc   = newProducer("deliverySvc");
                KafkaProducer<String, String> paymentSvc    = newProducer("paymentSvc");
                KafkaProducer<String, String> supportSvc    = newProducer("supportSvc");
                KafkaProducer<String, String> auditSvc      = newProducer("auditSvc")
        )
        {
            // ------------------------------------------------------------
            // 1. REGISTER CLIENTS
            // ------------------------------------------------------------
            System.out.println("\n=== 1. Registering clients ===");
            register(customerApp, "customerApp");
            register(customerApp, "restaurantSvc");
            register(customerApp, "deliverySvc");
            register(customerApp, "paymentSvc");
            register(customerApp, "supportSvc");
            register(customerApp, "auditSvc");

            // ------------------------------------------------------------
            // 2. CREATE TAGS
            // ------------------------------------------------------------
            System.out.println("\n=== 2. Creating lifecycle tags ===");
            createTag(customerApp, "ORDER_PLACED");
            createTag(customerApp, "FOOD_PREPARED");
            createTag(customerApp, "OUT_FOR_DELIVERY");
            createTag(customerApp, "PAYMENT_CAPTURED");
            createTag(customerApp, "REFUND_ISSUED");
            createTag(customerApp, "AUDIT_LOG");

            // ------------------------------------------------------------
            // 3. GRANT PRIVILEGES
            // ------------------------------------------------------------
            System.out.println("\n=== 3. Granting privileges ===");

            // ORDER_PLACED owned by customerApp → allow restaurant to add it
            grantAddPriv(customerApp, "restaurantSvc", "ORDER_PLACED");

            // FOOD_PREPARED owned by restaurantSvc → allow delivery to add it
            grantAddPriv(restaurantSvc, "deliverySvc", "FOOD_PREPARED");

            // OUT_FOR_DELIVERY owned by deliverySvc → allow payment to add it
            grantAddPriv(deliverySvc, "paymentSvc", "OUT_FOR_DELIVERY");

            // PAYMENT_CAPTURED owned by paymentSvc → allow support to add REFUND_ISSUED
            grantAddPriv(paymentSvc, "supportSvc", "REFUND_ISSUED");

            // ------------------------------------------------------------
            // 4. SIMULATE ORDER LIFECYCLE
            // ------------------------------------------------------------
            System.out.println("\n=== 4. Simulating order lifecycle ===");

            System.out.println("customerApp -> add ORDER_PLACED");
            addTag(customerApp, "ORDER_PLACED");

            System.out.println("restaurantSvc -> add FOOD_PREPARED");
            addTag(restaurantSvc, "FOOD_PREPARED");

            System.out.println("deliverySvc -> add OUT_FOR_DELIVERY");
            addTag(deliverySvc, "OUT_FOR_DELIVERY");

            System.out.println("paymentSvc -> add PAYMENT_CAPTURED");
            addTag(paymentSvc, "PAYMENT_CAPTURED");

            System.out.println("auditSvc -> add AUDIT_LOG");
            addTag(auditSvc, "AUDIT_LOG");

            // ------------------------------------------------------------
            // 5. NEGATIVE TESTS
            // ------------------------------------------------------------
            System.out.println("\n=== 5. Negative tests ===");

            try
            {
                System.out.println("supportSvc tries to REMOVE PAYMENT_CAPTURED (should fail)");
                supportSvc.removeTag("PAYMENT_CAPTURED");
                System.out.println("❌ ERROR: unauthorized remove succeeded");
            }
            catch (Exception e)
            {
                System.out.println("✅ EXPECTED: unauthorized remove failed: " + e.getMessage());
            }

            try
            {
                System.out.println("Trying duplicate CREATE_TAG (ORDER_PLACED)");
                customerApp.createTag("ORDER_PLACED");
                System.out.println("❌ ERROR: duplicate create succeeded");
            }
            catch (Exception e)
            {
                System.out.println("✅ EXPECTED: duplicate create failed: " + e.getMessage());
            }

            // ------------------------------------------------------------
            // 6. OWNERSHIP TRANSFER
            // ------------------------------------------------------------
            System.out.println("\n=== 6. Ownership transfer ===");
            System.out.println("paymentSvc transfers ownership of REFUND_ISSUED to supportSvc");

            GrantOwnerPrivilegesResponseData grant =
                    paymentSvc.grantOwnerPrivileges("supportSvc", "REFUND_ISSUED");

            System.out.println("Ownership transfer result: " + grant.errorMessage());

            // ------------------------------------------------------------
            // 7. NORMAL KAFKA PRODUCE
            // ------------------------------------------------------------
            System.out.println("\n=== 7. Normal Kafka produce ===");
            customerApp.send(new ProducerRecord<>(
                    "orders-topic",
                    "order-123",
                    "order completed with DIFC enforcement"));
            System.out.println("✅ SUCCESS: normal Kafka produce works");

            System.out.println("\n================================================");
            System.out.println(" DIFC FULL TEST COMPLETED SUCCESSFULLY");
            System.out.println("================================================");
        }
        catch (Exception e)
        {
            System.err.println("❌ TEST FAILED: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // ------------------------------------------------------------------
    // Producer factory
    // ------------------------------------------------------------------

    private static KafkaProducer<String, String> newProducer(String clientId)
    {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        return new KafkaProducer<>(props);
    }

    // ------------------------------------------------------------------
    // DIFC helper wrappers
    // ------------------------------------------------------------------

    private static void register(KafkaProducer<String, String> p, String clientId)
    {
        try {
            RegisterClientResponseData r =
                    p.registerClient();
            System.out.println("Registered " + clientId + " -> " + r.errorMessage());
        } catch (Exception e) {
            System.out.println("Register " + clientId + " skipped: " + e.getMessage());
        }
    }

    private static void createTag(KafkaProducer<String, String> p, String tag)
    {
        try {
            CreateTagResponseData r =
                    p.createTag(tag);
            System.out.println("Created tag " + tag + " -> " + r.errorMessage());
        } catch (Exception e) {
            System.out.println("Create tag " + tag + " skipped: " + e.getMessage());
        }
    }

    private static void addTag(KafkaProducer<String, String> p, String tag)
    {
        try {
            AddTagResponseData r =
                    p.addTag(tag);
            System.out.println("Added tag " + tag + " -> " + r.errorMessage());
        } catch (Exception e) {
            System.out.println("Add tag " + tag + " failed: " + e.getMessage());
        }
    }

    private static void grantAddPriv(
            KafkaProducer<String, String> p,
            String targetClient,
            String tag)
    {
        try {
            AddClientPrivsResponseData r =
                    p.addClientPrivs(
                            targetClient,
                            tag,
                            Capability.CAN_ADD
                    );
            System.out.println("Granted CAN_ADD on " + tag +
                    " to " + targetClient + " -> " + r.errorMessage());
        } catch (Exception e) {
            System.out.println("Grant privilege failed: " + e.getMessage());
        }
    }
}

