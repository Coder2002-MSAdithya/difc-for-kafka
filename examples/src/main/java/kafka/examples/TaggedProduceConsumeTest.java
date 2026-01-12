package kafka.examples;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class TaggedProduceConsumeTest {

    private static final String TOPIC = "orders";

    public static void main(String[] args) throws Exception {

        // We’ll run producer and consumer in parallel
        CountDownLatch latch = new CountDownLatch(1);

        // ---- CONSUMER THREAD ----
        Thread consumerThread = new Thread(() -> {
            try {
                runConsumer(latch);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // ---- PRODUCER THREAD ----
        Thread producerThread = new Thread(() -> {
            try {
                runProducer();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        consumerThread.start();

        // small delay so consumer subscribes before producing
        Thread.sleep(1500);

        producerThread.start();

        // wait until consumer sees a record
        latch.await();

        System.out.println("\nTest finished.");
        System.exit(0);
    }

    // ----------------------------------------------------------------------
    // PRODUCER
    // ----------------------------------------------------------------------
    private static void runProducer() {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // IMPORTANT: must be a client registered in TagRegistrar.initialize()
        // Try different ones: customerApp, restaurantSvc, deliverySvc, paymentSvc, supportSvc, auditSvc
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "supportSvc");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC, "order-1", "Order created");

            System.out.println("Producer started with client.id=supportSvc");
            System.out.println("Producer sending record with tags .. ");

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                } else {
                    System.out.printf(
                            "Producer sent to %s-%d @ offset %d%n",
                            metadata.topic(),
                            metadata.partition(),
                            metadata.offset()
                    );
                }
            });

            producer.flush();
        }
    }

    // ----------------------------------------------------------------------
    // CONSUMER
    // ----------------------------------------------------------------------
    private static void runConsumer(CountDownLatch latch) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // IMPORTANT: this is the RECEIVING client id for DIFC checks
        // Try switching this to:
        //   - deliverySvc   (likely unauthorized for ORDER_PLACED + FOOD_PREPARED)
        //   - paymentSvc
        //   - auditSvc
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "paymentSvc");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "difc-test-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

            consumer.subscribe(Collections.singleton(TOPIC));

            System.out.println("Consumer started with client.id=paymentSvc");

            while (true) {

//                consumer.sendAddTagRequest("REFUND_ISSUED");

                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofSeconds(1));

                if (records.isEmpty())
                    continue;

                for (ConsumerRecord<String, String> r : records) {

                    System.out.println("\n--- Consumer received record ---");
                    System.out.println("offset = " + r.offset());
                    System.out.println("key    = " + r.key());
                    System.out.println("value  = " + r.value());

                    boolean hasMissingTags = false;

                    for (Header h : r.headers()) {
                        if ("missing_tags".equals(h.key())) {
                            hasMissingTags = true;
                            String v = new String(h.value(), StandardCharsets.UTF_8);
                            System.out.println("missing_tags = " + v);
                        }
                        if ("tags".equals(h.key())) {
                            String v = new String(h.value(), StandardCharsets.UTF_8);
                            System.out.println("tags = " + v);
                        }
                    }

                    if (r.value() == null) {
                        System.out.println(">>> RECORD MASKED by broker");
                        if (!hasMissingTags) {
                            System.out.println(">>> (but no missing_tags header present)");
                        }
                    } else {
                        System.out.println(">>> RECORD DELIVERED normally");
                    }

                    latch.countDown();
                    return;
                }
            }
        }
    }
}

