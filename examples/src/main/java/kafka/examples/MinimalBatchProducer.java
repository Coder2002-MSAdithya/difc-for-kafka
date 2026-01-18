package kafka.examples;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class MinimalBatchProducer {

    public static void main(String[] args) throws Exception {

        // 1. Configure producer with batching
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        // BATCHING SETTINGS - These enable automatic batching
        props.put("linger.ms", 500);      // Wait up to 500ms for more records
        props.put("batch.size", 1);   // 32KB batch size

        // 2. Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 3. Send records - Kafka will batch them automatically
        for (int i = 1; i <= 50; i++) {
            String key = "key-" + (i % 5);  // 5 different keys
            String value = "Message " + i + " at " + System.currentTimeMillis();

            producer.send(new ProducerRecord<>("test-topic", key, value));
            System.out.println("Sent: " + value);

            Thread.sleep(100);  // Wait between sends
        }

        // 4. Close (automatically flushes)
        producer.close();
        System.out.println("Done! Kafka handled batching automatically.");
    }
}