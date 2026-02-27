package kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Future;

public class MyDebugProducer {
    public static void main(String[] args) throws Exception {
        String bootstrap = args.length > 0 ? args[0] : "localhost:9092";
        String topic = args.length > 1 ? args[1] : "debug-topic";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "my-debug-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Keep it simple for debugging
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "0");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> rec =
                    new ProducerRecord<>(topic, "k1", "hello-from-intellij-debug");

            producer.registerClient("debug-client");
            Future<RecordMetadata> f = producer.send(rec);
            RecordMetadata md = f.get(); // wait for ack so you can see full request flow
            System.out.printf("Sent to topic=%s partition=%d offset=%d%n",
                    md.topic(), md.partition(), md.offset());
        }

        // Optional helper if you want to print Kafka-only stack from your own code
        Arrays.stream(Thread.currentThread().getStackTrace())
                .map(StackTraceElement::toString)
                .filter(s -> s.startsWith("org.apache.kafka."))
                .forEach(System.out::println);
    }
}
