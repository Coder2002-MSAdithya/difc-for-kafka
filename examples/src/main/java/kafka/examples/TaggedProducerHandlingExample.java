package kafka.examples;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Set;

public class TaggedProducerHandlingExample {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // IMPORTANT: must match a client registered in TagRegistrar.initialize()
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "restaurantSvc");
        // try also: restaurantSvc, deliverySvc, paymentSvc, supportSvc, auditSvc

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        String topic = "orders";

        ProducerRecord<String, String> record =
                new ProducerRecord<>(topic, "order-1", "Order created");

        System.out.println("Sending record with header tags : ");

        producer.sendWithTags(record, Set.of("ORDER_PLACED", "FAKE_TAG", "IGNORE_ME", "AUDIT_LOG"), (metadata, exception) -> {
            if (exception != null) {
                exception.printStackTrace();
            } else {
                System.out.printf(
                        "Sent to %s-%d @ offset %d%n",
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset()
                );
            }
        });

        producer.flush();
        producer.close();
    }
}

