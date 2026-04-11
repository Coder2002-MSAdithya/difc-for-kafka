package kafka.examples;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

public class TaggedProducerTest
{

    public static void main(String[] args) throws Exception
    {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        String topic = "difc-test";

        System.out.println(producer.registerClient().errorMessage());
        System.out.println(producer.createTag("secret").errorMessage());
        System.out.println(producer.createTag("pii").errorMessage());
        System.out.println(producer.createTag("internal").errorMessage());
        System.out.println(producer.createTag("finance").errorMessage());
        System.out.println(producer.getOwnedTags().ownedTags());

//        System.out.println(producer.destroyTag("finance").errorCode());

        // ---- messages with tags ----
        producer.sendWithTags(
                new ProducerRecord<>(topic, "k1", "message-1"),
                Set.of("secret", "pii"),
                (md, ex) -> System.out.println("sent message-1")
        );

        producer.sendWithTags(
                new ProducerRecord<>(topic, "k2", "message-2"),
                Set.of("finance", "internal"),
                (md, ex) -> System.out.println("sent message-2")
        );

        // ---- message WITHOUT tags ----
        producer.sendWithTags(
                new ProducerRecord<>(topic, "k3", "message-3"),
                Collections.emptySet(),
                (md, ex) -> System.out.println("sent message-3")
        );

        producer.flush();
        producer.close();
    }
}
