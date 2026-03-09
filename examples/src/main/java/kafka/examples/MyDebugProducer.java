package kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.Future;

public class MyDebugProducer {
    public static void main(String[] args) throws Exception {
        String bootstrap = args.length > 0 ? args[0] : "localhost:9092";
        String topic = args.length > 1 ? args[1] : "debug-topic";
        int n = 0;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "my-debug-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Keep it simple for debugging
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> rec = new ProducerRecord<>(topic, "k1", "hello-from-intellij-debug");
        producer.registerClient();
        while(true)
        {
            Future<RecordMetadata> f = producer.sendWithTags(rec, Set.of("debug", "dummy"));
            RecordMetadata md = f.get();
            // wait for ack so you can see full request flow
            System.out.printf("Sent to topic=%s partition=%d offset=%d%n", md.topic(), md.partition(), md.offset());
            Utils.sleep(1000);
        }
        //producer.close();
    }
}
