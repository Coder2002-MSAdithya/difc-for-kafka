package kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Properties;

/**
 * Benign test app for the policy agent.
 *
 * Expected behavior (with -javaagent):
 * - Producer network activity is allowed because socket connects occur through trusted Kafka jars.
 */
public final class PolicyAgentBenignProducer {
    private PolicyAgentBenignProducer() {
    }

    public static void main(String[] args) {
        String bootstrap = args.length > 0 ? args[0] : "localhost:9092";
        String topic = args.length > 1 ? args[1] : "policy-agent-topic";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "policy-agent-benign-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    topic,
                    "policy-agent-key",
                    "hello-from-benign-producer"
            );
            producer.send(record).get();
            producer.flush();
            System.out.println("Benign producer send completed.");
        } catch (Exception e) {
            System.err.println("Benign producer encountered error: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }
}