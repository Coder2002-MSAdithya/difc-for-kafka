package org.apache.kafka.streams.examples.difc;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public final class PolicyAgentMixedClient {

    private PolicyAgentMixedClient() {
    }

    public static void main(String[] args) {

        Properties p = new Properties();

        p.put(
                "bootstrap.servers",
                "localhost:9092"
        );

        p.put(
                "key.serializer",
                StringSerializer.class.getName()
        );

        p.put(
                "value.serializer",
                StringSerializer.class.getName()
        );

        // First logical client
        new KafkaProducer<String, String>(p);

        // Streams config
        Properties streamsProps = new Properties();

        streamsProps.put(
                StreamsConfig.APPLICATION_ID_CONFIG,
                "mixed-client-test"
        );

        streamsProps.put(
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream("input-topic")
                .to("output-topic");

        KafkaStreams streams =
                new KafkaStreams(
                        builder.build(),
                        streamsProps
                );

        streams.start();
    }
}