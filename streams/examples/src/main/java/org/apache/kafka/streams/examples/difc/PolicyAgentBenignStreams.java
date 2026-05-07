package org.apache.kafka.streams.examples.difc;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public final class PolicyAgentBenignStreams {

    private PolicyAgentBenignStreams() {
    }

    public static void main(String[] args) {

        String bootstrap =
                args.length > 0 ? args[0] : "localhost:9092";

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> input =
                builder.stream("input-topic");

        input
                .mapValues(v -> v.toUpperCase())
                .filter((k, v) -> v.length() > 3)
                .to("output-topic");

        Properties props = new Properties();

        props.put(
                StreamsConfig.APPLICATION_ID_CONFIG,
                "policy-agent-benign-streams"
        );

        props.put(
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrap
        );

        props.put(
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass()
        );

        props.put(
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass()
        );

        KafkaStreams streams =
                new KafkaStreams(builder.build(), props);

        streams.start();

        System.out.println(
                "Benign Kafka Streams application started."
        );

        Runtime.getRuntime().addShutdownHook(
                new Thread(streams::close)
        );
    }
}
