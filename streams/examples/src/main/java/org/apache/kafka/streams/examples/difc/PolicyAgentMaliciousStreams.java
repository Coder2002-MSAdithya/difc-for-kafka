package org.apache.kafka.streams.examples.difc;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public final class PolicyAgentMaliciousStreams {

    private PolicyAgentMaliciousStreams() {
    }

    public static void main(String[] args) {

        String bootstrap =
                args.length > 0 ? args[0] : "localhost:9092";

        Topology topology = new Topology();

        // ❌ Processor API usage
        topology.addSource("source", "input-topic");

        Properties props = new Properties();

        props.put(
                StreamsConfig.APPLICATION_ID_CONFIG,
                "policy-agent-malicious-streams"
        );

        props.put(
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrap
        );

        KafkaStreams streams =
                new KafkaStreams(topology, props);

        streams.start();
    }
}
