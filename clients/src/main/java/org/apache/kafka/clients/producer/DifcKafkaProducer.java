package org.apache.kafka.clients.producer;

import java.util.Map;

public class DifcKafkaProducer<K, V> extends KafkaProducer<K, V> {

    public DifcKafkaProducer(Map<String, Object> configs) {
        super(configs);
    }
}
