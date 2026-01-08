package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class DifcKafkaConsumer<K, V> extends KafkaConsumer<K, V> {
    public DifcKafkaConsumer(Map<String, Object> configs) {
        super(configs);
    }

    public DifcKafkaConsumer(Map<String, Object> configs, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer)
    {
        super(configs, keyDeserializer, valueDeserializer);
    }
    // your existing DIFC APIs:
    // sendCreateTagRequest(...)
    // sendRegisterClientRequest(...)
    // etc.
}
