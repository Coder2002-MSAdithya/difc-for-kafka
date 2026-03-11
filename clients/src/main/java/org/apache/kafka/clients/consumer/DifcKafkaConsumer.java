package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class DifcKafkaConsumer<K, V> extends KafkaConsumer<K, V>
{
    private static final String APPLICATION_ID_CONFIG = "application.id";
    private final String appId;

    public DifcKafkaConsumer(final Map<String, Object> configs)
    {
        super(configs);
        this.appId = appIdFrom(configs);
    }

    public DifcKafkaConsumer(Map<String, Object> configs, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer)
    {
        super(configs, keyDeserializer, valueDeserializer);
        this.appId = appIdFrom(configs);
    }

    public String getAppId()
    {
        return appId;
    }

    private String appIdFrom(final Map<String, Object> configs)
    {
        final Object value = configs.get(APPLICATION_ID_CONFIG);
        return value == null ? null : value.toString();
    }
    // your existing DIFC APIs:
    // createTag(...)
    // registerClient(...)
    // etc.
}
