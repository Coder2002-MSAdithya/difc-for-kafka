package org.apache.kafka.clients.producer;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class DifcKafkaProducer<K, V> extends KafkaProducer<K, V>
{
    private static final String APPLICATION_ID_CONFIG = "application.id";
    private String appId = APPLICATION_ID_CONFIG;

    public DifcKafkaProducer(final Map<String, Object> configs) {
        super(configs, null, null);
        this.appId = appIdFrom(configs);
    }

    public DifcKafkaProducer(final Map<String, Object> configs,
                             final Serializer<K> keySerializer,
                             final Serializer<V> valueSerializer)
    {
        super(configs, keySerializer, valueSerializer);
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
}