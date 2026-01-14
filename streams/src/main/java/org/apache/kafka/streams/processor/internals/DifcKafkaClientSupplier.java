package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.DifcKafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.DifcKafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.internals.DifcStreamsRuntime;

import java.util.Map;

public final class DifcKafkaClientSupplier implements KafkaClientSupplier {

    @Override
    public Producer<byte[], byte[]> getProducer(Map<String, Object> config) {
        enforceClientId(config);
        DifcKafkaProducer<byte[], byte[]> p = new DifcKafkaProducer<>(config);
        DifcStreamsRuntime.registerProducer(p);
        return p;
    }

    @Override
    public Consumer<byte[], byte[]> getConsumer(Map<String, Object> config) {
        enforceClientId(config);
        DifcKafkaConsumer<byte[], byte[]> c = new DifcKafkaConsumer<>(config);
        DifcStreamsRuntime.registerConsumer(c);
        return c;
    }

    @Override
    public Consumer<byte[], byte[]> getRestoreConsumer(Map<String, Object> config) {
        return new DifcKafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @Override
    public Consumer<byte[], byte[]> getGlobalConsumer(Map<String, Object> config) {
        return new DifcKafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @Override
    public Admin getAdmin(Map<String, Object> config) {
        enforceClientId(config);
        return Admin.create(config);
    }

    /**
     * 🔴 The ONLY place where identity is enforced.
     * Forces: client.id = application.id
     */
    private void enforceClientId(Map<String, Object> config) {
        Object appId = config.get(StreamsConfig.APPLICATION_ID_CONFIG);
        if (appId != null) {
            config.put(CommonClientConfigs.CLIENT_ID_CONFIG, appId.toString());
        }
    }
}
