package org.apache.kafka.streams.internals.metrics;

import org.apache.kafka.clients.consumer.DifcKafkaConsumer;
import org.apache.kafka.clients.producer.DifcKafkaProducer;

public final class DifcStreamsRuntime
{

    private static volatile DifcKafkaProducer<?, ?> producer;
    private static volatile DifcKafkaConsumer<?, ?> consumer;

    private DifcStreamsRuntime() {}

    public static void registerProducer(DifcKafkaProducer<?, ?> p) {
        producer = p;
    }

    public static void registerConsumer(DifcKafkaConsumer<?, ?> c) {
        consumer = c;
    }

    public static DifcKafkaProducer<?, ?> producer() {
        return producer;
    }

    public static DifcKafkaConsumer<?, ?> consumer() {
        return consumer;
    }
}

