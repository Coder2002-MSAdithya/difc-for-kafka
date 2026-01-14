package org.apache.kafka.streams.internals;

import org.apache.kafka.clients.consumer.DifcKafkaConsumer;
import org.apache.kafka.clients.producer.DifcKafkaProducer;
import org.apache.kafka.streams.difc.DifcSyncFacade;

public final class DifcStreamsRuntime {

    private static volatile DifcKafkaProducer<?, ?> producer;
    private static volatile DifcKafkaConsumer<?, ?> consumer;
    private static volatile DifcSyncFacade facade;

    private DifcStreamsRuntime() {}

    public static void registerProducer(DifcKafkaProducer<?, ?> p) {
        producer = p;
        tryInit();
    }

    public static void registerConsumer(DifcKafkaConsumer<?, ?> c) {
        consumer = c;
        tryInit();
    }

    private static synchronized void tryInit() {
        if (producer != null && consumer != null && facade == null) {
            facade = new DifcSyncFacade(producer);
        }
    }

    public static DifcSyncFacade difc() {
        if (facade == null) {
            throw new IllegalStateException(
                    "DIFC runtime not initialized yet. " +
                            "Make sure Streams uses DifcKafkaClientSupplier.");
        }
        return facade;
    }
}

