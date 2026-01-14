package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.nio.charset.StandardCharsets;

public final class AddTagsProcessor<K, V>
        implements Processor<K, V, K, V> {

    private final String tagValue;
    private ProcessorContext<K, V> context;

    public AddTagsProcessor(final String tagValue) {
        this.tagValue = tagValue;
    }

    @Override
    public void init(final ProcessorContext<K, V> context) {
        this.context = context;
    }

    @Override
    public void process(final Record<K, V> record) {
        Headers headers = record.headers();

        // APPEND semantics
        headers.add(
                "tags",
                tagValue.getBytes(StandardCharsets.UTF_8)
        );

        // forward unchanged record
        context.forward(record);
    }

    @Override
    public void close() {}
}
