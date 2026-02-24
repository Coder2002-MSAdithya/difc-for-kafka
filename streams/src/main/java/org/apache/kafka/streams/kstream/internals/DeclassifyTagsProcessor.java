package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import java.util.Set;

public class DeclassifyTagsProcessor<K, V> implements Processor<K, V, K, V>
{
    private final Set<String> tags;
    private ProcessorContext<K, V> context;

    public DeclassifyTagsProcessor(final Set<String> tags)
    {
        this.tags = tags;
    }

    @Override
    public void init(final ProcessorContext<K, V> context) {
        this.context = context;
    }

    public void process(final Record<K, V> record)
    {
        // forward record with tags to declassify added
        context.forward(record.declassifyTags(tags));
    }
}
