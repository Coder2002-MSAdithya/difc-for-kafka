package org.apache.kafka.security.agent.policy;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Runtime topic → field-name registry populated from observed records and projections. */
public final class TopicFieldRegistry {

  private static final Map<String, Set<String>> TOPIC_FIELDS = new ConcurrentHashMap<>();

  private TopicFieldRegistry() {
  }

  public static void register(final String topic, final Set<String> fields) {
    if (topic == null || topic.isEmpty() || fields == null || fields.isEmpty()) {
      return;
    }
    TOPIC_FIELDS.put(topic, Collections.unmodifiableSet(new LinkedHashSet<>(fields)));
  }

  public static Set<String> fieldsForTopic(final String topic) {
    if (topic == null) {
      return Set.of();
    }
    return TOPIC_FIELDS.getOrDefault(topic, Set.of());
  }

  public static void clearForTests() {
    TOPIC_FIELDS.clear();
  }
}
