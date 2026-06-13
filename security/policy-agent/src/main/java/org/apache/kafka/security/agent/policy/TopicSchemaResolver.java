package org.apache.kafka.security.agent.policy;

import java.util.LinkedHashSet;
import java.util.Set;

/** Resolves topic field names from runtime observation with static-catalog fallback. */
public final class TopicSchemaResolver {

  private TopicSchemaResolver() {
  }

  public static Set<String> valueFieldsForTopic(final String topic) {
    final Set<String> observed = TopicFieldRegistry.fieldsForTopic(topic);
    if (isUsefulFieldObservation(observed)) {
      return new LinkedHashSet<>(observed);
    }
    return TopicSchemaCatalog.copyFields(TopicSchemaCatalog.valueFieldsForTopic(topic));
  }

  static boolean isUsefulFieldObservation(final Set<String> fields) {
    if (fields == null || fields.isEmpty()) {
      return false;
    }
    final Set<String> meaningful = new LinkedHashSet<>(fields);
    meaningful.removeAll(Set.of("class", "schema", "specificData", "customEncoder", "customDecoder"));
    return !meaningful.isEmpty();
  }

  public static Set<String> sensitiveOrderFields() {
    return TopicSchemaCatalog.sensitiveOrderFields();
  }
}
