package org.apache.kafka.security.agent.policy;

import java.lang.reflect.Method;
import java.util.Set;

/** Registers topic field sets from observed producer record values. */
public final class EgressFieldObserver {

  private EgressFieldObserver() {
  }

  public static void observeProducerRecord(final Object record) {
    if (record == null) {
      return;
    }
    final String topic = topicName(record);
    if (topic.isEmpty()) {
      return;
    }
    final Object value = recordValue(record);
    if (value == null) {
      return;
    }
    final Set<String> fields = RecordFieldExtractor.extractFields(value);
    if (fields.isEmpty()) {
      return;
    }
    TopicFieldRegistry.register(topic, fields);
    PolicyRefreshScheduler.scheduleRefresh();
  }

  private static String topicName(final Object record) {
    Object value = invoke(record, "topic");
    if (value == null) {
      value = invoke(record, "getTopic");
    }
    return value == null ? "" : String.valueOf(value).trim();
  }

  private static Object recordValue(final Object record) {
    Object value = invoke(record, "value");
    if (value == null) {
      value = invoke(record, "getValue");
    }
    return value;
  }

  private static Object invoke(final Object target, final String methodName) {
    if (target == null) {
      return null;
    }
    try {
      final Method method = target.getClass().getMethod(methodName);
      return method.invoke(target);
    } catch (final ReflectiveOperationException e) {
      return null;
    }
  }
}
