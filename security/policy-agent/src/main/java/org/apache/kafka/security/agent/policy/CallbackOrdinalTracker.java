package org.apache.kafka.security.agent.policy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Tracks per-capturing-class callback ordinals for method-reference alignment. */
final class CallbackOrdinalTracker {

  private static final Map<Integer, Integer> MAPPER_ORDINALS = new ConcurrentHashMap<>();
  private static final Map<String, AtomicInteger> NEXT_ORDINAL = new ConcurrentHashMap<>();

  private CallbackOrdinalTracker() {
  }

  static void resetForTests() {
    MAPPER_ORDINALS.clear();
    NEXT_ORDINAL.clear();
  }

  static Map<Integer, Integer> ordinalsFor(final String capturingClassName) {
    return MAPPER_ORDINALS;
  }

  static int nextOrdinal(final String capturingClassName) {
    return NEXT_ORDINAL.computeIfAbsent(capturingClassName, key -> new AtomicInteger()).getAndIncrement();
  }
}
