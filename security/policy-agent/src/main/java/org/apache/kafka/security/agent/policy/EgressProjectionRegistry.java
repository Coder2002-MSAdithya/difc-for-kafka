package org.apache.kafka.security.agent.policy;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Static egress projections inferred from mapValues lambdas before runtime produce. */
public final class EgressProjectionRegistry {

  private static final Map<String, Set<String>> PROJECTIONS = new ConcurrentHashMap<>();

  private EgressProjectionRegistry() {
  }

  public static void register(final String egressTopic, final Set<String> outputFields) {
    if (egressTopic == null || egressTopic.isEmpty() || outputFields == null || outputFields.isEmpty()) {
      return;
    }
    PROJECTIONS.put(egressTopic, Set.copyOf(outputFields));
    PolicyRefreshScheduler.scheduleRefresh();
  }

  public static Set<String> projectionForEgress(final String egressTopic) {
    if (egressTopic == null) {
      return Set.of();
    }
    return PROJECTIONS.getOrDefault(egressTopic, Set.of());
  }

  public static void clearForTests() {
    PROJECTIONS.clear();
  }
}
