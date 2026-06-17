package org.apache.kafka.security.agent.policy;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Demo-specific expected JDBC/external endpoints for grant-time {@code CAN_ADD} checks. */
public final class ExpectedExternalConnectionsRegistry {

  private static final Map<String, Set<String>> DEFAULTS = buildDefaults();

  private ExpectedExternalConnectionsRegistry() {
  }

  public static Set<String> defaultsForPrincipal(final String principal) {
    if (principal == null) {
      return Set.of();
    }
    return DEFAULTS.getOrDefault(principal, Set.of());
  }

  private static Map<String, Set<String>> buildDefaults() {
    final Map<String, Set<String>> defaults = new LinkedHashMap<>();
    // Spring Boot demo MySQL (when not using in-memory H2 override).
    defaults.put(
        "ms-orders-svc",
        Set.of("127.0.0.1:33066", "localhost:33066"));
    defaults.put(
        "ms-payment-svc",
        Set.of("127.0.0.1:33066", "localhost:33066"));
    defaults.put(
        "ms-stock-svc",
        Set.of("127.0.0.1:33066", "localhost:33066"));
    return Collections.unmodifiableMap(defaults);
  }
}
