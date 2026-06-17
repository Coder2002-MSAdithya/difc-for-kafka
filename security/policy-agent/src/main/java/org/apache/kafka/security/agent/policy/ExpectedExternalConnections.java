package org.apache.kafka.security.agent.policy;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Grant-time expected non-Kafka endpoints per requester principal.
 * Configure with {@code -Dpolicy.grantor.expected.external.hosts.<principal>=host:port[,host:port]}.
 */
public final class ExpectedExternalConnections {

  public static final String EXPECTED_PREFIX = "policy.grantor.expected.external.hosts.";

  private ExpectedExternalConnections() {
  }

  public static Set<String> forPrincipal(final String principal) {
    if (principal == null || principal.isEmpty()) {
      return Set.of();
    }
    final Set<String> merged = new LinkedHashSet<>();
    merged.addAll(fromSystemProperty(principal));
    merged.addAll(ExpectedExternalConnectionsRegistry.defaultsForPrincipal(principal));
    return Collections.unmodifiableSet(merged);
  }

  private static Set<String> fromSystemProperty(final String principal) {
    final String raw = System.getProperty(EXPECTED_PREFIX + principal, "").trim();
    if (raw.isEmpty()) {
      return Set.of();
    }
    final Set<String> endpoints = new LinkedHashSet<>();
    for (final String part : raw.split(",")) {
      final String normalized = ExternalConnectionAllowlist.normalizeEndpoint(part);
      if (!normalized.isEmpty()) {
        endpoints.add(normalized);
      }
    }
    return endpoints;
  }
}
