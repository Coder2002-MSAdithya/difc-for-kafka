package org.apache.kafka.security.agent.policy;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Records non-Kafka socket connects observed at runtime for attestation and grant-time checks. */
public final class ExternalConnectionTracker {

  private static final Object LOCK = new Object();
  private static final Set<String> OBSERVED = new LinkedHashSet<>();

  private ExternalConnectionTracker() {
  }

  public static void clearForTests() {
    synchronized (LOCK) {
      OBSERVED.clear();
    }
  }

  public static void recordConnect(final InetSocketAddress address) {
    if (address == null || address.isUnresolved()) {
      return;
    }
    if (org.apache.kafka.security.agent.bootstrap.internal.SocketPolicyBootstrap.isTrustedKafkaConnectStack()) {
      return;
    }
    if (isKafkaBootstrapEndpoint(address)) {
      return;
    }
    final String endpoint = ExternalConnectionAllowlist.normalizeEndpoint(address);
    if (endpoint.isEmpty()) {
      return;
    }
    synchronized (LOCK) {
      if (OBSERVED.add(endpoint)) {
        System.out.printf(
            "[POLICY][NETWORK] externalConnect endpoint=%s allowed=%s%n",
            endpoint,
            ExternalConnectionAllowlist.isAllowed(address));
        PolicyRefreshScheduler.scheduleRefresh();
      }
    }
  }

  public static Set<String> observedEndpoints() {
    synchronized (LOCK) {
      return Set.copyOf(OBSERVED);
    }
  }

  public static void mergeInto(final AppProcessingPolicy policy) {
    if (policy == null) {
      return;
    }
    final List<AppProcessingPolicy.ExternalConnection> connections = new ArrayList<>();
    final Set<String> allowed = ExternalConnectionAllowlist.configuredEndpoints();
    for (final String endpoint : observedEndpoints()) {
      final AppProcessingPolicy.ExternalConnection connection =
          new AppProcessingPolicy.ExternalConnection();
      connection.setEndpoint(endpoint);
      connection.setAllowed(allowed.contains(endpoint));
      connections.add(connection);
    }
    policy.setExternalConnections(connections);
  }

  private static boolean isKafkaBootstrapEndpoint(final InetSocketAddress address) {
    final String bootstrap = System.getProperty("policy.agent.kafka.bootstrap", "");
    final String effective =
        bootstrap.isBlank()
            ? System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "")
            : bootstrap;
    if (effective.isBlank()) {
      return false;
    }
    final String normalized = ExternalConnectionAllowlist.normalizeEndpoint(address);
    for (final String part : effective.split(",")) {
      if (ExternalConnectionAllowlist.normalizeEndpoint(part).equals(normalized)) {
        return true;
      }
    }
    return false;
  }
}
