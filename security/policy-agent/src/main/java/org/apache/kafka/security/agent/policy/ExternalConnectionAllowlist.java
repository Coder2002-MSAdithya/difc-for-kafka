package org.apache.kafka.security.agent.policy;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

/** Parses {@code -Dpolicy.agent.allowed.external.hosts=host:port[,host:port...]}. */
public final class ExternalConnectionAllowlist {

  public static final String ALLOWED_EXTERNAL_HOSTS_PROP = "policy.agent.allowed.external.hosts";

  private ExternalConnectionAllowlist() {
  }

  public static Set<String> configuredEndpoints() {
    final String raw = System.getProperty(ALLOWED_EXTERNAL_HOSTS_PROP, "").trim();
    if (raw.isEmpty()) {
      return Set.of();
    }
    final Set<String> endpoints = new LinkedHashSet<>();
    for (final String part : raw.split(",")) {
      final String normalized = normalizeEndpoint(part);
      if (!normalized.isEmpty()) {
        endpoints.add(normalized);
      }
    }
    return Collections.unmodifiableSet(endpoints);
  }

  public static boolean isAllowed(final InetSocketAddress address) {
    if (address == null || address.isUnresolved()) {
      return false;
    }
    final Set<String> allowed = configuredEndpoints();
    if (allowed.isEmpty()) {
      return false;
    }
    return allowed.contains(normalizeEndpoint(address))
        || allowed.contains(normalizeHostOnly(address));
  }

  public static String normalizeEndpoint(final InetSocketAddress address) {
    if (address == null) {
      return "";
    }
    return normalizeEndpoint(address.getHostString() + ":" + address.getPort());
  }

  public static String normalizeHostOnly(final InetSocketAddress address) {
    if (address == null) {
      return "";
    }
    return address.getHostString().toLowerCase(Locale.ROOT);
  }

  public static String normalizeEndpoint(final String raw) {
    if (raw == null) {
      return "";
    }
    final String trimmed = raw.trim();
    if (trimmed.isEmpty()) {
      return "";
    }
    final int colon = trimmed.lastIndexOf(':');
    if (colon <= 0 || colon == trimmed.length() - 1) {
      return trimmed.toLowerCase(Locale.ROOT);
    }
    final String host = trimmed.substring(0, colon).toLowerCase(Locale.ROOT);
    final String port = trimmed.substring(colon + 1);
    return host + ":" + port;
  }
}
