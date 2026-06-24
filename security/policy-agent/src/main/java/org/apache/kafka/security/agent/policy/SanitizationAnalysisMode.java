package org.apache.kafka.security.agent.policy;

import java.util.Locale;

/** Controls which sanitization analyses are emitted in policy attestations. */
public enum SanitizationAnalysisMode {
  RA,
  TAINT,
  BOTH;

  private static final String PROP = "policy.sanitization.analysis.mode";
  private static final String ENV = "DIFC_AGENT_SANITIZATION_MODE";

  public static SanitizationAnalysisMode current() {
    final String raw = readMode();
    if (raw == null || raw.isBlank()) {
      return BOTH;
    }
    return switch (raw.trim().toLowerCase(Locale.ROOT)) {
      case "ra", "lineage", "relational", "relational-algebra" -> RA;
      case "taint", "tainting" -> TAINT;
      case "both", "hybrid" -> BOTH;
      default -> BOTH;
    };
  }

  public boolean emitTaintReport() {
    return this == TAINT || this == BOTH;
  }

  private static String readMode() {
    final String prop = System.getProperty(PROP);
    if (prop != null && !prop.isBlank()) {
      return prop;
    }
    final String env = System.getenv(ENV);
    if (env != null && !env.isBlank()) {
      return env;
    }
    return null;
  }
}
