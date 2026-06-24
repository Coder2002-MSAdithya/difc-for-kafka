package org.apache.kafka.security.agent.policy;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Merges live callback capture with manifest supplements without dropping expression lineage. */
public final class OperatorCallbackProjectionSupport {

  private OperatorCallbackProjectionSupport() {}

  public static List<AppProcessingPolicy.OperatorCallbackProjection> mergePreferringLive(
      final List<AppProcessingPolicy.OperatorCallbackProjection> live,
      final List<AppProcessingPolicy.OperatorCallbackProjection> supplement) {
    if (supplement == null || supplement.isEmpty()) {
      return live == null ? List.of() : new ArrayList<>(live);
    }
    if (live == null || live.isEmpty()) {
      return new ArrayList<>(supplement);
    }
    final Map<String, AppProcessingPolicy.OperatorCallbackProjection> byOperator = new LinkedHashMap<>();
    for (final AppProcessingPolicy.OperatorCallbackProjection projection : live) {
      if (projection != null && projection.getOperator() != null) {
        byOperator.put(normalize(projection.getOperator()), projection);
      }
    }
    for (final AppProcessingPolicy.OperatorCallbackProjection projection : supplement) {
      if (projection == null || projection.getOperator() == null) {
        continue;
      }
      final String key = normalize(projection.getOperator());
      final AppProcessingPolicy.OperatorCallbackProjection existing = byOperator.get(key);
      byOperator.put(key, existing == null ? projection : mergeProjection(existing, projection));
    }
    return new ArrayList<>(byOperator.values());
  }

  public static List<AppProcessingPolicy.OperatorCallbackProjection> alignToOperators(
      final List<String> operators,
      final List<AppProcessingPolicy.OperatorCallbackProjection> callbacks) {
    if (operators == null || operators.isEmpty()) {
      return callbacks == null ? List.of() : new ArrayList<>(callbacks);
    }
    final Map<String, AppProcessingPolicy.OperatorCallbackProjection> byOperator = new LinkedHashMap<>();
    if (callbacks != null) {
      for (final AppProcessingPolicy.OperatorCallbackProjection callback : callbacks) {
        if (callback != null && callback.getOperator() != null) {
          byOperator.putIfAbsent(normalize(callback.getOperator()), callback);
        }
      }
    }
    final List<AppProcessingPolicy.OperatorCallbackProjection> aligned = new ArrayList<>();
    for (final String operator : operators) {
      final AppProcessingPolicy.OperatorCallbackProjection callback = byOperator.get(normalize(operator));
      if (callback != null) {
        aligned.add(callback);
      } else {
        final AppProcessingPolicy.OperatorCallbackProjection empty =
            new AppProcessingPolicy.OperatorCallbackProjection();
        empty.setOperator(operator);
        aligned.add(empty);
      }
    }
    return aligned;
  }

  public static AppProcessingPolicy.OperatorCallbackProjection mergeProjection(
      final AppProcessingPolicy.OperatorCallbackProjection live,
      final AppProcessingPolicy.OperatorCallbackProjection supplement) {
    if (live == null) {
      return supplement;
    }
    if (supplement == null) {
      return live;
    }
    final AppProcessingPolicy.OperatorCallbackProjection merged = new AppProcessingPolicy.OperatorCallbackProjection();
    merged.setOperator(live.getOperator() != null ? live.getOperator() : supplement.getOperator());
    merged.setOutputFields(preferOutputFields(live, supplement));
    merged.setSelectionFields(
        preferList(live.getSelectionFields(), supplement.getSelectionFields()));
    merged.setSelectionExpression(
        preferString(live.getSelectionExpression(), supplement.getSelectionExpression()));
    merged.setKeyFields(preferList(live.getKeyFields(), supplement.getKeyFields()));
    merged.setFieldLineages(
        preferLineages(live.getFieldLineages(), supplement.getFieldLineages()));
    return merged;
  }

  private static List<String> preferOutputFields(
      final AppProcessingPolicy.OperatorCallbackProjection live,
      final AppProcessingPolicy.OperatorCallbackProjection supplement) {
    final List<String> liveFields = sanitizeOutputFields(live.getOutputFields());
    final List<String> supplementFields = sanitizeOutputFields(supplement.getOutputFields());
    if (!live.getFieldLineages().isEmpty()) {
      final LinkedHashSet<String> fromLineage = new LinkedHashSet<>();
      for (final FieldLineage lineage : live.getFieldLineages()) {
        if (lineage != null
            && lineage.getOutputField() != null
            && !lineage.getOutputField().isEmpty()
            && !LambdaBytecodeInspector.isInfrastructureFieldName(lineage.getOutputField())) {
          fromLineage.add(lineage.getOutputField());
        }
      }
      if (!fromLineage.isEmpty()) {
        return new ArrayList<>(fromLineage);
      }
    }
    if (!liveFields.isEmpty()
        && supplementFields.size() > liveFields.size()
        && live.getFieldLineages().isEmpty()) {
      return new ArrayList<>(supplementFields);
    }
    return preferList(liveFields, supplementFields);
  }

  private static List<String> sanitizeOutputFields(final List<String> fields) {
    final LinkedHashSet<String> sanitized = new LinkedHashSet<>();
    if (fields == null) {
      return List.of();
    }
    for (final String field : fields) {
      if (field != null
          && !field.isEmpty()
          && !LambdaBytecodeInspector.isInfrastructureFieldName(field)) {
        sanitized.add(field);
      }
    }
    return new ArrayList<>(sanitized);
  }

  public static List<FieldLineage> lineagesFromFieldNames(
      final Set<String> names, final FieldLineage.SanitizationKind defaultKind) {
    if (names == null || names.isEmpty()) {
      return List.of();
    }
    final List<FieldLineage> lineages = new ArrayList<>();
    for (final String name : names) {
      if (name == null || name.isEmpty() || LambdaBytecodeInspector.isInfrastructureFieldName(name)) {
        continue;
      }
      final FieldLineage.ValueType type =
          name.toLowerCase(Locale.ROOT).startsWith("is")
                  || name.toLowerCase(Locale.ROOT).startsWith("has")
              ? FieldLineage.ValueType.BOOLEAN
              : FieldLineage.ValueType.UNKNOWN;
      lineages.add(new FieldLineage(name, type, Set.of(name), name, defaultKind));
    }
    return lineages;
  }

  public static int richnessScore(final AppProcessingPolicy.OperatorCallbackProjection projection) {
    if (projection == null) {
      return 0;
    }
    int score = 0;
    score += projection.getOutputFields().size();
    score += projection.getSelectionFields().size();
    if (!projection.getSelectionExpression().isEmpty()) {
      score += 2;
    }
    score += projection.getKeyFields().size();
    score += projection.getFieldLineages().size() * 3;
    for (final FieldLineage lineage : projection.getFieldLineages()) {
      if (lineage != null && !lineage.getExpression().isEmpty()) {
        score += 2;
      }
    }
    return score;
  }

  private static String normalize(final String operator) {
    return RelationalAlgebraTreeSupport.normalizeOp(
        operator.endsWith("()") ? operator : operator + "()");
  }

  private static List<String> preferList(final List<String> live, final List<String> supplement) {
    if (live != null && !live.isEmpty()) {
      return new ArrayList<>(live);
    }
    return supplement == null ? new ArrayList<>() : new ArrayList<>(supplement);
  }

  private static String preferString(final String live, final String supplement) {
    if (live != null && !live.isEmpty()) {
      return live;
    }
    return supplement == null ? "" : supplement;
  }

  private static List<FieldLineage> preferLineages(
      final List<FieldLineage> live, final List<FieldLineage> supplement) {
    if (live != null && !live.isEmpty()) {
      return new ArrayList<>(live);
    }
    return supplement == null ? new ArrayList<>() : new ArrayList<>(supplement);
  }
}
