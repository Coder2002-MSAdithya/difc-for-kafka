package org.apache.kafka.security.agent.policy;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Builds per-path taint reports for every source-topic field that can flow into sink fields.
 *
 * <p>The report is conservative: if any sink lineage keeps a field as PASSTHROUGH/DERIVED, the
 * source field is marked unsanitized for that path.
 */
public final class SourceFieldTaintAnalyzer {

  private SourceFieldTaintAnalyzer() {}

  public static void annotate(final AppProcessingPolicy.RelationalAlgebraAnalysis analysis) {
    if (analysis == null || analysis.getProcessingPaths().isEmpty()) {
      return;
    }
    for (final AppProcessingPolicy.ProcessingPathAnalysis path : analysis.getProcessingPaths()) {
      annotatePath(path);
    }
  }

  static void annotatePath(final AppProcessingPolicy.ProcessingPathAnalysis path) {
    if (path == null || path.getExpressionTree() == null || path.getEgressTopic() == null) {
      return;
    }
    final Map<String, FieldLineage> egressLineages =
        FieldLineageEvaluator.evaluateEgressLineages(path.getExpressionTree(), path.getEgressTopic());
    final List<AppProcessingPolicy.SourceFieldTaintStatus> report = new ArrayList<>();
    final Set<String> ingressTopics = ingressTopics(path);
    for (final String sourceTopic : ingressTopics) {
      final Set<String> sourceFields = TopicSchemaResolver.valueFieldsForTopic(sourceTopic);
      for (final String sourceField : sourceFields) {
        report.add(evaluateSourceField(path, sourceTopic, sourceField, egressLineages));
      }
    }
    path.setSourceFieldTaint(report);
    path.setSourceFieldCount(report.size());
    int sanitized = 0;
    for (final AppProcessingPolicy.SourceFieldTaintStatus status : report) {
      if (status.isSanitized()) {
        sanitized++;
      }
    }
    path.setSanitizedSourceFieldCount(sanitized);
    path.setUnsanitizedSourceFieldCount(Math.max(0, report.size() - sanitized));
  }

  private static Set<String> ingressTopics(final AppProcessingPolicy.ProcessingPathAnalysis path) {
    final Set<String> topics = new LinkedHashSet<>(path.getIngressTopics());
    if (topics.isEmpty() && path.getIngressTopic() != null && !path.getIngressTopic().isEmpty()) {
      topics.add(path.getIngressTopic());
    }
    return topics;
  }

  private static AppProcessingPolicy.SourceFieldTaintStatus evaluateSourceField(
      final AppProcessingPolicy.ProcessingPathAnalysis path,
      final String sourceTopic,
      final String sourceField,
      final Map<String, FieldLineage> egressLineages) {
    final AppProcessingPolicy.SourceFieldTaintStatus status =
        new AppProcessingPolicy.SourceFieldTaintStatus();
    status.setSourceTopic(sourceTopic);
    status.setSourceField(sourceField);

    final List<FieldLineage> uses = usesOfSourceField(sourceField, egressLineages);
    if (uses.isEmpty()) {
      status.setSanitized(true);
      status.setStatus("DROPPED");
      status.setSanitizationEvidence(List.of("source field absent from sink egress lineages"));
      return status;
    }

    final Set<String> sinkFields = new LinkedHashSet<>();
    final List<String> evidence = new ArrayList<>();
    boolean hasPassthroughLeak = false;
    boolean hasDerivedLeak = false;
    boolean hasAggregate = false;
    boolean hasBoolean = false;
    boolean hasKeyOnly = false;
    boolean hasConstant = false;
    for (final FieldLineage lineage : uses) {
      if (lineage == null) {
        continue;
      }
      if (lineage.getOutputField() != null && !lineage.getOutputField().isEmpty()) {
        sinkFields.add(lineage.getOutputField());
      }
      final FieldLineage.SanitizationKind kind = lineage.sanitizationKindEnum();
      evidence.add(summarizeLineage(lineage));
      switch (kind) {
        case PASSTHROUGH -> hasPassthroughLeak = true;
        case DERIVED -> hasDerivedLeak = true;
        case AGGREGATE -> hasAggregate = true;
        case BOOLEAN_PREDICATE -> hasBoolean = true;
        case KEY_ONLY -> hasKeyOnly = true;
        case CONSTANT, ABSENT -> hasConstant = true;
      }
    }
    status.setSinkFields(new ArrayList<>(sinkFields));
    status.setSanitizationEvidence(evidence);

    final boolean hasUnsanitized = hasPassthroughLeak || hasDerivedLeak;
    if (hasUnsanitized) {
      status.setSanitized(false);
      if ((hasAggregate || hasBoolean || hasKeyOnly || hasConstant)) {
        status.setStatus("PARTIAL_UNSANITIZED");
      } else if (hasPassthroughLeak) {
        status.setStatus("UNSANITIZED_PASSTHROUGH");
      } else {
        status.setStatus("UNSANITIZED_DERIVED");
      }
      return status;
    }

    status.setSanitized(true);
    if (hasAggregate) {
      status.setStatus(pathHasWindow(path) ? "SANITIZED_WINDOWED_AGGREGATE" : "SANITIZED_AGGREGATE");
    } else if (hasBoolean) {
      status.setStatus("SANITIZED_BOOLEAN");
    } else if (hasKeyOnly) {
      status.setStatus("SANITIZED_KEY_ONLY");
    } else if (hasConstant) {
      status.setStatus("SANITIZED_CONSTANT");
    } else {
      status.setStatus("SANITIZED_TRANSFORMED");
    }
    return status;
  }

  private static boolean pathHasWindow(final AppProcessingPolicy.ProcessingPathAnalysis path) {
    if (path == null) {
      return false;
    }
    if (path.getAlgebraExpression() != null
        && (path.getAlgebraExpression().contains("ω")
            || path.getAlgebraExpression().toLowerCase(java.util.Locale.ROOT).contains("window"))) {
      return true;
    }
    return RelationalAlgebraTreeSupport.treeContainsOperator(path.getExpressionTree(), "windowedby");
  }

  private static List<FieldLineage> usesOfSourceField(
      final String sourceField, final Map<String, FieldLineage> egressLineages) {
    if (sourceField == null || sourceField.isEmpty() || egressLineages == null || egressLineages.isEmpty()) {
      return List.of();
    }
    final List<FieldLineage> uses = new ArrayList<>();
    for (final FieldLineage lineage : egressLineages.values()) {
      if (lineage == null) {
        continue;
      }
      if (lineage.sourceFieldSet().contains(sourceField)) {
        uses.add(lineage);
      }
    }
    return uses;
  }

  private static String summarizeLineage(final FieldLineage lineage) {
    if (lineage == null) {
      return "unknown";
    }
    final String expression =
        (lineage.getExpression() == null || lineage.getExpression().isEmpty())
            ? lineage.getOutputField()
            : lineage.getExpression();
    return lineage.getOutputField()
        + "="
        + expression
        + "["
        + lineage.sanitizationKindEnum().name()
        + "]";
  }
}
