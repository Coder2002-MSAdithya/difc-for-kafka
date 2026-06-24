package org.apache.kafka.security.agent.policy;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Infers relational-algebra effects from DSL operator callbacks: π projections, σ selections,
 * π_k key projections, and join value mappers.
 */
public final class CallbackProjectionAnalyzer {

  private CallbackProjectionAnalyzer() {}

  public static OperatorCallbackEffect analyzeEffect(final String operator, final Object callback) {
    if (operator == null || callback == null) {
      return OperatorCallbackEffect.empty();
    }
    return switch (RelationalAlgebraTreeSupport.normalizeOp(operator + "()")) {
      case "mapvalues",
              "map",
              "flatmapvalues",
              "flatmap",
              "transform",
              "transformvalues" ->
          projectionEffect(LambdaExpressionLineageAnalyzer.analyzeMapperLineages(callback));
      case "process" -> {
        final Set<String> fields = ProcessorOutputAnalyzer.analyzeProcessorSupplier(callback);
        if (fields.isEmpty()) {
          yield OperatorCallbackEffect.empty();
        }
        yield OperatorCallbackEffect.projectionWithLineage(
            OperatorCallbackProjectionSupport.lineagesFromFieldNames(
                fields, FieldLineage.SanitizationKind.PASSTHROUGH));
      }
      case "filter", "filternot" -> {
        final OperatorCallbackEffect selection = LambdaSelectionAnalyzer.analyzePredicate(callback);
        final FieldLineage predicate = LambdaExpressionLineageAnalyzer.analyzePredicateLineage(callback);
        if (predicate == null) {
          yield selection;
        }
        yield OperatorCallbackEffect.selectionWithLineage(
            selection.selectionFields(), selection.selectionExpression(), predicate);
      }
      case "selectkey", "groupby", "groupbykey" -> {
        final List<FieldLineage> keyLineages =
            LambdaExpressionLineageAnalyzer.analyzeKeyMapperLineages(callback);
        if (!keyLineages.isEmpty()) {
          final LinkedHashSet<String> keyFields = new LinkedHashSet<>();
          for (final FieldLineage lineage : keyLineages) {
            keyFields.add(lineage.getOutputField());
          }
          yield OperatorCallbackEffect.builder()
              .keyFields(keyFields)
              .fieldLineages(keyLineages)
              .build();
        }
        final Set<String> keyFields = LambdaProjectionAnalyzer.analyzeKeyMapper(callback);
        if (keyFields.isEmpty()) {
          yield OperatorCallbackEffect.empty();
        }
        yield OperatorCallbackEffect.builder()
            .keyFields(keyFields)
            .fieldLineages(
                OperatorCallbackProjectionSupport.lineagesFromFieldNames(
                    keyFields, FieldLineage.SanitizationKind.KEY_ONLY))
            .build();
      }
      case "windowedby" -> {
        if (callback instanceof final String semantics && !semantics.isEmpty()) {
          yield OperatorCallbackEffect.selection(Set.of(), "window:" + semantics);
        }
        yield OperatorCallbackEffect.empty();
      }
      case "aggregate", "reduce", "count" ->
          OperatorCallbackEffect.projectionWithLineage(
              LambdaExpressionLineageAnalyzer.analyzeAggregatorLineages(callback));
      case "join", "leftjoin", "outerjoin" ->
          projectionEffect(LambdaExpressionLineageAnalyzer.analyzeMapperLineages(callback));
      default -> OperatorCallbackEffect.empty();
    };
  }

  private static OperatorCallbackEffect projectionEffect(final List<FieldLineage> lineages) {
    if (lineages == null || lineages.isEmpty()) {
      return OperatorCallbackEffect.empty();
    }
    return OperatorCallbackEffect.projectionWithLineage(lineages);
  }

  public static Set<String> analyze(final String operator, final Object callback) {
    return analyzeEffect(operator, callback).outputFields();
  }
}
