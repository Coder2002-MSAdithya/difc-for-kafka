package org.apache.kafka.security.agent.policy;

import java.util.Set;

/**
 * Infers relational-algebra effects from DSL operator callbacks: π projections, σ selections,
 * π_k key projections, and join value mappers.
 */
public final class CallbackProjectionAnalyzer {

  private CallbackProjectionAnalyzer() {
  }

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
          OperatorCallbackEffect.projection(LambdaProjectionAnalyzer.analyzeMapper(callback));
      case "process" ->
          OperatorCallbackEffect.projection(ProcessorOutputAnalyzer.analyzeProcessorSupplier(callback));
      case "filter", "filternot" -> LambdaSelectionAnalyzer.analyzePredicate(callback);
      case "selectkey", "groupby", "groupbykey" ->
          OperatorCallbackEffect.keyProjection(LambdaProjectionAnalyzer.analyzeKeyMapper(callback));
      case "join", "leftjoin", "outerjoin" ->
          OperatorCallbackEffect.projection(LambdaProjectionAnalyzer.analyzeMapper(callback));
      default -> OperatorCallbackEffect.empty();
    };
  }

  public static Set<String> analyze(final String operator, final Object callback) {
    return analyzeEffect(operator, callback).outputFields();
  }
}
