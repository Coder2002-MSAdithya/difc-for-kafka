package org.apache.kafka.security.agent.policy;

import java.util.List;
import java.util.Set;

/** Static egress projections for the JUG Istanbul plain-client pipeline. */
public final class JugPipelineProjections {

  public static final String TOPIC_ORDER = "ORDER_EVENT_TOPIC";
  public static final String TOPIC_STOCK_CHECK = "STOCK_CHECK_EVENT_TOPIC";
  public static final String TOPIC_VALIDATION = "VALIDATION_EVENT_TOPIC";
  public static final String TOPIC_BILLING = "BILLING_EVENT_TOPIC";

  public static final String OP_FOR_STOCK_CHECK = "forStockCheck";
  public static final String OP_FOR_VALIDATION = "forValidation";
  public static final String OP_FOR_BILLING = "forBilling";

  private JugPipelineProjections() {
  }

  public static void registerEgressProjections() {
    EgressProjectionRegistry.register(
        TOPIC_STOCK_CHECK,
        Set.of("customerId", "productId", "amount", "inStock", "cardNumber"));
    EgressProjectionRegistry.register(
        TOPIC_VALIDATION, Set.of("customerId", "isNumberValid"));
    EgressProjectionRegistry.register(TOPIC_BILLING, Set.of("customerId", "price"));
  }

  public static AppProcessingPolicy.OperatorCallbackProjection callbackForOperator(
      final String operator) {
    final AppProcessingPolicy.OperatorCallbackProjection projection =
        new AppProcessingPolicy.OperatorCallbackProjection();
    projection.setOperator(operator);
    projection.setOutputFields(outputFieldsForOperator(operator));
    return projection;
  }

  public static List<AppProcessingPolicy.OperatorCallbackProjection> callbacksForOperators(
      final List<String> operators) {
    final List<AppProcessingPolicy.OperatorCallbackProjection> callbacks = new java.util.ArrayList<>();
    for (final String operator : operators) {
      if (isPipelineProjectionOperator(operator)) {
        callbacks.add(callbackForOperator(operator));
      } else {
        final AppProcessingPolicy.OperatorCallbackProjection empty =
            new AppProcessingPolicy.OperatorCallbackProjection();
        empty.setOperator(operator);
        callbacks.add(empty);
      }
    }
    return callbacks;
  }

  public static boolean isPipelineProjectionOperator(final String operator) {
    if (operator == null) {
      return false;
    }
    final String normalized = operator.trim();
    return OP_FOR_STOCK_CHECK.equals(normalized)
        || OP_FOR_VALIDATION.equals(normalized)
        || OP_FOR_BILLING.equals(normalized);
  }

  public static boolean isPipelineProjectionOperatorNormalized(final String normalizedOp) {
    return Set.of("forstockcheck", "forvalidation", "forbilling").contains(normalizedOp);
  }

  public static List<String> outputFieldsForOperator(final String operator) {
    return switch (operator) {
      case OP_FOR_STOCK_CHECK ->
          List.of("customerId", "productId", "amount", "inStock", "cardNumber");
      case OP_FOR_VALIDATION -> List.of("customerId", "isNumberValid");
      case OP_FOR_BILLING -> List.of("customerId", "price");
      default -> List.of();
    };
  }
}
