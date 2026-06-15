package org.apache.kafka.security.agent.policy;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LambdaSelectionAnalyzerTest {

  static final class Order {
    private String id;
    private String state;
    private int quantity;

    String getId() {
      return id;
    }

    String getState() {
      return state;
    }

    int getQuantity() {
      return quantity;
    }
  }

  static boolean isCreatedWithQuantity(final Order order) {
    return order != null && "CREATED".equals(order.getState()) && order.getQuantity() > 0;
  }

  @Test
  void infersSelectionFieldsFromPredicateMethodReference() {
    final Predicate<Order> predicate = LambdaSelectionAnalyzerTest::isCreatedWithQuantity;
    final OperatorCallbackEffect effect = LambdaSelectionAnalyzer.analyzePredicate(predicate);
    assertTrue(effect.selectionFields().contains("state"));
    assertTrue(effect.selectionFields().contains("quantity"));
    assertFalse(effect.selectionExpression().isEmpty());
    assertFalse(effect.selectionExpression().contains("CREATED"));
  }

  @Test
  void infersSelectionFieldsFromInlineFilterLambda() {
    final Predicate<Order> predicate = order -> order != null && "CREATED".equals(order.getState());
    final OperatorCallbackEffect effect = LambdaSelectionAnalyzer.analyzePredicate(predicate);
    assertTrue(effect.selectionFields().contains("state"));
  }
}
