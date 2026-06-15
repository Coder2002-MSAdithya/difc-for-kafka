package org.apache.kafka.security.agent.policy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LambdaProjectionAnalyzerTest {

  @BeforeEach
  void resetOrdinals() {
    LambdaProjectionAnalyzer.resetOrdinalTrackingForTests();
  }

  static final class SampleOrder {
    private Long id;
    private Long customerId;
    private int price;
    private String status;

    static Builder builder() {
      return new Builder();
    }

    static final class Builder {
      private final SampleOrder order = new SampleOrder();

      Builder id(final Long id) {
        order.id = id;
        return this;
      }

      Builder status(final String status) {
        order.status = status;
        return this;
      }

      SampleOrder build() {
        return order;
      }
    }
  }

  static SampleOrder downstreamResponse(final SampleOrder order) {
    if (order == null) {
      return null;
    }
    return SampleOrder.builder().id(order.id).status(order.status).build();
  }

  @Test
  void infersProjectionFromMethodReferenceMapper() {
    final Function<SampleOrder, SampleOrder> mapper = LambdaProjectionAnalyzerTest::downstreamResponse;
    final Set<String> fields = LambdaProjectionAnalyzer.analyzeMapper(mapper);
    assertTrue(fields.contains("id"));
    assertTrue(fields.contains("status"));
  }

  static final class SampleValidation {
    private String orderId;
    private String checkType;
    private String validationResult;

    SampleValidation(final String orderId, final String checkType, final String validationResult) {
      this.orderId = orderId;
      this.checkType = checkType;
      this.validationResult = validationResult;
    }
  }

  @Test
  void infersProjectionFromConstructorReturningMapper() {
    final Function<SampleOrder, SampleValidation> mapper =
        order -> new SampleValidation(String.valueOf(order.id), "CHECK", "PASS");
    final Set<String> fields = LambdaProjectionAnalyzer.analyzeMapper(mapper);
    assertTrue(fields.contains("orderId"));
    assertTrue(fields.contains("checkType"));
    assertTrue(fields.contains("validationResult"));
  }

  @Test
  void analyzeEffectCapturesSelectionAndProjectionSeparately() {
    final OperatorCallbackEffect filterEffect =
        CallbackProjectionAnalyzer.analyzeEffect(
            "filter", (Predicate<SampleOrder>) LambdaProjectionAnalyzerTest::hasStatus);
    assertTrue(filterEffect.selectionFields().contains("status"));
    assertFalse(filterEffect.selectionExpression().contains("sample"));

    final OperatorCallbackEffect mapEffect =
        CallbackProjectionAnalyzer.analyzeEffect(
            "mapValues",
            (Function<SampleOrder, SampleValidation>)
                order -> new SampleValidation(String.valueOf(order.id), "CHECK", "PASS"));
    assertTrue(mapEffect.outputFields().contains("orderId"));
  }

  static boolean hasStatus(final SampleOrder order) {
    return order != null && order.status != null;
  }
}
