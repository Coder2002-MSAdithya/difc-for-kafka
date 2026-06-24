package org.apache.kafka.security.agent.policy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LambdaExpressionLineageAnalyzerTest {

  @BeforeEach
  void resetOrdinalTracking() {
    LambdaProjectionAnalyzer.resetOrdinalTrackingForTests();
  }

  static final class PricedOrder {
    private final int price;

    PricedOrder(final int price) {
      this.price = price;
    }

    int getPrice() {
      return price;
    }
  }

  static final class PricedView {
    private final boolean isHighValue;

    PricedView(final boolean isHighValue) {
      this.isHighValue = isHighValue;
    }

    static Builder builder() {
      return new Builder();
    }

    static final class Builder {
      private boolean isHighValue;

      Builder isHighValue(final boolean value) {
        this.isHighValue = value;
        return this;
      }

      PricedView build() {
        return new PricedView(isHighValue);
      }
    }
  }

  @Test
  void projectionBooleanComparisonSanitizesNumericSource() {
    final Function<PricedOrder, PricedView> mapper =
        order -> PricedView.builder().isHighValue(order.getPrice() >= 50).build();
    final List<FieldLineage> lineages = LambdaExpressionLineageAnalyzer.analyzeMapperLineages(mapper);
    assertFalse(lineages.isEmpty());
    final FieldLineage lineage =
        lineages.stream().filter(l -> "isHighValue".equals(l.getOutputField())).findFirst().orElseThrow();
    assertTrue(lineage.sourceFieldSet().contains("price"));
    assertEquals(FieldLineage.SanitizationKind.BOOLEAN_PREDICATE, lineage.sanitizationKindEnum());
    assertEquals(FieldLineage.ValueType.BOOLEAN, lineage.valueTypeEnum());
  }

  @Test
  void passthroughProjectionRetainsSourceKind() {
    final Function<PricedOrder, PricedOrder> mapper = order -> order;
    final List<FieldLineage> lineages = LambdaExpressionLineageAnalyzer.analyzeMapperLineages(mapper);
    assertFalse(lineages.isEmpty());
    for (final FieldLineage lineage : lineages) {
      if ("price".equals(lineage.getOutputField())) {
        assertEquals(FieldLineage.SanitizationKind.PASSTHROUGH, lineage.sanitizationKindEnum());
      }
    }
  }
}
