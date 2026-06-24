package org.apache.kafka.security.agent.policy;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OperatorCallbackProjectionSupportTest {

  @Test
  void mergePreferringLiveRetainsFieldLineages() {
    final AppProcessingPolicy.OperatorCallbackProjection live =
        new AppProcessingPolicy.OperatorCallbackProjection();
    live.setOperator("mapValues");
    live.setOutputFields(List.of("orderId", "checkType"));
    live.setFieldLineages(
        List.of(
            new FieldLineage(
                "orderId",
                FieldLineage.ValueType.STRING,
                Set.of("id"),
                "orderId := id",
                FieldLineage.SanitizationKind.DERIVED)));

    final AppProcessingPolicy.OperatorCallbackProjection manifest =
        new AppProcessingPolicy.OperatorCallbackProjection();
    manifest.setOperator("mapValues");
    manifest.setOutputFields(List.of("orderId", "checkType", "validationResult"));

    final List<AppProcessingPolicy.OperatorCallbackProjection> merged =
        OperatorCallbackProjectionSupport.mergePreferringLive(List.of(live), List.of(manifest));

    assertEquals(1, merged.size());
    assertEquals(1, merged.get(0).getOutputFields().size());
    assertEquals("orderId", merged.get(0).getOutputFields().get(0));
    assertFalse(merged.get(0).getFieldLineages().isEmpty());
    assertEquals("orderId := id", merged.get(0).getFieldLineages().get(0).getExpression());
  }

  @Test
  void mergeFallsBackToManifestWhenLiveEmpty() {
    final AppProcessingPolicy.OperatorCallbackProjection manifest =
        new AppProcessingPolicy.OperatorCallbackProjection();
    manifest.setOperator("filter");
    manifest.setSelectionExpression("state ∧ quantity");

    final List<AppProcessingPolicy.OperatorCallbackProjection> merged =
        OperatorCallbackProjectionSupport.mergePreferringLive(List.of(), List.of(manifest));

    assertEquals(1, merged.size());
    assertTrue(merged.get(0).getSelectionExpression().contains("state"));
  }
}
