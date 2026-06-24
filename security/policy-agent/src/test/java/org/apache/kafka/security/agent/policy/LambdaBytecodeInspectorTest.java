package org.apache.kafka.security.agent.policy;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LambdaBytecodeInspectorTest {

  @Test
  void discoversMethodReferencesInCapturingClass() throws Exception {
    final List<LambdaBytecodeInspector.MethodReferenceTarget> targets =
        LambdaBytecodeInspector.listLambdaMethodReferences(
            LambdaSelectionAnalyzerTest.class.getName(),
            LambdaSelectionAnalyzerTest.class.getClassLoader());
    assertFalse(targets.isEmpty());
    boolean foundSelection = false;
    for (final LambdaBytecodeInspector.MethodReferenceTarget target : targets) {
      final Set<String> selection =
          LambdaBytecodeInspector.analyzeMethodSelection(
              target.owner(), target.name(), target.desc(), getClass().getClassLoader());
      if (selection.contains("state") && selection.contains("quantity")) {
        foundSelection = true;
      }
    }
    assertTrue(foundSelection);
  }

  @Test
  void mainConfigLoaderLambdaIsNotTreatedAsDomainProjection() throws Exception {
    final List<LambdaBytecodeInspector.MethodReferenceTarget> targets =
        LambdaBytecodeInspector.listLambdaMethodReferences(
            "io.confluent.examples.streams.microservices.ValidationsAggregatorService",
            getClass().getClassLoader());
    final LambdaBytecodeInspector.MethodReferenceTarget mainConfig =
        targets.stream()
            .filter(t -> "lambda$main$15".equals(t.name()))
            .findFirst()
            .orElse(null);
    if (mainConfig == null) {
      return;
    }
    final Set<String> projection =
        LambdaBytecodeInspector.analyzeMethodProjection(
            mainConfig.owner(), mainConfig.name(), mainConfig.desc(), getClass().getClassLoader());
    assertFalse(projection.contains("buildPropertiesFromConfigFile"));
  }

  @Test
  void joinLambdaProjectsOrderState() throws Exception {
    final List<LambdaBytecodeInspector.MethodReferenceTarget> targets =
        LambdaBytecodeInspector.listLambdaMethodReferences(
            "io.confluent.examples.streams.microservices.ValidationsAggregatorService",
            getClass().getClassLoader());
    final LambdaBytecodeInspector.MethodReferenceTarget joinLambda =
        targets.stream()
            .filter(t -> "lambda$aggregateOrderValidations$13".equals(t.name()))
            .findFirst()
            .orElse(null);
    if (joinLambda == null) {
      return;
    }
    final Set<String> projection =
        LambdaBytecodeInspector.analyzeMethodProjection(
            joinLambda.owner(), joinLambda.name(), joinLambda.desc(), getClass().getClassLoader());
    assertTrue(projection.contains("state") || projection.contains("customerId"));
    assertFalse(projection.contains("buildPropertiesFromConfigFile"));
  }
}
