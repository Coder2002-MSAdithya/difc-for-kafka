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
        LambdaBytecodeInspector.listUnaryMethodReferences(
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
}
