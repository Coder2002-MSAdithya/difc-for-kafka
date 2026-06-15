package org.apache.kafka.security.agent.policy;

import org.objectweb.asm.tree.MethodNode;

import java.util.Set;

/** Infers σ (selection) field references from filter / predicate callbacks. */
public final class LambdaSelectionAnalyzer {

  private LambdaSelectionAnalyzer() {
  }

  public static OperatorCallbackEffect analyzePredicate(final Object predicate) {
    if (predicate == null) {
      return OperatorCallbackEffect.empty();
    }
    final Set<String> fromCapturing =
        LambdaBytecodeInspector.analyzeFromCapturingClassReferences(
            predicate,
            target -> {
              try {
                return LambdaBytecodeInspector.analyzeMethodSelection(
                    target.owner(),
                    target.name(),
                    target.desc(),
                    predicate.getClass().getClassLoader());
              } catch (final Exception ignored) {
                return Set.of();
              }
            });
    if (!fromCapturing.isEmpty()) {
      return OperatorCallbackEffect.selection(
          fromCapturing, LambdaBytecodeInspector.buildSelectionExpression(fromCapturing));
    }
    final LambdaBytecodeInspector.ResolvedCallbackMethod resolved =
        LambdaBytecodeInspector.resolveCallbackMethod(predicate);
    if (resolved != null) {
      try {
        final Set<String> fields =
            LambdaBytecodeInspector.analyzeMethodSelection(
                resolved.ownerClass(),
                resolved.methodName(),
                resolved.methodDesc(),
                predicate.getClass().getClassLoader());
        if (!fields.isEmpty()) {
          return OperatorCallbackEffect.selection(
              fields, LambdaBytecodeInspector.buildSelectionExpression(fields));
        }
      } catch (final Exception ignored) {
        // fall through
      }
    }
    final MethodNode functional = LambdaBytecodeInspector.findFunctionalMethodNode(predicate.getClass());
    if (functional != null) {
      final Set<String> fields = LambdaBytecodeInspector.extractSelectionFieldReferences(functional);
      if (!fields.isEmpty()) {
        return OperatorCallbackEffect.selection(
            fields, LambdaBytecodeInspector.buildSelectionExpression(fields));
      }
    }
    return OperatorCallbackEffect.empty();
  }
}
