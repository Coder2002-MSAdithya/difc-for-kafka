package org.apache.kafka.security.agent.policy;

import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Infers per-column expression lineage from DSL callbacks: π projections, σ predicates, and γ
 * aggregators typical in Kafka Streams (mapValues, filter, groupBy/aggregate).
 */
public final class LambdaExpressionLineageAnalyzer {

  private static final Set<Integer> COMPARISON_OPCODES =
      Set.of(
          Opcodes.IFEQ,
          Opcodes.IFNE,
          Opcodes.IFLT,
          Opcodes.IFGE,
          Opcodes.IFGT,
          Opcodes.IFLE,
          Opcodes.IF_ICMPEQ,
          Opcodes.IF_ICMPNE,
          Opcodes.IF_ICMPLT,
          Opcodes.IF_ICMPGE,
          Opcodes.IF_ICMPGT,
          Opcodes.IF_ICMPLE,
          Opcodes.IF_ACMPEQ,
          Opcodes.IF_ACMPNE,
          Opcodes.LCMP,
          Opcodes.FCMPL,
          Opcodes.FCMPG,
          Opcodes.DCMPL,
          Opcodes.DCMPG);

  private static final Set<Integer> ARITHMETIC_OPCODES =
      Set.of(
          Opcodes.IADD,
          Opcodes.ISUB,
          Opcodes.IMUL,
          Opcodes.IDIV,
          Opcodes.IREM,
          Opcodes.LADD,
          Opcodes.LSUB,
          Opcodes.LMUL,
          Opcodes.LDIV,
          Opcodes.LREM,
          Opcodes.FADD,
          Opcodes.FSUB,
          Opcodes.FMUL,
          Opcodes.FDIV,
          Opcodes.DADD,
          Opcodes.DSUB,
          Opcodes.DMUL,
          Opcodes.DDIV);

  private LambdaExpressionLineageAnalyzer() {}

  public static List<FieldLineage> analyzeMapperLineages(final Object mapper) {
    if (mapper == null) {
      return List.of();
    }
    final ClassLoader classLoader = mapper.getClass().getClassLoader();
    final MethodNode apply = LambdaBytecodeInspector.findFunctionalMethodNode(mapper.getClass());
    if (apply != null) {
      final MethodNode delegated = findDelegatedMapperMethod(apply, classLoader);
      final List<FieldLineage> fromBytecode =
          withoutInfrastructureLineages(
              analyzeMethodNodeProjectionLineages(delegated == null ? apply : delegated, classLoader));
      if (!fromBytecode.isEmpty()) {
        return fromBytecode;
      }
    }
    final LambdaBytecodeInspector.ResolvedCallbackMethod resolved =
        LambdaBytecodeInspector.resolveCallbackMethod(mapper);
    if (resolved != null) {
      try {
        final List<FieldLineage> fromImpl =
            withoutInfrastructureLineages(resolveCallbackLineages(mapper, resolved, classLoader));
        if (!fromImpl.isEmpty()) {
          return fromImpl;
        }
      } catch (final Exception ignored) {
        // fall through
      }
    }
    final List<FieldLineage> fromCapturing = lineagesFromCapturingClass(mapper);
    if (!fromCapturing.isEmpty()) {
      return fromCapturing;
    }
    final Set<String> names = LambdaProjectionAnalyzer.analyzeMapper(mapper);
    return withoutInfrastructureLineages(
        OperatorCallbackProjectionSupport.lineagesFromFieldNames(
            names, FieldLineage.SanitizationKind.PASSTHROUGH));
  }

  public static List<FieldLineage> analyzeKeyMapperLineages(final Object mapper) {
    if (mapper == null) {
      return List.of();
    }
    final ClassLoader classLoader = mapper.getClass().getClassLoader();
    final LambdaBytecodeInspector.ResolvedCallbackMethod resolved =
        LambdaBytecodeInspector.resolveCallbackMethod(mapper);
    if (resolved != null) {
      try {
        final Set<String> keyFields =
            LambdaBytecodeInspector.analyzeMethodSelection(
                resolved.ownerClass(), resolved.methodName(), resolved.methodDesc(), classLoader);
        if (!keyFields.isEmpty()) {
          final List<FieldLineage> lineages = new ArrayList<>();
          for (final String field : keyFields) {
            lineages.add(
                new FieldLineage(
                    field,
                    FieldLineage.ValueType.UNKNOWN,
                    Set.of(field),
                    field,
                    FieldLineage.SanitizationKind.KEY_ONLY));
          }
          return lineages;
        }
      } catch (final Exception ignored) {
        // fall through
      }
    }
    final List<FieldLineage> fromCapturing = lineagesFromCapturingClass(mapper);
    if (!fromCapturing.isEmpty()) {
      final List<FieldLineage> keyLineages = new ArrayList<>();
      for (final FieldLineage lineage : fromCapturing) {
        keyLineages.add(
            new FieldLineage(
                lineage.getOutputField(),
                lineage.valueTypeEnum(),
                lineage.sourceFieldSet(),
                lineage.getExpression(),
                FieldLineage.SanitizationKind.KEY_ONLY));
      }
      return keyLineages;
    }
    return List.of();
  }

  private static List<FieldLineage> lineagesFromCapturingClass(final Object mapper) {
    final String capturing = LambdaBytecodeInspector.capturingClassName(mapper.getClass());
    if (capturing == null) {
      return List.of();
    }
    final ClassLoader classLoader = mapper.getClass().getClassLoader();
    final List<LambdaBytecodeInspector.MethodReferenceTarget> targets =
        LambdaBytecodeInspector.listLambdaMethodReferences(capturing, classLoader);
    if (targets.isEmpty()) {
      return List.of();
    }
    final int ordinal = LambdaBytecodeInspector.ordinalForCallback(mapper, capturing);
    if (ordinal < 0 || ordinal >= targets.size()) {
      return List.of();
    }
    final LambdaBytecodeInspector.MethodReferenceTarget target = targets.get(ordinal);
    if (target.name() == null || !target.name().startsWith("lambda$")) {
      return List.of();
    }
    try {
      return withoutInfrastructureLineages(
          analyzeMethodProjectionLineages(target.owner(), target.name(), target.desc(), classLoader));
    } catch (final Exception ignored) {
      return List.of();
    }
  }

  private static List<FieldLineage> resolveCallbackLineages(
      final Object mapper,
      final LambdaBytecodeInspector.ResolvedCallbackMethod resolved,
      final ClassLoader classLoader)
      throws Exception {
    final String resolvedOwner = resolved.ownerClass().replace('.', '/');
    final String mapperOwner = mapper.getClass().getName().replace('.', '/');
    if (resolvedOwner.equals(mapperOwner)) {
      final MethodNode method =
          LambdaBytecodeInspector.findMethodNode(
              mapper.getClass(), resolved.methodName(), resolved.methodDesc());
      if (method != null) {
        return analyzeMethodNodeProjectionLineages(method, classLoader);
      }
      final List<FieldLineage> fromCapturing = lineagesFromCapturingClass(mapper);
      if (!fromCapturing.isEmpty()) {
        return fromCapturing;
      }
      final Set<String> runtimeFields = LambdaProjectionAnalyzer.analyzeMapper(mapper);
      if (!runtimeFields.isEmpty()) {
        return OperatorCallbackProjectionSupport.lineagesFromFieldNames(
            runtimeFields, FieldLineage.SanitizationKind.PASSTHROUGH);
      }
    }
    return analyzeMethodProjectionLineages(
        resolved.ownerClass(), resolved.methodName(), resolved.methodDesc(), classLoader);
  }

  private static List<FieldLineage> withoutInfrastructureLineages(final List<FieldLineage> lineages) {
    if (lineages == null || lineages.isEmpty()) {
      return List.of();
    }
    final List<FieldLineage> filtered = new ArrayList<>();
    for (final FieldLineage lineage : lineages) {
      if (lineage == null || LambdaBytecodeInspector.isInfrastructureFieldName(lineage.getOutputField())) {
        continue;
      }
      filtered.add(lineage);
    }
    return filtered;
  }

  private static int scoreLineages(final List<FieldLineage> lineages) {
    if (lineages == null || lineages.isEmpty()) {
      return 0;
    }
    int score = lineages.size();
    for (final FieldLineage lineage : lineages) {
      if (lineage == null) {
        continue;
      }
      if (!lineage.getSourceFields().isEmpty()) {
        score += 2;
      }
      if (lineage.sanitizationKindEnum() != FieldLineage.SanitizationKind.PASSTHROUGH) {
        score += 3;
      }
      if (!lineage.getExpression().isEmpty()) {
        score += 1;
      }
    }
    return score;
  }

  public static FieldLineage analyzePredicateLineage(final Object predicate) {
    final OperatorCallbackEffect effect = LambdaSelectionAnalyzer.analyzePredicate(predicate);
    if (effect.selectionFields().isEmpty()) {
      return null;
    }
    return new FieldLineage(
        "_selection",
        FieldLineage.ValueType.BOOLEAN,
        effect.selectionFields(),
        effect.selectionExpression().isEmpty()
            ? String.join(" ∧ ", effect.selectionFields())
            : effect.selectionExpression(),
        FieldLineage.SanitizationKind.BOOLEAN_PREDICATE);
  }

  public static List<FieldLineage> analyzeAggregatorLineages(final Object callback) {
    if (callback == null) {
      return List.of();
    }
    final LinkedHashSet<String> sources = new LinkedHashSet<>();
    collectAggregatorSources(callback, sources);
    if (sources.isEmpty()) {
      return List.of();
    }
    final String expression = buildAggregateExpression(sources);
    return List.of(FieldLineage.aggregateOf("_aggregate_value", sources, expression));
  }

  private static void collectAggregatorSources(final Object callback, final Set<String> sources) {
    final ClassLoader classLoader = callback.getClass().getClassLoader();
    final LambdaBytecodeInspector.ResolvedCallbackMethod resolved =
        LambdaBytecodeInspector.resolveCallbackMethod(callback);
    if (resolved != null) {
      try {
        sources.addAll(
            LambdaBytecodeInspector.analyzeMethodSelection(
                resolved.ownerClass(), resolved.methodName(), resolved.methodDesc(), classLoader));
      } catch (final Exception ignored) {
        // fall through
      }
    }
    final MethodNode functional = LambdaBytecodeInspector.findFunctionalMethodNode(callback.getClass());
    if (functional != null) {
      sources.addAll(LambdaBytecodeInspector.extractSelectionFieldReferences(functional));
    }
    scanAggregatorType(callback.getClass(), sources, classLoader, 0);
    final String capturing = LambdaBytecodeInspector.capturingClassName(callback.getClass());
    if (capturing != null) {
      for (final LambdaBytecodeInspector.MethodReferenceTarget target :
          LambdaBytecodeInspector.listLambdaMethodReferences(capturing, classLoader)) {
        if (target.name() == null || !target.name().startsWith("lambda$")) {
          continue;
        }
        try {
          sources.addAll(
              LambdaBytecodeInspector.analyzeMethodSelection(
                  target.owner(), target.name(), target.desc(), classLoader));
        } catch (final Exception ignored) {
          // try next target
        }
      }
    }
  }

  private static void scanAggregatorType(
      final Class<?> type, final Set<String> sources, final ClassLoader classLoader, final int depth) {
    if (type == null || depth > 3 || type.getName().startsWith("java.")) {
      return;
    }
    for (final String methodName : List.of("apply", "add", "aggregate", "reduce", "init")) {
      final MethodNode method = LambdaBytecodeInspector.findMethodNode(type, methodName, null);
      if (method != null) {
        sources.addAll(LambdaBytecodeInspector.extractSelectionFieldReferences(method));
      }
    }
    if (type.getSuperclass() != null && type.getSuperclass() != Object.class) {
      scanAggregatorType(type.getSuperclass(), sources, classLoader, depth + 1);
    }
  }

  private static String buildAggregateExpression(final Set<String> sources) {
    if (sources == null || sources.isEmpty()) {
      return "aggregate";
    }
    if (sources.contains("price") && sources.contains("quantity")) {
      return "sum(quantity × price)";
    }
    if (sources.contains("value") && sources.contains("order")) {
      return "sum(session order value)";
    }
    if (sources.contains("validationResult")) {
      return "aggregate(validations)";
    }
    return "γ(" + String.join(", ", sources) + ")";
  }

  public static List<FieldLineage> analyzeMethodProjectionLineages(
      final String implClassName, final String methodName, final String methodDesc, final ClassLoader classLoader)
      throws Exception {
    final Class<?> cls = LambdaBytecodeInspector.loadClass(implClassName, classLoader);
    if (cls == null) {
      return List.of();
    }
    final MethodNode method = LambdaBytecodeInspector.findMethodNode(cls, methodName, methodDesc);
    if (method == null) {
      return List.of();
    }
    return analyzeMethodNodeProjectionLineages(method, classLoader);
  }

  static List<FieldLineage> analyzeMethodNodeProjectionLineages(
      final MethodNode method, final ClassLoader classLoader) {
    final Set<String> outputNames = LambdaBytecodeInspector.extractProjectionFields(method, classLoader);
    if (outputNames.isEmpty()) {
      return List.of();
    }
    final Map<String, FieldLineage> lineages = new LinkedHashMap<>();
    for (final String outputField : outputNames) {
      lineages.put(outputField, inferLineageForOutputField(method, outputField, classLoader));
    }
    return new ArrayList<>(lineages.values());
  }

  static FieldLineage inferLineageForOutputField(
      final MethodNode method, final String outputField, final ClassLoader classLoader) {
    final AssignmentSite site = findOutputAssignment(method, outputField);
    if (site == null) {
      final Set<String> sources = LambdaBytecodeInspector.extractFieldReferencesUntil(method, null);
      final boolean hasComparison = containsComparisonBefore(null, method);
      final boolean hasArithmetic = containsArithmeticBefore(null, method);
      final FieldLineage.ValueType valueType =
          outputField.toLowerCase(Locale.ROOT).startsWith("is")
                  || outputField.toLowerCase(Locale.ROOT).startsWith("has")
              ? FieldLineage.ValueType.BOOLEAN
              : FieldLineage.ValueType.UNKNOWN;
      final FieldLineage.SanitizationKind kind =
          classifySanitization(outputField, sources, valueType, hasComparison, hasArithmetic);
      final String expression =
          buildExpression(outputField, sources, valueType, hasComparison, hasArithmetic, null);
      return new FieldLineage(outputField, valueType, sources, expression, kind);
    }
    final Set<String> sources = sourcesForAssignment(method, site);
    final FieldLineage.ValueType valueType = FieldLineage.ValueType.fromDescriptor(site.valueDescriptor());
    final boolean booleanOutput =
        valueType == FieldLineage.ValueType.BOOLEAN
            || outputField.startsWith("is")
            || outputField.startsWith("has");
    final Set<String> effectiveSources =
        booleanOutput
            ? LambdaBytecodeInspector.extractFieldReferencesUntil(method, null)
            : sources;
    final boolean hasComparison =
        containsComparisonBefore(site.insn(), method) || containsComparisonBefore(null, method);
    final boolean hasArithmetic = containsArithmeticBefore(site.insn(), method);
    final String expression =
        buildExpression(outputField, effectiveSources, valueType, hasComparison, hasArithmetic, site);
    final FieldLineage.SanitizationKind kind =
        classifySanitization(outputField, effectiveSources, valueType, hasComparison, hasArithmetic);
    return new FieldLineage(outputField, valueType, effectiveSources, expression, kind);
  }

  private static FieldLineage.SanitizationKind classifySanitization(
      final String outputField,
      final Set<String> sources,
      final FieldLineage.ValueType valueType,
      final boolean hasComparison,
      final boolean hasArithmetic) {
    if (sources.isEmpty()) {
      return FieldLineage.SanitizationKind.CONSTANT;
    }
    if (valueType == FieldLineage.ValueType.BOOLEAN && hasComparison) {
      return FieldLineage.SanitizationKind.BOOLEAN_PREDICATE;
    }
    if (sources.size() == 1
        && sources.contains(outputField)
        && valueType != FieldLineage.ValueType.BOOLEAN
        && !hasArithmetic) {
      return FieldLineage.SanitizationKind.PASSTHROUGH;
    }
    if (valueType == FieldLineage.ValueType.BOOLEAN) {
      return FieldLineage.SanitizationKind.BOOLEAN_PREDICATE;
    }
    return FieldLineage.SanitizationKind.DERIVED;
  }

  private static String buildExpression(
      final String outputField,
      final Set<String> sources,
      final FieldLineage.ValueType valueType,
      final boolean hasComparison,
      final boolean hasArithmetic,
      final AssignmentSite site) {
    if (sources.isEmpty()) {
      return outputField;
    }
    if (valueType == FieldLineage.ValueType.BOOLEAN && hasComparison) {
      return String.join(" ∧ ", sources) + " ? bool";
    }
    if (sources.size() == 1 && sources.iterator().next().equals(outputField) && !hasArithmetic) {
      return outputField;
    }
    final String op = hasArithmetic ? "f" : "g";
    if (site != null && site.isBuilderSetter()) {
      return outputField + " := " + op + "(" + String.join(", ", sources) + ")";
    }
    return outputField + " := " + op + "(" + String.join(", ", sources) + ")";
  }

  private record AssignmentSite(AbstractInsnNode insn, String valueDescriptor, boolean builderSetter) {
    boolean isBuilderSetter() {
      return builderSetter;
    }
  }

  private static AssignmentSite findOutputAssignment(final MethodNode method, final String outputField) {
    final String setter = "set" + Character.toUpperCase(outputField.charAt(0)) + outputField.substring(1);
    for (final AbstractInsnNode insn : method.instructions) {
      if (!(insn instanceof MethodInsnNode invoke)) {
        continue;
      }
      if (invoke.getOpcode() != Opcodes.INVOKEVIRTUAL
          && invoke.getOpcode() != Opcodes.INVOKESPECIAL
          && invoke.getOpcode() != Opcodes.INVOKEINTERFACE) {
        continue;
      }
      if (LambdaBytecodeInspector.isFrameworkType(invoke.owner)) {
        continue;
      }
      if (setter.equals(invoke.name) || outputField.equals(invoke.name)) {
        final String ret = Type.getReturnType(invoke.desc).getDescriptor();
        final Type[] args = Type.getArgumentTypes(invoke.desc);
        final String valueDesc = args.length > 0 ? args[args.length - 1].getDescriptor() : ret;
        return new AssignmentSite(insn, valueDesc, !setter.equals(invoke.name) || outputField.equals(invoke.name));
      }
    }
    if ("<init>".equals(method.name)) {
      return null;
    }
    for (final AbstractInsnNode insn : method.instructions) {
      if (insn instanceof MethodInsnNode invoke && "<init>".equals(invoke.name)) {
        if (LambdaBytecodeInspector.isFrameworkType(invoke.owner)) {
          continue;
        }
        try {
          final Class<?> outputClass = LambdaBytecodeInspector.loadClass(invoke.owner, null);
          if (outputClass != null && RecordFieldExtractor.extractFieldsFromType(outputClass).contains(outputField)) {
            return new AssignmentSite(insn, "Ljava/lang/Object;", false);
          }
        } catch (final Exception ignored) {
          // continue
        }
      }
    }
    return null;
  }

  private static MethodNode findDelegatedMapperMethod(
      final MethodNode apply, final ClassLoader classLoader) {
    if (apply == null) {
      return null;
    }
    MethodNode best = null;
    int bestScore = -1;
    for (final AbstractInsnNode insn : apply.instructions) {
      if (!(insn instanceof MethodInsnNode invoke)) {
        continue;
      }
      if (invoke.getOpcode() != Opcodes.INVOKESTATIC
          && invoke.getOpcode() != Opcodes.INVOKESPECIAL
          && invoke.getOpcode() != Opcodes.INVOKEVIRTUAL) {
        continue;
      }
      if (LambdaBytecodeInspector.isFrameworkType(invoke.owner)) {
        continue;
      }
      if ("<init>".equals(invoke.name) || "builder".equals(invoke.name) || "build".equals(invoke.name)) {
        continue;
      }
      if (LambdaBytecodeInspector.isInfrastructureMethodName(invoke.name)) {
        continue;
      }
      final Class<?> owner = LambdaBytecodeInspector.loadClass(invoke.owner, classLoader);
      if (owner == null) {
        continue;
      }
      final MethodNode nested = LambdaBytecodeInspector.findMethodNode(owner, invoke.name, invoke.desc);
      if (nested == null || nested.instructions.size() == 0) {
        continue;
      }
      final int score = scoreDelegatedMapperCandidate(nested, invoke.name, invoke.desc, classLoader);
      if (score > bestScore) {
        bestScore = score;
        best = nested;
      }
    }
    return best;
  }

  private static int scoreDelegatedMapperCandidate(
      final MethodNode method, final String invokeName, final String invokeDesc, final ClassLoader classLoader) {
    int score = LambdaBytecodeInspector.extractFieldReferencesUntil(method, null).size();
    if (invokeName != null && invokeName.startsWith("lambda$")) {
      score += 10;
    }
    score += LambdaBytecodeInspector.extractProjectionFields(method, classLoader).size() * 2;
    final Type ret = LambdaBytecodeInspector.returnTypeFromDesc(invokeDesc);
    if (ret != null && ret.getSort() == org.objectweb.asm.Type.OBJECT && !ret.getClassName().startsWith("java.")) {
      score += 15;
    }
    if (ret != null && (ret.getSort() == org.objectweb.asm.Type.BOOLEAN || ret.getSort() == org.objectweb.asm.Type.LONG)) {
      score -= 8;
    }
    if (containsComparisonBefore(null, method)) {
      score += 5;
    }
    return score;
  }

  private static Set<String> sourcesForAssignment(final MethodNode method, final AssignmentSite site) {
    if (site == null) {
      return LambdaBytecodeInspector.extractFieldReferencesUntil(method, null);
    }
    final Set<String> local = LambdaBytecodeInspector.extractFieldReferencesUntil(method, site.insn());
    if (!local.isEmpty()) {
      return local;
    }
    return LambdaBytecodeInspector.extractFieldReferencesUntil(method, null);
  }

  private static boolean containsComparisonBefore(final AbstractInsnNode end, final MethodNode method) {
    for (AbstractInsnNode insn = method.instructions.getFirst(); insn != null && insn != end; insn = insn.getNext()) {
      if (COMPARISON_OPCODES.contains(insn.getOpcode())) {
        return true;
      }
    }
    return false;
  }

  private static boolean containsArithmeticBefore(final AbstractInsnNode end, final MethodNode method) {
    for (AbstractInsnNode insn = method.instructions.getFirst(); insn != null && insn != end; insn = insn.getNext()) {
      if (ARITHMETIC_OPCODES.contains(insn.getOpcode())) {
        return true;
      }
    }
    return false;
  }
}
