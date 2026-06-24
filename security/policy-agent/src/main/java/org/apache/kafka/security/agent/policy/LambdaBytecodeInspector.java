package org.apache.kafka.security.agent.policy;

import org.apache.kafka.security.agent.LambdaRegistry;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.io.InputStream;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Shared bytecode inspection for DSL callbacks: builder/constructor projections, getter references
 * for selections, and constant stripping in predicate expressions.
 */
public final class LambdaBytecodeInspector {

  static final Set<String> IGNORED_BUILDER_METHODS =
      Set.of(
          "build",
          "builder",
          "newBuilder",
          "create",
          "make",
          "of",
          "valueOf",
          "requireNonNull",
          "equals",
          "hashCode",
          "toString",
          "clone",
          "getClass",
          "apply",
          "accept",
          "test",
          "run",
          "call",
          "invoke",
          "compareTo",
          "compare",
          "validate",
          "newInstance",
          "buildPropertiesFromConfigFile",
          "baseStreamsConfig",
          "getOptionValue",
          "simpleMerge");

  private LambdaBytecodeInspector() {
  }

  public record ResolvedCallbackMethod(String ownerClass, String methodName, String methodDesc) {}

  public static ResolvedCallbackMethod resolveCallbackMethod(final Object callback) {
    if (callback == null) {
      return null;
    }
    final SerializedLambda lambda = trySerializedLambda(callback);
    if (lambda != null) {
      return new ResolvedCallbackMethod(
          lambda.getImplClass(), lambda.getImplMethodName(), lambda.getImplMethodSignature());
    }
    final LambdaRegistry.LambdaInfo registryInfo = LambdaRegistry.lookup(callback.getClass());
    if (registryInfo != null) {
      return new ResolvedCallbackMethod(
          registryInfo.implClass, registryInfo.implMethod, registryInfo.implDesc);
    }
    final ResolvedCallbackMethod fromApply = resolveDelegatedImplFromApply(callback);
    if (fromApply != null) {
      return fromApply;
    }
    return resolveFromGeneratedLambda(callback.getClass());
  }

  static ResolvedCallbackMethod resolveFromCapturingClass(final Object callback) {
    final String capturingClassName = capturingClassName(callback.getClass());
    if (capturingClassName == null) {
      return null;
    }
    final ClassLoader classLoader = callback.getClass().getClassLoader();
    final List<MethodReferenceTarget> targets = listLambdaMethodReferences(capturingClassName, classLoader);
    if (targets.isEmpty()) {
      return null;
    }
    final int ordinal = ordinalForCallback(callback, capturingClassName);
    if (ordinal < 0 || ordinal >= targets.size()) {
      return null;
    }
    final MethodReferenceTarget target = targets.get(ordinal);
    if (target.name() == null || !target.name().startsWith("lambda$")) {
      return null;
    }
    return new ResolvedCallbackMethod(target.owner(), target.name(), target.desc());
  }

  /** Hidden-lambda {@code apply} trampolines that delegate to {@code lambda$…} in the capturing class. */
  static ResolvedCallbackMethod resolveDelegatedImplFromApply(final Object callback) {
    if (callback == null) {
      return null;
    }
    final String capturingClassName = capturingClassName(callback.getClass());
    if (capturingClassName == null) {
      return null;
    }
    final String capturingInternal = capturingClassName.replace('.', '/');
    final MethodNode apply = findFunctionalMethodNode(callback.getClass());
    if (apply == null) {
      return null;
    }
    final ClassLoader classLoader = callback.getClass().getClassLoader();
    ResolvedCallbackMethod best = null;
    int bestScore = -1;
    for (final AbstractInsnNode insn : apply.instructions) {
      if (!(insn instanceof MethodInsnNode invoke)) {
        continue;
      }
      if (!capturingInternal.equals(invoke.owner)) {
        continue;
      }
      if (invoke.name == null || !invoke.name.startsWith("lambda$")) {
        continue;
      }
      final MethodNode nested = findMethodNode(loadClass(invoke.owner, classLoader), invoke.name, invoke.desc);
      int score = nested == null ? 0 : scoreDelegatedImplCandidate(nested, invoke.name, invoke.desc, classLoader);
      if (score > bestScore) {
        bestScore = score;
        best = new ResolvedCallbackMethod(invoke.owner, invoke.name, invoke.desc);
      }
    }
    return best;
  }

  private static int scoreDelegatedImplCandidate(
      final MethodNode method,
      final String invokeName,
      final String methodDesc,
      final ClassLoader classLoader) {
    int score = extractProjectionFields(method, classLoader).size();
    if (invokeName != null && invokeName.startsWith("lambda$")) {
      score += 10;
    }
    final Type ret = returnTypeFromDesc(methodDesc);
    if (ret != null && ret.getSort() == Type.OBJECT && !ret.getClassName().startsWith("java.")) {
      score += 15;
    }
    if (ret != null && (ret.getSort() == Type.BOOLEAN || ret.getSort() == Type.LONG)) {
      score -= 8;
    }
    score += extractFieldReferencesUntil(method, null).size();
    return score;
  }

  static boolean isInfrastructureMethodName(final String name) {
    if (name == null || name.isEmpty()) {
      return false;
    }
    if (IGNORED_BUILDER_METHODS.contains(name)) {
      return true;
    }
    return name.startsWith("getOptionValue")
        || (name.startsWith("build") && (name.contains("Config") || name.contains("Properties")));
  }

  static boolean isInfrastructureFieldName(final String field) {
    if (isInfrastructureMethodName(field)) {
      return true;
    }
    return field != null
        && (field.startsWith("get")
            || field.equals("printf")
            || field.equals("key")
            || field.equals("_aggregate_value")
            || field.equals("_selection")
            || field.equals("simpleMerge")
            || field.matches("(long|int|double|float|short|byte|char)Value"));
  }

  public static MethodNode findMethodNode(final Class<?> cls, final String name, final String desc) {
    if (cls == null || name == null) {
      return null;
    }
    try (InputStream in = openClassBytecode(cls)) {
      if (in == null) {
        return null;
      }
      final ClassReader reader = new ClassReader(in);
      final ClassNode node = new ClassNode();
      reader.accept(node, 0);
      MethodNode nameMatch = null;
      for (final MethodNode method : node.methods) {
        if (!name.equals(method.name)) {
          continue;
        }
        if (desc == null || desc.equals(method.desc)) {
          return method;
        }
        nameMatch = method;
      }
      return nameMatch;
    } catch (final Exception ignored) {
      return null;
    }
  }

  public static MethodNode findFunctionalMethodNode(final Class<?> lambdaClass) {
    if (lambdaClass == null) {
      return null;
    }
    for (final String name : List.of("apply", "test", "get")) {
      final MethodNode method = findMethodNode(lambdaClass, name, null);
      if (method != null) {
        return method;
      }
    }
    return null;
  }

  public static Set<String> extractProjectionFields(final MethodNode method, final ClassLoader classLoader) {
    if (method == null) {
      return Set.of();
    }
    final Set<String> builderFields = extractBuilderSetterFields(method);
    final Set<String> constructorFields = extractConstructorOutputFields(method, classLoader);
    if (!builderFields.isEmpty() || !constructorFields.isEmpty()) {
      final LinkedHashSet<String> merged = new LinkedHashSet<>(constructorFields);
      merged.addAll(builderFields);
      return merged;
    }
    return extractDelegatedProjectionFields(method, classLoader);
  }

  public static Set<String> extractSelectionFieldReferences(final MethodNode method) {
    return extractFieldReferencesUntil(method, null);
  }

  /**
   * Record accessors and domain GETFIELD references from method entry until {@code end} (exclusive).
   */
  public static Set<String> extractFieldReferencesUntil(
      final MethodNode method, final AbstractInsnNode end) {
    if (method == null) {
      return Set.of();
    }
    final LinkedHashSet<String> fields = new LinkedHashSet<>();
    for (AbstractInsnNode insn = method.instructions.getFirst();
        insn != null && insn != end;
        insn = insn.getNext()) {
      if (insn instanceof MethodInsnNode invoke && isRecordAccessorInvoke(invoke)) {
        final String field = getterOrBooleanAccessorToField(invoke.name);
        if (isLikelyDomainField(field)) {
          fields.add(field);
        }
      }
      if (insn.getOpcode() == Opcodes.GETFIELD && insn instanceof org.objectweb.asm.tree.FieldInsnNode fieldInsn) {
        if (fieldInsn.owner.startsWith("java/") || isFrameworkType(fieldInsn.owner)) {
          continue;
        }
        if (isLikelyDomainField(fieldInsn.name)) {
          fields.add(fieldInsn.name);
        }
      }
    }
    return fields;
  }

  /**
   * Builds a field-only selection expression: constants and enum literals are omitted, referenced
   * record fields are joined with {@code ∧}.
   */
  public static String buildSelectionExpression(final Set<String> selectionFields) {
    if (selectionFields == null || selectionFields.isEmpty()) {
      return "";
    }
    return String.join(" ∧ ", selectionFields);
  }

  public static String buildPredicateExpression(final MethodNode method) {
    if (method == null) {
      return "";
    }
    final Set<String> fields = extractSelectionFieldReferences(method);
    if (fields.isEmpty()) {
      return "";
    }
    if (containsComparisonOpcode(method)) {
      return String.join(" ∧ ", fields) + " ?";
    }
    return buildSelectionExpression(fields);
  }

  private static boolean containsComparisonOpcode(final MethodNode method) {
    for (AbstractInsnNode insn = method.instructions.getFirst(); insn != null; insn = insn.getNext()) {
      if (isComparisonOpcode(insn.getOpcode())) {
        return true;
      }
    }
    return false;
  }

  private static boolean isComparisonOpcode(final int opcode) {
    return opcode == Opcodes.IFEQ
        || opcode == Opcodes.IFNE
        || opcode == Opcodes.IFLT
        || opcode == Opcodes.IFGE
        || opcode == Opcodes.IFGT
        || opcode == Opcodes.IFLE
        || opcode == Opcodes.IF_ICMPEQ
        || opcode == Opcodes.IF_ICMPNE
        || opcode == Opcodes.IF_ICMPLT
        || opcode == Opcodes.IF_ICMPGE
        || opcode == Opcodes.IF_ICMPGT
        || opcode == Opcodes.IF_ICMPLE
        || opcode == Opcodes.IF_ACMPEQ
        || opcode == Opcodes.IF_ACMPNE
        || opcode == Opcodes.LCMP
        || opcode == Opcodes.FCMPL
        || opcode == Opcodes.FCMPG
        || opcode == Opcodes.DCMPL
        || opcode == Opcodes.DCMPG;
  }

  static boolean isLikelyDomainField(final String field) {
    if (field == null || field.isEmpty()) {
      return false;
    }
    if (isInfrastructureMethodName(field) || isInfrastructureFieldName(field)) {
      return false;
    }
    if (field.equals(field.toUpperCase(java.util.Locale.ROOT)) && field.contains("_")) {
      return false;
    }
    return !field.startsWith("lambda$");
  }

  public static boolean isConstantInstruction(final AbstractInsnNode insn) {
    if (insn == null) {
      return false;
    }
    return switch (insn.getOpcode()) {
      case Opcodes.ACONST_NULL,
          Opcodes.ICONST_M1,
          Opcodes.ICONST_0,
          Opcodes.ICONST_1,
          Opcodes.ICONST_2,
          Opcodes.ICONST_3,
          Opcodes.ICONST_4,
          Opcodes.ICONST_5,
          Opcodes.LCONST_0,
          Opcodes.LCONST_1,
          Opcodes.FCONST_0,
          Opcodes.FCONST_1,
          Opcodes.FCONST_2,
          Opcodes.DCONST_0,
          Opcodes.DCONST_1,
          Opcodes.BIPUSH,
          Opcodes.SIPUSH ->
          true;
      default -> insn instanceof LdcInsnNode;
    };
  }

  static Set<String> extractBuilderSetterFields(final MethodNode method) {
    final LinkedHashSet<String> fields = new LinkedHashSet<>();
    for (final AbstractInsnNode insn : method.instructions) {
      if (!(insn instanceof MethodInsnNode invoke)) {
        continue;
      }
      if (!isBuilderStyleInvoke(invoke)) {
        continue;
      }
      if (isFrameworkType(invoke.owner) || invoke.owner.startsWith("java/io/")) {
        continue;
      }
      final String name = invoke.name;
      if (name == null || name.isEmpty() || isInfrastructureMethodName(name)) {
        continue;
      }
      if (name.startsWith("get") || name.endsWith("Value")) {
        continue;
      }
      if (name.startsWith("set") && name.length() > 3) {
        fields.add(Character.toLowerCase(name.charAt(3)) + name.substring(4));
        continue;
      }
      if (Character.isLowerCase(name.charAt(0)) && name.chars().allMatch(Character::isLetterOrDigit)) {
        fields.add(name);
      }
    }
    return fields;
  }

  static Set<String> extractConstructorOutputFields(final MethodNode method, final ClassLoader classLoader) {
    MethodInsnNode constructorCall = null;
    for (AbstractInsnNode insn = method.instructions.getLast(); insn != null; insn = insn.getPrevious()) {
      if (!(insn instanceof MethodInsnNode invoke)) {
        continue;
      }
      if (invoke.getOpcode() == Opcodes.INVOKESPECIAL && "<init>".equals(invoke.name)) {
        constructorCall = invoke;
        break;
      }
    }
    if (constructorCall == null || isFrameworkType(constructorCall.owner)) {
      return Set.of();
    }
    final Class<?> outputClass = loadClass(constructorCall.owner, classLoader);
    if (outputClass == null) {
      return Set.of();
    }
    return RecordFieldExtractor.extractFieldsFromType(outputClass);
  }

  static Set<String> extractDelegatedProjectionFields(final MethodNode method, final ClassLoader classLoader) {
    for (final AbstractInsnNode insn : method.instructions) {
      if (!(insn instanceof MethodInsnNode invoke)) {
        continue;
      }
      if (invoke.owner.startsWith("org/apache/kafka/streams")
          || invoke.owner.startsWith("org/apache/kafka/clients")
          || invoke.owner.startsWith("org/apache/kafka/common")
          || invoke.owner.startsWith("java/")
          || invoke.owner.startsWith("jdk/")) {
        continue;
      }
      if ("<init>".equals(invoke.name)) {
        continue;
      }
      try {
        final Class<?> owner = loadClass(invoke.owner, classLoader);
        if (owner == null) {
          continue;
        }
        final MethodNode delegated = findMethodNode(owner, invoke.name, invoke.desc);
        if (delegated == null) {
          continue;
        }
        final Set<String> nested = extractProjectionFields(delegated, classLoader);
        if (!nested.isEmpty()) {
          return nested;
        }
      } catch (final Exception ignored) {
        // try next target
      }
    }
    return Set.of();
  }

  static Set<String> extractKeyFieldReferences(final MethodNode method) {
    return extractSelectionFieldReferences(method);
  }

  static boolean isBuilderStyleInvoke(final MethodInsnNode invoke) {
    return invoke.getOpcode() == Opcodes.INVOKEVIRTUAL
        || invoke.getOpcode() == Opcodes.INVOKESPECIAL
        || invoke.getOpcode() == Opcodes.INVOKEINTERFACE;
  }

  static boolean isRecordAccessorInvoke(final MethodInsnNode invoke) {
    if (invoke.getOpcode() != Opcodes.INVOKEVIRTUAL && invoke.getOpcode() != Opcodes.INVOKEINTERFACE) {
      return false;
    }
    if (isFrameworkType(invoke.owner)) {
      return false;
    }
    final String name = invoke.name;
    return name != null
        && ((name.startsWith("get") && name.length() > 3) || (name.startsWith("is") && name.length() > 2));
  }

  static String getterOrBooleanAccessorToField(final String methodName) {
    if (methodName == null || methodName.isEmpty()) {
      return null;
    }
    if (methodName.startsWith("get") && methodName.length() > 3) {
      return Character.toLowerCase(methodName.charAt(3)) + methodName.substring(4);
    }
    if (methodName.startsWith("is") && methodName.length() > 2) {
      return Character.toLowerCase(methodName.charAt(2)) + methodName.substring(3);
    }
    return null;
  }

  static boolean isFrameworkType(final String internalName) {
    return internalName == null
        || internalName.startsWith("java/")
        || internalName.startsWith("javax/")
        || internalName.startsWith("jdk/")
        || internalName.startsWith("org/apache/kafka/streams/")
        || internalName.startsWith("org/apache/kafka/clients/")
        || internalName.startsWith("org/apache/kafka/common/")
        || internalName.startsWith("org/apache/kafka/server/");
  }

  static Class<?> loadClass(final String internalOrBinaryName, final ClassLoader classLoader) {
    if (internalOrBinaryName == null || internalOrBinaryName.isEmpty()) {
      return null;
    }
    final String binary = internalOrBinaryName.replace('/', '.');
    try {
      return Class.forName(binary, false, classLoader == null ? ClassLoader.getSystemClassLoader() : classLoader);
    } catch (final ClassNotFoundException ignored) {
      return null;
    }
  }

  static InputStream openClassBytecode(final Class<?> cls) {
    if (cls == null) {
      return null;
    }
    final String resource = cls.getName().replace('.', '/') + ".class";
    final ClassLoader loader = cls.getClassLoader();
    if (loader != null) {
      final InputStream fromLoader = loader.getResourceAsStream(resource);
      if (fromLoader != null) {
        return fromLoader;
      }
    }
    return cls.getResourceAsStream("/" + resource);
  }

  static Set<String> analyzeMethodProjection(
      final String implClassName, final String methodName, final String methodDesc, final ClassLoader classLoader)
      throws Exception {
    final Class<?> cls = loadClass(implClassName, classLoader);
    if (cls == null) {
      return Set.of();
    }
    final MethodNode method = findMethodNode(cls, methodName, methodDesc);
    if (method == null) {
      return Set.of();
    }
    return extractProjectionFields(method, classLoader);
  }

  static Set<String> analyzeMethodSelection(
      final String implClassName, final String methodName, final String methodDesc, final ClassLoader classLoader)
      throws Exception {
    final Class<?> cls = loadClass(implClassName, classLoader);
    if (cls == null) {
      return Set.of();
    }
    final MethodNode method = findMethodNode(cls, methodName, methodDesc);
    if (method == null) {
      return Set.of();
    }
    return extractSelectionFieldReferences(method);
  }

  static ResolvedCallbackMethod resolveFromGeneratedLambda(final Class<?> lambdaClass) {
    final MethodNode apply = findFunctionalMethodNode(lambdaClass);
    if (apply != null) {
      final String owner = lambdaClass.getName().replace('.', '/');
      return new ResolvedCallbackMethod(owner, apply.name, apply.desc);
    }
    for (final String name : List.of("apply", "test", "get")) {
      for (final java.lang.reflect.Method method : lambdaClass.getDeclaredMethods()) {
        if (!name.equals(method.getName())) {
          continue;
        }
        return new ResolvedCallbackMethod(
            lambdaClass.getName().replace('.', '/'),
            method.getName(),
            Type.getMethodDescriptor(method));
      }
    }
    return null;
  }

  static Class<?> firstParameterTypeFromDesc(final String desc, final ClassLoader classLoader) {
    if (!isFunctionalDescriptor(desc)) {
      return null;
    }
    final Type[] args = Type.getArgumentTypes(desc);
    if (args.length == 0) {
      return null;
    }
    final Type first = args[0];
    if (first.getSort() == Type.OBJECT) {
      return loadClass(first.getInternalName(), classLoader);
    }
    return switch (first.getSort()) {
      case Type.BOOLEAN -> boolean.class;
      case Type.BYTE -> byte.class;
      case Type.CHAR -> char.class;
      case Type.SHORT -> short.class;
      case Type.INT -> int.class;
      case Type.LONG -> long.class;
      case Type.FLOAT -> float.class;
      case Type.DOUBLE -> double.class;
      default -> null;
    };
  }

  static SerializedLambda trySerializedLambda(final Object lambda) {
    try {
      final Method writeReplace = lambda.getClass().getDeclaredMethod("writeReplace");
      writeReplace.setAccessible(true);
      final Object replacement = writeReplace.invoke(lambda);
      if (replacement instanceof SerializedLambda sl) {
        return sl;
      }
    } catch (final ReflectiveOperationException ignored) {
      return null;
    }
    return null;
  }

  static boolean isUnaryFunctionalDescriptor(final String desc) {
    if (desc == null || !desc.startsWith("(")) {
      return false;
    }
    final int close = desc.indexOf(')');
    if (close <= 1) {
      return false;
    }
    final String params = desc.substring(1, close);
    if (params.isEmpty() || params.charAt(0) == '[') {
      return false;
    }
    if (params.charAt(0) == 'L') {
      return params.endsWith(";") && params.indexOf(';') == params.length() - 1;
    }
    return false;
  }

  static Class<?> parameterTypeFromDesc(final String desc, final ClassLoader classLoader) {
    if (!isUnaryFunctionalDescriptor(desc)) {
      return null;
    }
    final String paramDesc = desc.substring(1, desc.indexOf(')'));
    if (!paramDesc.startsWith("L") || !paramDesc.endsWith(";")) {
      return null;
    }
    return loadClass(paramDesc.substring(1, paramDesc.length() - 1), classLoader);
  }

  static List<MethodReferenceTarget> listLambdaMethodReferences(
      final String capturingClassName, final ClassLoader classLoader) {
    final List<MethodReferenceTarget> targets = new java.util.ArrayList<>();
    try {
      final Class<?> owner = Class.forName(capturingClassName, false, classLoader);
      try (InputStream in = openClassBytecode(owner)) {
        if (in == null) {
          return List.of();
        }
        final ClassReader reader = new ClassReader(in);
        reader.accept(
            new org.objectweb.asm.ClassVisitor(Opcodes.ASM9) {
              @Override
              public org.objectweb.asm.MethodVisitor visitMethod(
                  final int access,
                  final String name,
                  final String desc,
                  final String signature,
                  final String[] exceptions) {
                return new org.objectweb.asm.MethodVisitor(Opcodes.ASM9) {
                  @Override
                  public void visitInvokeDynamicInsn(
                      final String invokeName,
                      final String invokeDesc,
                      final Handle bootstrapMethodHandle,
                      final Object... bootstrapMethodArguments) {
                    if (bootstrapMethodArguments == null) {
                      return;
                    }
                    for (final Object arg : bootstrapMethodArguments) {
                      if (!(arg instanceof Handle handle)) {
                        continue;
                      }
                      if (!isFunctionalDescriptor(handle.getDesc())) {
                        continue;
                      }
                      targets.add(
                          new MethodReferenceTarget(handle.getOwner(), handle.getName(), handle.getDesc()));
                    }
                  }
                };
              }
            },
            0);
      }
    } catch (final Exception ignored) {
      return List.of();
    }
    return targets;
  }

  static List<MethodReferenceTarget> listUnaryMethodReferences(
      final String capturingClassName, final ClassLoader classLoader) {
    return listLambdaMethodReferences(capturingClassName, classLoader);
  }

  static boolean isFunctionalDescriptor(final String desc) {
    if (desc == null || !desc.startsWith("(")) {
      return false;
    }
    final int close = desc.indexOf(')');
    return close > 0 && close < desc.length() - 1;
  }

  static String capturingClassName(final Class<?> lambdaClass) {
    if (lambdaClass == null) {
      return null;
    }
    final String lambdaName = lambdaClass.getName();
    final int idx = lambdaName.indexOf("$$Lambda");
    if (idx < 0) {
      return null;
    }
    return lambdaName.substring(0, idx);
  }

  static int ordinalForCallback(final Object callback, final String capturingClassName) {
    final Map<Integer, Integer> ordinals = CallbackOrdinalTracker.ordinalsFor(capturingClassName);
    final int identity = System.identityHashCode(callback);
    return ordinals.computeIfAbsent(
        identity,
        id -> CallbackOrdinalTracker.nextOrdinal(capturingClassName));
  }

  static Set<String> analyzeFromCapturingClassReferences(
      final Object callback,
      final java.util.function.Function<MethodReferenceTarget, Set<String>> analyzer) {
    final String capturingClassName = capturingClassName(callback.getClass());
    if (capturingClassName == null) {
      return Set.of();
    }
    final List<MethodReferenceTarget> targets =
        listLambdaMethodReferences(capturingClassName, callback.getClass().getClassLoader());
    if (targets.isEmpty()) {
      return Set.of();
    }
    final int ordinal = ordinalForCallback(callback, capturingClassName);
    if (ordinal >= 0 && ordinal < targets.size()) {
      final Set<String> selected = analyzer.apply(targets.get(ordinal));
      if (!selected.isEmpty()) {
        return selected;
      }
    }
    return Set.of();
  }

  static Type returnTypeFromDesc(final String desc) {
    if (desc == null) {
      return Type.VOID_TYPE;
    }
    try {
      return Type.getReturnType(desc);
    } catch (final Exception ignored) {
      return Type.VOID_TYPE;
    }
  }

  record MethodReferenceTarget(String owner, String name, String desc) {}
}
