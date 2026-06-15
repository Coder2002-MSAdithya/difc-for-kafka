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
          "newInstance");

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
    return resolveFromGeneratedLambda(callback.getClass());
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
    if (!builderFields.isEmpty()) {
      return builderFields;
    }
    final Set<String> constructorFields = extractConstructorOutputFields(method, classLoader);
    if (!constructorFields.isEmpty()) {
      return constructorFields;
    }
    return extractDelegatedProjectionFields(method, classLoader);
  }

  public static Set<String> extractSelectionFieldReferences(final MethodNode method) {
    if (method == null) {
      return Set.of();
    }
    final LinkedHashSet<String> fields = new LinkedHashSet<>();
    for (final AbstractInsnNode insn : method.instructions) {
      if (!(insn instanceof MethodInsnNode invoke)) {
        continue;
      }
      if (!isRecordAccessorInvoke(invoke)) {
        continue;
      }
      final String field = getterOrBooleanAccessorToField(invoke.name);
      if (field != null && !field.isEmpty()) {
        fields.add(field);
      }
    }
    for (final AbstractInsnNode insn : method.instructions) {
      if (insn.getOpcode() != Opcodes.GETFIELD) {
        continue;
      }
      if (!(insn instanceof org.objectweb.asm.tree.FieldInsnNode fieldInsn)) {
        continue;
      }
      if (fieldInsn.owner.startsWith("java/") || isFrameworkType(fieldInsn.owner)) {
        continue;
      }
      if (fieldInsn.name != null && !fieldInsn.name.isEmpty()) {
        fields.add(fieldInsn.name);
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
      final String name = invoke.name;
      if (name == null || name.isEmpty() || IGNORED_BUILDER_METHODS.contains(name)) {
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
        || invoke.getOpcode() == Opcodes.INVOKEINTERFACE
        || invoke.getOpcode() == Opcodes.INVOKESTATIC;
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
    if (apply == null) {
      return null;
    }
    final String owner = lambdaClass.getName().replace('.', '/');
    return new ResolvedCallbackMethod(owner, apply.name, apply.desc);
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

  static List<MethodReferenceTarget> listUnaryMethodReferences(
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
                      if (!isUnaryFunctionalDescriptor(handle.getDesc())) {
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
        listUnaryMethodReferences(capturingClassName, callback.getClass().getClassLoader());
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
