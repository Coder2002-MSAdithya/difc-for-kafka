package org.apache.kafka.security.agent.policy;

import org.apache.kafka.security.agent.LambdaRegistry;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.io.InputStream;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Infers mapValues/map projection output columns by scanning mapper lambda implementation bytecode
 * for builder-style method names (e.g. {@code .id(}, {@code .orderId(}).
 */
public final class LambdaProjectionAnalyzer {

  private static final Set<String> IGNORED_METHODS =
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
          "reserve",
          "confirm",
          "newInstance");

  private LambdaProjectionAnalyzer() {
  }

  public static Set<String> analyzeMapper(final Object mapper) {
    if (mapper == null) {
      return Set.of();
    }
    final LambdaRegistry.LambdaInfo registryInfo = LambdaRegistry.lookup(mapper.getClass());
    if (registryInfo != null) {
      try {
        return analyzeMethod(registryInfo.implClass, registryInfo.implMethod, registryInfo.implDesc);
      } catch (final Exception e) {
        return Set.of();
      }
    }
    final SerializedLambda lambda = trySerializedLambda(mapper);
    if (lambda == null) {
      return Set.of();
    }
    try {
      return analyzeMethod(
          lambda.getImplClass(), lambda.getImplMethodName(), lambda.getImplMethodSignature());
    } catch (final Exception e) {
      return Set.of();
    }
  }

  static Set<String> analyzeMethod(
      final String implClassName, final String methodName, final String methodDesc) throws Exception {
    final String resource = "/" + implClassName.replace('.', '/') + ".class";
    final Class<?> cls = Class.forName(implClassName.replace('/', '.'));
    try (InputStream in = cls.getResourceAsStream(resource)) {
      if (in == null) {
        return Set.of();
      }
      final ClassReader reader = new ClassReader(in);
      final ClassNode node = new ClassNode();
      reader.accept(node, 0);
      for (final MethodNode method : node.methods) {
        if (method.name.equals(methodName) && method.desc.equals(methodDesc)) {
          return extractProjectedFields(method);
        }
      }
    }
    return Set.of();
  }

  private static Set<String> extractProjectedFields(final MethodNode method) {
    final LinkedHashSet<String> fields = new LinkedHashSet<>();
    for (final AbstractInsnNode insn : method.instructions) {
      if (!(insn instanceof MethodInsnNode invoke)) {
        continue;
      }
      if (invoke.getOpcode() != Opcodes.INVOKEVIRTUAL
          && invoke.getOpcode() != Opcodes.INVOKESPECIAL
          && invoke.getOpcode() != Opcodes.INVOKEINTERFACE
          && invoke.getOpcode() != Opcodes.INVOKESTATIC) {
        continue;
      }
      final String name = invoke.name;
      if (name == null || name.isEmpty() || IGNORED_METHODS.contains(name)) {
        continue;
      }
      if (name.startsWith("get") && name.length() > 3) {
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

  private static SerializedLambda trySerializedLambda(final Object lambda) {
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
}
