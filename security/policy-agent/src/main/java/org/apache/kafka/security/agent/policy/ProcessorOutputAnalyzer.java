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
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.LinkedHashSet;
import java.util.Set;

/** Infers processor π output columns from Processor generics and process() bytecode. */
public final class ProcessorOutputAnalyzer {

  private static final Set<String> IGNORED_METHODS =
      Set.of(
          "build",
          "builder",
          "newBuilder",
          "init",
          "close",
          "schedule",
          "forward",
          "get",
          "put",
          "delete",
          "range",
          "all",
          "flush",
          "apply",
          "run",
          "invoke",
          "requireNonNull",
          "equals",
          "hashCode",
          "toString",
          "getClass",
          "getStateStore",
          "withKey",
          "withValue",
          "withTimestamp");

  private ProcessorOutputAnalyzer() {
  }

  public static boolean isUserProvidedProcessorSupplier(final Object processorSupplier) {
    if (processorSupplier == null) {
      return false;
    }
    if (processorSupplier.getClass().getName().startsWith("org.apache.kafka.streams.")) {
      return false;
    }
    final SerializedLambda lambda = trySerializedLambda(processorSupplier);
    if (lambda != null) {
      return !lambda.getImplClass().startsWith("org/apache/kafka/streams");
    }
    if (isGeneratedLambdaClass(processorSupplier.getClass())) {
      return isUserApplicationJar(processorSupplier.getClass());
    }
    return true;
  }

  private static boolean isGeneratedLambdaClass(final Class<?> type) {
    return type.isSynthetic() || type.getName().contains("$$Lambda");
  }

  private static boolean isUserApplicationJar(final Class<?> type) {
    try {
      final java.net.URL location = type.getProtectionDomain().getCodeSource().getLocation();
      if (location == null) {
        return true;
      }
      final String path = location.toString();
      if (path.contains("org/apache/kafka/streams/")) {
        return false;
      }
      return !isOfficialKafkaStreamsLibraryJar(path);
    } catch (final SecurityException ignored) {
      return true;
    }
  }

  private static boolean isOfficialKafkaStreamsLibraryJar(final String path) {
    return path.matches(".*[/\\\\]kafka-streams-[0-9][^/\\\\]*\\.jar(?:$|[?#]).*");
  }

  public static Set<String> analyzeProcessorSupplier(final Object processorSupplier) {
    if (processorSupplier == null || !isUserProvidedProcessorSupplier(processorSupplier)) {
      return Set.of();
    }
    final Class<?> processorClass = resolveProcessorClass(processorSupplier);
    if (processorClass == null) {
      return Set.of();
    }
    final Set<String> fromGenerics = outputFieldsFromProcessorClass(processorClass);
    if (!fromGenerics.isEmpty()) {
      return fromGenerics;
    }
    return analyzeProcessMethod(processorClass);
  }

  private static Class<?> resolveProcessorClass(final Object processorSupplier) {
    final LambdaRegistry.LambdaInfo registryInfo = LambdaRegistry.lookup(processorSupplier.getClass());
    if (registryInfo != null) {
      try {
        return Class.forName(registryInfo.implClass.replace('/', '.'));
      } catch (final ClassNotFoundException ignored) {
        return null;
      }
    }
    final SerializedLambda lambda = trySerializedLambda(processorSupplier);
    if (lambda != null) {
      try {
        return Class.forName(lambda.getImplClass().replace('/', '.'));
      } catch (final ClassNotFoundException ignored) {
        return null;
      }
    }
    final Class<?> fromGet = resolveProcessorClassFromSupplierGet(processorSupplier);
    if (fromGet != null) {
      return fromGet;
    }
    return resolveProcessorClassFromSupplierBytecode(processorSupplier.getClass());
  }

  private static Class<?> resolveProcessorClassFromSupplierGet(final Object processorSupplier) {
    if (processorSupplier instanceof java.util.function.Supplier<?> supplier) {
      try {
        final Object processor = supplier.get();
        if (processor != null) {
          return processor.getClass();
        }
      } catch (final RuntimeException ignored) {
        return null;
      }
    }
    try {
      final Method get = processorSupplier.getClass().getMethod("get");
      final Object processor = get.invoke(processorSupplier);
      if (processor != null) {
        return processor.getClass();
      }
    } catch (final ReflectiveOperationException ignored) {
      return null;
    }
    return null;
  }

  private static Class<?> resolveProcessorClassFromSupplierBytecode(final Class<?> supplierClass) {
    try (InputStream in = supplierClass.getResourceAsStream("/" + supplierClass.getName().replace('.', '/') + ".class")) {
      if (in == null) {
        return null;
      }
      final ClassReader reader = new ClassReader(in);
      final ClassNode node = new ClassNode();
      reader.accept(node, 0);
      for (final MethodNode method : node.methods) {
        if (!"get".equals(method.name) || method.desc == null) {
          continue;
        }
        for (AbstractInsnNode insn = method.instructions.getFirst(); insn != null; insn = insn.getNext()) {
          if (insn instanceof MethodInsnNode invoke
              && Opcodes.INVOKESPECIAL == invoke.getOpcode()
              && "<init>".equals(invoke.name)) {
            final Class<?> candidate = loadProcessorCandidate(invoke.owner);
            if (candidate != null) {
              return candidate;
            }
          }
          if (insn.getOpcode() == Opcodes.NEW) {
            final Class<?> candidate = loadProcessorCandidate(
                org.objectweb.asm.Type.getObjectType(((org.objectweb.asm.tree.TypeInsnNode) insn).desc).getClassName());
            if (candidate != null) {
              return candidate;
            }
          }
        }
      }
    } catch (final Exception ignored) {
      return null;
    }
    return null;
  }

  private static Class<?> loadProcessorCandidate(final String internalOrBinaryName) {
    if (internalOrBinaryName == null || internalOrBinaryName.startsWith("org/apache/kafka/streams")) {
      return null;
    }
    try {
      final Class<?> candidate = Class.forName(internalOrBinaryName.replace('/', '.'));
      if (implementsProcessorApi(candidate)) {
        return candidate;
      }
    } catch (final ClassNotFoundException ignored) {
      return null;
    }
    return null;
  }

  private static boolean implementsProcessorApi(final Class<?> type) {
    for (final Class<?> iface : type.getInterfaces()) {
      final String name = iface.getName();
      if (name.startsWith("org.apache.kafka.streams.")
          && (name.endsWith(".Processor") || name.endsWith(".ProcessorSupplier"))) {
        return true;
      }
    }
    return type.getSuperclass() != null
        && type.getSuperclass() != Object.class
        && implementsProcessorApi(type.getSuperclass());
  }

  static Set<String> outputFieldsFromProcessorClass(final Class<?> processorClass) {
    final Class<?> valueOut = extractProcessorOutputValueType(processorClass);
    if (valueOut == null || valueOut == Object.class || valueOut == Void.class) {
      return Set.of();
    }
    try {
      final Object sample = valueOut.getDeclaredConstructor().newInstance();
      final Set<String> fields = RecordFieldExtractor.extractFields(sample);
      if (!fields.isEmpty()) {
        return fields;
      }
    } catch (final ReflectiveOperationException ignored) {
      // Avro SpecificRecord types may not have a no-arg ctor; fall through to type/schema scan.
    }
    final Set<String> fromType = RecordFieldExtractor.extractFieldsFromType(valueOut);
    if (!fromType.isEmpty()) {
      return fromType;
    }
    return analyzeProcessMethod(processorClass);
  }

  static Class<?> extractProcessorOutputValueType(final Class<?> processorClass) {
    for (final Type type : processorClass.getGenericInterfaces()) {
      final Class<?> resolved = resolveProcessorValueOut(type);
      if (resolved != null) {
        return resolved;
      }
    }
    final Type superType = processorClass.getGenericSuperclass();
    return resolveProcessorValueOut(superType);
  }

  private static Class<?> resolveProcessorValueOut(final Type type) {
    if (!(type instanceof ParameterizedType parameterized)) {
      return null;
    }
    final Type raw = parameterized.getRawType();
    if (!(raw instanceof Class<?> rawClass)) {
      return null;
    }
    final String name = rawClass.getName();
    if (!name.endsWith("Processor") && !name.endsWith("ProcessorSupplier")) {
      return null;
    }
    final Type[] args = parameterized.getActualTypeArguments();
    if (args.length < 4) {
      return null;
    }
    return toClass(args[3]);
  }

  private static Class<?> toClass(final Type type) {
    if (type instanceof Class<?> cls) {
      return cls;
    }
    if (type instanceof ParameterizedType parameterized && parameterized.getRawType() instanceof Class<?> cls) {
      return cls;
    }
    return null;
  }

  static Set<String> analyzeProcessMethod(final Class<?> processorClass) {
    final MethodNode process = LambdaBytecodeInspector.findMethodNode(processorClass, "process", null);
    if (process != null) {
      final Set<String> fromBytecode =
          LambdaBytecodeInspector.extractProjectionFields(process, processorClass.getClassLoader());
      if (!fromBytecode.isEmpty()) {
        return fromBytecode;
      }
    }
    try (InputStream in = processorClass.getResourceAsStream("/" + processorClass.getName().replace('.', '/') + ".class")) {
      if (in == null) {
        return Set.of();
      }
      final ClassReader reader = new ClassReader(in);
      final ClassNode node = new ClassNode();
      reader.accept(node, 0);
      final LinkedHashSet<String> fields = new LinkedHashSet<>();
      for (final MethodNode method : node.methods) {
        if (!"process".equals(method.name)) {
          continue;
        }
        fields.addAll(LambdaBytecodeInspector.extractProjectionFields(method, processorClass.getClassLoader()));
      }
      return fields;
    } catch (final Exception e) {
      return Set.of();
    }
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
