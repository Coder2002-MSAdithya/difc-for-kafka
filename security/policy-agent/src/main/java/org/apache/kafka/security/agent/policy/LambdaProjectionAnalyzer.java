package org.apache.kafka.security.agent.policy;

import org.objectweb.asm.tree.MethodNode;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Set;

/**
 * Infers mapValues/map π output columns and selectKey π_k field references from callback bytecode,
 * runtime apply(), or constructor/builder output types.
 */
public final class LambdaProjectionAnalyzer {

  private LambdaProjectionAnalyzer() {
  }

  static void resetOrdinalTrackingForTests() {
    CallbackOrdinalTracker.resetForTests();
  }

  public static Set<String> analyzeMapper(final Object mapper) {
    if (mapper == null) {
      return Set.of();
    }
    final ClassLoader classLoader = mapper.getClass().getClassLoader();
    final LambdaBytecodeInspector.ResolvedCallbackMethod resolved =
        LambdaBytecodeInspector.resolveCallbackMethod(mapper);
    if (resolved != null) {
      try {
        final Set<String> fromImpl =
            LambdaBytecodeInspector.analyzeMethodProjection(
                resolved.ownerClass(), resolved.methodName(), resolved.methodDesc(), classLoader);
        if (!fromImpl.isEmpty()) {
          return fromImpl;
        }
      } catch (final Exception ignored) {
        // fall through
      }
    }
    final Set<String> fromApply = analyzeMapperViaApply(mapper);
    if (!fromApply.isEmpty()) {
      return fromApply;
    }
    final Set<String> fromMethodRefs =
        LambdaBytecodeInspector.analyzeFromCapturingClassReferences(
            mapper,
            target -> {
              if (target.name().startsWith("lambda$")) {
                return Set.of();
              }
              try {
                return LambdaBytecodeInspector.analyzeMethodProjection(
                    target.owner(), target.name(), target.desc(), classLoader);
              } catch (final Exception ignored) {
                return Set.of();
              }
            });
    if (!fromMethodRefs.isEmpty()) {
      return fromMethodRefs;
    }
    final MethodNode apply = LambdaBytecodeInspector.findFunctionalMethodNode(mapper.getClass());
    if (apply != null) {
      final Set<String> fromBytecode = LambdaBytecodeInspector.extractProjectionFields(apply, classLoader);
      if (!fromBytecode.isEmpty()) {
        return fromBytecode;
      }
    }
    return LambdaBytecodeInspector.analyzeFromCapturingClassReferences(
        mapper,
        target -> {
          if (!target.name().startsWith("lambda$")) {
            return Set.of();
          }
          try {
            return LambdaBytecodeInspector.analyzeMethodProjection(
                target.owner(), target.name(), target.desc(), classLoader);
          } catch (final Exception ignored) {
            return Set.of();
          }
        });
  }

  public static Set<String> analyzeKeyMapper(final Object mapper) {
    if (mapper == null) {
      return Set.of();
    }
    final MethodNode apply = LambdaBytecodeInspector.findFunctionalMethodNode(mapper.getClass());
    if (apply != null) {
      final Set<String> keyFields = LambdaBytecodeInspector.extractKeyFieldReferences(apply);
      if (!keyFields.isEmpty()) {
        return keyFields;
      }
    }
    final LambdaBytecodeInspector.ResolvedCallbackMethod resolved =
        LambdaBytecodeInspector.resolveCallbackMethod(mapper);
    if (resolved != null) {
      try {
        return LambdaBytecodeInspector.analyzeMethodSelection(
            resolved.ownerClass(), resolved.methodName(), resolved.methodDesc(), mapper.getClass().getClassLoader());
      } catch (final Exception ignored) {
        return Set.of();
      }
    }
    return Set.of();
  }

  static Set<String> analyzeMethod(
      final String implClassName, final String methodName, final String methodDesc) throws Exception {
    return LambdaBytecodeInspector.analyzeMethodProjection(
        implClassName, methodName, methodDesc, Thread.currentThread().getContextClassLoader());
  }

  private static Set<String> analyzeMapperViaApply(final Object mapper) {
    Class<?> inputType = extractMapperInputType(mapper);
    Object sampleInput = safeCreateSampleValue(inputType);
    if (sampleInput == null) {
      final String capturingClassName = LambdaBytecodeInspector.capturingClassName(mapper.getClass());
      if (capturingClassName != null) {
        for (final LambdaBytecodeInspector.MethodReferenceTarget target :
            LambdaBytecodeInspector.listUnaryMethodReferences(
                capturingClassName, mapper.getClass().getClassLoader())) {
          final Class<?> candidate =
              LambdaBytecodeInspector.parameterTypeFromDesc(target.desc(), mapper.getClass().getClassLoader());
          sampleInput = safeCreateSampleValue(candidate);
          if (sampleInput != null) {
            break;
          }
        }
      }
    }
    if (sampleInput == null) {
      return Set.of();
    }
    try {
      final Method apply = findApplyMethod(mapper.getClass());
      if (apply == null) {
        return Set.of();
      }
      final Object output = apply.invoke(mapper, sampleInput);
      if (output == null) {
        return Set.of();
      }
      final Set<String> populated = RecordFieldExtractor.extractPopulatedFields(output);
      if (!populated.isEmpty()) {
        return populated;
      }
      return RecordFieldExtractor.extractFields(output);
    } catch (final ReflectiveOperationException | RuntimeException ignored) {
      return Set.of();
    }
  }

  private static Method findApplyMethod(final Class<?> mapperClass) {
    for (final String name : new String[] {"apply", "test", "get"}) {
      for (final Method method : mapperClass.getMethods()) {
        if (name.equals(method.getName()) && method.getParameterCount() == 1) {
          method.setAccessible(true);
          return method;
        }
      }
    }
    return null;
  }

  private static Class<?> extractMapperInputType(final Object mapper) {
    final Class<?> fromGenerics = extractFunctionInputType(mapper.getClass());
    if (fromGenerics != null && fromGenerics != Object.class) {
      return fromGenerics;
    }
    final Class<?> fromApply = extractApplyParameterType(mapper.getClass());
    if (fromApply != null && fromApply != Object.class) {
      return fromApply;
    }
    return extractApplyParameterTypeFromBytecode(mapper.getClass());
  }

  private static Class<?> extractApplyParameterType(final Class<?> mapperClass) {
    for (final Method method : mapperClass.getDeclaredMethods()) {
      if (!"apply".equals(method.getName()) || method.getParameterCount() != 1) {
        continue;
      }
      if (method.isBridge() || method.isSynthetic()) {
        continue;
      }
      final Class<?> param = method.getParameterTypes()[0];
      if (param != Object.class) {
        return param;
      }
    }
    for (final Method method : mapperClass.getMethods()) {
      if (!"apply".equals(method.getName()) || method.getParameterCount() != 1) {
        continue;
      }
      final Class<?> param = method.getParameterTypes()[0];
      if (param != Object.class) {
        return param;
      }
    }
    return null;
  }

  private static Class<?> extractApplyParameterTypeFromBytecode(final Class<?> mapperClass) {
    final MethodNode apply = LambdaBytecodeInspector.findFunctionalMethodNode(mapperClass);
    if (apply == null || apply.desc == null || apply.desc.indexOf(')') < 2) {
      return null;
    }
    final String paramDesc = apply.desc.substring(1, apply.desc.indexOf(')'));
    if (paramDesc.startsWith("L") && paramDesc.endsWith(";")) {
      return LambdaBytecodeInspector.loadClass(
          paramDesc.substring(1, paramDesc.length() - 1), mapperClass.getClassLoader());
    }
    return null;
  }

  private static Class<?> extractFunctionInputType(final Class<?> lambdaClass) {
    for (final Type type : lambdaClass.getGenericInterfaces()) {
      final Class<?> resolved = resolveFunctionInput(type);
      if (resolved != null) {
        return resolved;
      }
    }
    return null;
  }

  private static Class<?> resolveFunctionInput(final Type type) {
    if (!(type instanceof ParameterizedType parameterized)) {
      return null;
    }
    final Type raw = parameterized.getRawType();
    if (!(raw instanceof Class<?> rawClass)
        || !java.util.function.Function.class.isAssignableFrom(rawClass)) {
      return null;
    }
    final Type[] args = parameterized.getActualTypeArguments();
    if (args.length < 1) {
      return null;
    }
    return toClass(args[0]);
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

  private static Object safeCreateSampleValue(final Class<?> type) {
    if (type == null || type == Object.class) {
      return null;
    }
    try {
      return createSampleValue(type);
    } catch (final RuntimeException ignored) {
      return null;
    }
  }

  private static Object createSampleValue(final Class<?> type) {
    if (type == String.class) {
      return "sample";
    }
    if (type.isEnum()) {
      final Object[] constants = type.getEnumConstants();
      return constants != null && constants.length > 0 ? constants[0] : null;
    }
    if (type == int.class || type == Integer.class) {
      return 1;
    }
    if (type == long.class || type == Long.class) {
      return 1L;
    }
    if (type == boolean.class || type == Boolean.class) {
      return false;
    }
    if (type == double.class || type == Double.class) {
      return 1.0d;
    }
    if (type == float.class || type == Float.class) {
      return 1.0f;
    }
    try {
      final Object fromBuilder = tryBuilderSample(type);
      if (fromBuilder != null) {
        return fromBuilder;
      }
      if (type.getPackageName().startsWith("java.")) {
        return null;
      }
      final Object instance = type.getDeclaredConstructor().newInstance();
      populateSampleFields(instance);
      return instance;
    } catch (final ReflectiveOperationException ignored) {
      return null;
    }
  }

  private static Object tryBuilderSample(final Class<?> type) {
    try {
      final Method builderMethod = type.getMethod("builder");
      final Object builder = builderMethod.invoke(null);
      for (final Method method : builder.getClass().getMethods()) {
        if (method.getParameterCount() != 1 || method.getReturnType() != builder.getClass()) {
          continue;
        }
        final Class<?> param = method.getParameterTypes()[0];
        if (param == String.class) {
          method.invoke(builder, "sample");
        } else if (param == int.class || param == Integer.class) {
          method.invoke(builder, 1);
        } else if (param == long.class || param == Long.class) {
          method.invoke(builder, 1L);
        } else if (param.isEnum()) {
          final Object[] constants = param.getEnumConstants();
          if (constants != null && constants.length > 0) {
            method.invoke(builder, constants[0]);
          }
        }
      }
      final Method build = builder.getClass().getMethod("build");
      return build.invoke(builder);
    } catch (final ReflectiveOperationException ignored) {
      return null;
    }
  }

  private static void populateSampleFields(final Object instance) {
    for (final java.lang.reflect.Field field : instance.getClass().getDeclaredFields()) {
      if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
        continue;
      }
      if (!field.trySetAccessible()) {
        continue;
      }
      try {
        if (field.getType() == String.class) {
          field.set(instance, "sample");
        } else if (field.getType() == int.class) {
          field.setInt(instance, 1);
        } else if (field.getType() == long.class || field.getType() == Long.class) {
          field.set(instance, 1L);
        } else if (field.getType().isEnum()) {
          final Object[] constants = field.getType().getEnumConstants();
          if (constants != null && constants.length > 0) {
            field.set(instance, constants[0]);
          }
        }
      } catch (final IllegalAccessException ignored) {
        // best effort
      }
    }
  }
}
