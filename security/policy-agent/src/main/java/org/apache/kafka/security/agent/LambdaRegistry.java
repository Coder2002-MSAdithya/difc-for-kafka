package org.apache.kafka.security.agent;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class LambdaRegistry
{
    public static final class LambdaInfo
    {
        public final String implClass;
        public final String implMethod;
        public final String implDesc;

        public LambdaInfo(
                String implClass,
                String implMethod,
                String implDesc)
        {
            this.implClass = implClass;
            this.implMethod = implMethod;
            this.implDesc = implDesc;
        }
    }

    private static final Map<String, LambdaInfo>
            REGISTRY =
            new ConcurrentHashMap<>();

    private LambdaRegistry()
    {

    }

    public static void register(
            String generatedLambdaClass,
            String implClass,
            String implMethod,
            String implDesc)
    {
        LambdaInfo info =
                new LambdaInfo(
                        implClass,
                        implMethod,
                        implDesc);

        // Register multiple aliases because generated lambda names can appear
        // in different forms across JVM internals and runtime class names:
        // - slash package form: a/b/C$$Lambda$1/0x...
        // - dot package form:   a.b.C$$Lambda$1/0x...
        // - normalized form:    a.b.C$$Lambda
        registerAlias(generatedLambdaClass, info);
        registerAlias(generatedLambdaClass.replace('/', '.'), info);
        registerAlias(generatedLambdaClass.replace('.', '/'), info);
    }

    private static void registerAlias(
            String key,
            LambdaInfo info)
    {
        REGISTRY.put(key, info);
        REGISTRY.put(normalizeLambdaName(key), info);
    }

    public static LambdaInfo lookup(
            Class<?> lambdaClass)
    {
        String runtimeName =
                lambdaClass.getName();

        LambdaInfo direct =
                REGISTRY.get(runtimeName);

        if (direct != null)
        {
            return direct;
        }

        String normalizedRuntime =
                normalizeLambdaName(runtimeName);

        for (Map.Entry<String, LambdaInfo> e : REGISTRY.entrySet())
        {
            if (normalizeLambdaName(e.getKey()).equals(normalizedRuntime))
            {
                return e.getValue();
            }
        }

        return null;
    }

    private static String normalizeLambdaName(
            String className)
    {
        String normalized =
                className;

        int hiddenIdx =
                normalized.indexOf("/0x");

        if (hiddenIdx >= 0)
        {
            normalized =
                    normalized.substring(0, hiddenIdx);
        }

        int lambdaPrefixIdx =
                normalized.indexOf("$$Lambda");

        if (lambdaPrefixIdx >= 0)
        {
            return normalized.substring(0, lambdaPrefixIdx + "$$Lambda".length());
        }

        return normalized;
    }
}