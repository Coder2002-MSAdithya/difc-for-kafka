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
        REGISTRY.put(
                generatedLambdaClass,
                new LambdaInfo(
                        implClass,
                        implMethod,
                        implDesc));
    }

    public static LambdaInfo lookup(
            Class<?> lambdaClass)
    {
        return REGISTRY.get(
                lambdaClass.getName());
    }
}