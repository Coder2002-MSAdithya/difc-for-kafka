package org.apache.kafka.security.agent;

import net.bytebuddy.asm.Advice;

import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;

public class LambdaObjectCtorAdvice
{
    @Advice.OnMethodExit
    public static void exit(
            @Advice.This Object obj)
    {
        try
        {
            SerializedLambda sl =
                    extractSerializedLambda(obj);

            if (sl == null)
            {
                return;
            }

            LambdaRegistry.register(
                    obj.getClass().getName(),
                    sl.getImplClass().replace('/', '.'),
                    sl.getImplMethodName(),
                    sl.getImplMethodSignature());

            System.out.println(
                    "[POLICY][LAMBDA-REGISTER] "
                            + obj.getClass().getName()
                            + " -> "
                            + sl.getImplClass()
                            + "::"
                            + sl.getImplMethodName());
        }
        catch (Throwable t)
        {
            System.out.println(
                    "[POLICY][LAMBDA-REGISTER-FAIL] "
                            + t);

            t.printStackTrace(System.out);
        }
    }

    private static SerializedLambda extractSerializedLambda(
            Object lambda)
    {
        try
        {
            Method m =
                    lambda.getClass()
                            .getDeclaredMethod(
                                    "writeReplace");

            m.setAccessible(true);

            Object replacement =
                    m.invoke(lambda);

            if (replacement instanceof SerializedLambda sl)
            {
                return sl;
            }
        }
        catch (Throwable ignored)
        {

        }

        return null;
    }
}