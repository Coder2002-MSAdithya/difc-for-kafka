package org.apache.kafka.security.agent;

import net.bytebuddy.asm.Advice;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandleInfo;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;

public class LambdaMetafactoryAdvice
{
    @Advice.OnMethodExit
    public static void exit(
            @Advice.This Object self,

            @Advice.Argument(5)
            MethodHandle implMethod)
    {
        try
        {
            // ====================================================
            // Get generated lambda class name
            // ====================================================

            Field lambdaClassNameField =
                    self.getClass()
                            .getDeclaredField(
                                    "lambdaClassName");

            lambdaClassNameField.setAccessible(true);

            String generatedClass =
                    (String)
                            lambdaClassNameField.get(self);

            generatedClass =
                    generatedClass.replace('/', '.');

            // ====================================================
            // Recover implementation method
            // ====================================================

            MethodHandleInfo info =
                    MethodHandles.lookup()
                            .revealDirect(
                                    implMethod);

            String implClass =
                    info.getDeclaringClass()
                            .getName();

            String implMethodName =
                    info.getName();

            String implDesc =
                    implMethod.type()
                            .toMethodDescriptorString();

            // ====================================================
            // Register exact mapping
            // ====================================================

            LambdaRegistry.register(
                    generatedClass,
                    implClass,
                    implMethodName,
                    implDesc);
        }
        catch (Throwable ignored)
        {

        }
    }
}