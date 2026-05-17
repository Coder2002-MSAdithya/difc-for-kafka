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
            @Advice.AllArguments Object[] args)
    {
        try
        {
            MethodHandle implMethod =
                    null;

            if (args != null)
            {
                for (Object arg : args)
                {
                    if (arg instanceof MethodHandle mh)
                    {
                        implMethod = mh;
                        break;
                    }
                }
            }

            if (implMethod == null)
            {
                return;
            }

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