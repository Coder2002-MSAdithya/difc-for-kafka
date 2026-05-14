package org.apache.kafka.security.agent;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Handle;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.io.InputStream;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.ProtectionDomain;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class StreamsDslAttestation
{
    private static final Set<String> USER_DSL_CALLSITES = ConcurrentHashMap.newKeySet();

    private StreamsDslAttestation()
    {

    }

    private static StackWalker.StackFrame findUserFrame()
    {
        StackWalker walker =
                StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);

        return walker.walk(stream ->

                stream.filter(f -> {

                            String cn =
                                    f.getClassName();

                            if (cn.startsWith(
                                    "org.apache.kafka.security.agent"))
                            {
                                return false;
                            }

                            if (cn.startsWith(
                                    "org.apache.kafka.streams.kstream.internals"))
                            {
                                return false;
                            }

                            if (cn.startsWith(
                                    "org.apache.kafka.streams.processor.internals"))
                            {
                                return false;
                            }

//                            if (cn.startsWith("org.apache.kafka.streams."))
//                            {
//                                return false;
//                            }

                            if (cn.startsWith(
                                    "java."))
                            {
                                return false;
                            }

                            if (cn.startsWith(
                                    "jdk."))
                            {
                                return false;
                            }

                            return true;
                        })

                        .findFirst()

                        .orElse(null));
    }

    public static boolean shouldEmitUserDsl(
            String operator)
    {
        try
        {
            StackWalker.StackFrame frame =
                    findUserFrame();

            if (frame == null)
            {
                return true;
            }

            String id =
                    operator
                            + "|"
                            + frame.getFileName()
                            + "|"
                            + frame.getLineNumber();

            return USER_DSL_CALLSITES
                    .add(id);
        }
        catch (Throwable ignored)
        {
            return true;
        }
    }

    public static void attest(
            String operator,
            Object function)
    {
        try
        {
            if (function == null)
            {
                return;
            }

            System.out.println(
                    "[POLICY][ATTEST] operator="
                            + operator);

            Class<?> lambdaClass =
                    function.getClass();

            LambdaRegistry.LambdaInfo info =
                    LambdaRegistry.lookup(
                            lambdaClass);

            if (info != null)
            {
                byte[] bytecode =
                        extractMethodBytecode(
                                info.implClass.replace('.', '/'),
                                info.implMethod,
                                info.implDesc);

                String hash =
                        sha256Base64(bytecode);

                System.out.println(
                        "[POLICY][ATTEST] impl.class="
                                + info.implClass);

                System.out.println(
                        "[POLICY][ATTEST] impl.method="
                                + info.implMethod);

                System.out.println(
                        "[POLICY][ATTEST] impl.desc="
                                + info.implDesc);

                System.out.println(
                        "[POLICY][ATTEST] bytecode.sha256="
                                + hash);

                System.out.println(
                        "[POLICY][ATTEST] topology.statement="
                                + operator
                                + "("
                                + info.implClass
                                + "::"
                                + info.implMethod
                                + ")");

                return;
            }

            System.out.println(
                    "[POLICY][ATTEST] lambda.class="
                            + lambdaClass.getName());

            ProtectionDomain pd =
                    lambdaClass.getProtectionDomain();

            if (pd != null &&
                    pd.getCodeSource() != null)
            {
                URL src =
                        pd.getCodeSource()
                                .getLocation();

                System.out.println(
                        "[POLICY][ATTEST] lambda.source="
                                + src);
            }

            SerializedLambda sl =
                    trySerializedLambda(function);

            if (sl != null)
            {
                attestSerializedLambda(
                        operator,
                        sl);

                return;
            }

        }
        catch (Throwable t)
        {
            t.printStackTrace();
        }
    }

    private static SerializedLambda
    trySerializedLambda(
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

    private static void attestSerializedLambda(
            String operator,
            SerializedLambda sl)
            throws Exception
    {
        System.out.println(
                "[POLICY][ATTEST] impl.class="
                        + sl.getImplClass());

        System.out.println(
                "[POLICY][ATTEST] impl.method="
                        + sl.getImplMethodName());

        System.out.println(
                "[POLICY][ATTEST] impl.signature="
                        + sl.getImplMethodSignature());

        byte[] bytecode =
                extractMethodBytecode(
                        sl.getImplClass(),
                        sl.getImplMethodName(),
                        sl.getImplMethodSignature());

        String hash =
                sha256Base64(bytecode);

        System.out.println(
                "[POLICY][ATTEST] bytecode.sha256="
                        + hash);

        System.out.println(
                "[POLICY][ATTEST] topology.statement="
                        + operator
                        + "("
                        + sl.getImplClass()
                        + "::"
                        + sl.getImplMethodName()
                        + ")");
    }

    private static byte[] extractMethodBytecode(
            String implClassName,
            String methodName,
            String methodDesc)
            throws Exception
    {
        String implClass =
                implClassName.replace('/', '.');

        Class<?> cls =
                Class.forName(implClass);

        String resource =
                "/"
                        + implClassName
                        + ".class";

        try (InputStream in =
                     cls.getResourceAsStream(resource))
        {
            ClassReader reader =
                    new ClassReader(in);

            ClassNode node =
                    new ClassNode();

            reader.accept(node, 0);

            List<MethodNode> methods =
                    node.methods;

            for (MethodNode mn : methods)
            {
                if (mn.name.equals(methodName)
                        &&
                        mn.desc.equals(methodDesc))
                {
                    return mn.instructions
                            .toString()
                            .getBytes(StandardCharsets.UTF_8);
                }
            }
        }

        throw new RuntimeException(
                "Lambda impl method not found");
    }

    private static String sha256Base64(
            byte[] data)
            throws Exception
    {
        MessageDigest md =
                MessageDigest.getInstance(
                        "SHA-256");

        byte[] digest =
                md.digest(data);

        return Base64.getEncoder()
                .encodeToString(digest);
    }
}