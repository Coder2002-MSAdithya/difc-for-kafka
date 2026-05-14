package org.apache.kafka.security.agent;

import net.bytebuddy.asm.Advice;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;

public class SocketAdvice
{
    private static volatile boolean initialized =
            false;

    public static Method registerClientMethod;

    public static Method checkSocketMethod;

    public static Method enterStreamsInternalMethod;

    public static Method exitStreamsInternalMethod;

    public static void init()
    {
        if (initialized)
        {
            return;
        }

        synchronized (SocketAdvice.class)
        {
            if (initialized)
            {
                return;
            }

            try
            {
                Class<?> bootstrapClass =
                        Class.forName(
                                "org.apache.kafka.security.agent.bootstrap.internal.SocketPolicyBootstrap");

                registerClientMethod =
                        bootstrapClass.getMethod(
                                "registerClient",
                                Object.class,
                                String.class);

                checkSocketMethod =
                        bootstrapClass.getMethod(
                                "checkSocketConnect",
                                InetSocketAddress.class);

                enterStreamsInternalMethod =
                        bootstrapClass.getMethod(
                                "enterStreamsInternal");

                exitStreamsInternalMethod =
                        bootstrapClass.getMethod(
                                "exitStreamsInternal");

                initialized = true;
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    // ============================================================
    // GENERIC CLIENT DETECTION
    // ============================================================

    public static class KafkaClientCtorAdvice
    {
        @Advice.OnMethodExit
        public static void exit(
                @Advice.This Object obj)
        {
            try
            {
                init();

                registerClientMethod.invoke(
                        null,
                        obj,
                        obj.getClass().getName());
            }
            catch (InvocationTargetException e)
            {
                Throwable cause =
                        e.getCause();

                if (cause instanceof RuntimeException)
                {
                    throw (RuntimeException) cause;
                }

                if (cause instanceof Error)
                {
                    throw (Error) cause;
                }

                throw new RuntimeException(cause);
            }
            catch (Exception e)
            {
                if (e instanceof RuntimeException)
                {
                    throw (RuntimeException) e;
                }

                throw new RuntimeException(e);
            }
        }
    }

    // ============================================================
    // STREAMS INTERNAL REGION
    // ============================================================

    public static class StreamsInternalRegionAdvice
    {
        @Advice.OnMethodEnter
        public static void enter()
        {
            try
            {
                init();

                enterStreamsInternalMethod.invoke(
                        null);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        @Advice.OnMethodExit(
                onThrowable = Throwable.class)
        public static void exit()
        {
            try
            {
                init();

                exitStreamsInternalMethod.invoke(
                        null);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    // ============================================================
    // STREAMS LOGICAL CLIENT
    // ============================================================

    public static class StreamsLogicalClientAdvice
    {
        @Advice.OnMethodEnter
        public static void enter(
                @Advice.This Object obj)
        {
            try
            {
                init();

                registerClientMethod.invoke(
                        null,
                        obj,
                        obj.getClass().getName());
            }
            catch (InvocationTargetException e)
            {
                Throwable cause =
                        e.getCause();

                if (cause instanceof RuntimeException)
                {
                    throw (RuntimeException) cause;
                }

                if (cause instanceof Error)
                {
                    throw (Error) cause;
                }

                throw new RuntimeException(cause);
            }
            catch (Exception e)
            {
                if (e instanceof RuntimeException)
                {
                    throw (RuntimeException) e;
                }

                throw new RuntimeException(e);
            }
        }
    }

    // ============================================================
    // STREAMS TOPOLOGY EXTRACTION
    // ============================================================

    public static class StreamsTopologyAdvice
    {
        @Advice.OnMethodExit
        public static void exit(
                @Advice.AllArguments Object[] args)
        {
            try
            {
                if (args == null ||
                        args.length == 0)
                {
                    return;
                }

                Object topology =
                        args[0];

                if (topology == null)
                {
                    return;
                }

                try
                {
                    Method describeMethod =
                            topology.getClass()
                                    .getMethod(
                                            "describe");

                    Object desc =
                            describeMethod.invoke(
                                    topology);

                    System.out.println(
                            "[POLICY] Kafka Streams DSL Topology:");

                    System.out.println(desc);

                    String canonicalTopology =
                            canonicalizeTopology(
                                    String.valueOf(desc));

                    String topologyDigest =
                            sha256Base64(
                                    canonicalTopology);

                    System.out.println(
                            "[POLICY][ATTEST] streams.topology.canonical="
                                    + canonicalTopology);

                    System.out.println(
                            "[POLICY][ATTEST] streams.topology.sha256="
                                    + topologyDigest);
                }
                catch (NoSuchMethodException ignored)
                {

                }
            }
            catch (Throwable t)
            {
                System.err.println(
                        "[policy-agent] Failed to print topology: "
                                + t.getMessage());
            }
        }
    }

    private static String canonicalizeTopology(
            String raw)
    {
        return Arrays.stream(
                        raw.split("\\R"))
                .map(String::trim)
                .filter(line -> !line.isEmpty())
                .reduce((a, b) -> a + "|" + b)
                .orElse("");
    }

    public static String sha256Base64(
            String input)
    {
        try
        {
            MessageDigest digest =
                    MessageDigest.getInstance(
                            "SHA-256");

            byte[] hash =
                    digest.digest(
                            input.getBytes(
                                    StandardCharsets.UTF_8));

            return java.util.Base64
                    .getEncoder()
                    .encodeToString(hash);
        }
        catch (Exception e)
        {
            throw new RuntimeException(
                    "Unable to hash topology for attestation",
                    e);
        }
    }

    // ============================================================
    // FORBID PROCESSOR API
    // ============================================================

    public static class ForbidProcessorApiAdvice
    {
        @Advice.OnMethodEnter
        public static void enter(
                @Advice.Origin String method)
        {
            System.err.println(
                    "[POLICY] Forbidden Kafka Streams Processor API usage: "
                            + method);

            Runtime.getRuntime().halt(1);
        }
    }

    // ============================================================
    // SOCKET CONNECT
    // ============================================================

    public static class SocketConnectAdvice
    {
        @Advice.OnMethodEnter
        public static void enter(
                @Advice.Argument(0)
                Object addr)
        {
            try
            {
                init();

                if (addr instanceof InetSocketAddress)
                {
                    checkSocketMethod.invoke(
                            null,
                            addr);
                }
            }
            catch (InvocationTargetException e)
            {
                Throwable cause =
                        e.getCause();

                if (cause instanceof RuntimeException)
                {
                    throw (RuntimeException) cause;
                }

                if (cause instanceof Error)
                {
                    throw (Error) cause;
                }

                throw new RuntimeException(cause);
            }
            catch (Exception e)
            {
                if (e instanceof RuntimeException)
                {
                    throw (RuntimeException) e;
                }

                throw new RuntimeException(e);
            }
        }
    }

    // ============================================================
    // SOCKET CHANNEL CONNECT
    // ============================================================

    public static class SocketChannelConnectAdvice
    {
        @Advice.OnMethodEnter
        public static void enter(
                @Advice.Argument(0)
                Object addr)
        {
            try
            {
                init();

                if (addr instanceof InetSocketAddress)
                {
                    checkSocketMethod.invoke(
                            null,
                            addr);
                }
            }
            catch (InvocationTargetException e)
            {
                Throwable cause =
                        e.getCause();

                if (cause instanceof RuntimeException)
                {
                    throw (RuntimeException) cause;
                }

                if (cause instanceof Error)
                {
                    throw (Error) cause;
                }

                throw new RuntimeException(cause);
            }
            catch (Exception e)
            {
                if (e instanceof RuntimeException)
                {
                    throw (RuntimeException) e;
                }

                throw new RuntimeException(e);
            }
        }
    }
}