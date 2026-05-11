package org.apache.kafka.security.agent;

import net.bytebuddy.agent.builder.AgentBuilder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.lang.instrument.Instrumentation;
import java.nio.file.Files;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;

import static net.bytebuddy.matcher.ElementMatchers.*;

public class PolicyAgent
{

    public static void premain(String agentArgs, Instrumentation inst)
    {
        System.out.println("[policy-agent] premain loaded");

        try
        {

            File tempJar = Files.createTempFile("policy-agent-bootstrap", ".jar").toFile();

            try(JarOutputStream jos = new JarOutputStream(new FileOutputStream(tempJar)))
            {
                addClassToJar(jos, SocketAdvice.class);
                addClassToJar(jos, SocketAdvice.KafkaClientCtorAdvice.class);
                addClassToJar(jos, SocketAdvice.StreamsInternalRegionAdvice.class);
                addClassToJar(jos, SocketAdvice.StreamsLogicalClientAdvice.class);
                addClassToJar(jos, SocketAdvice.StreamsTopologyAdvice.class);
                addClassToJar(jos, SocketAdvice.ForbidProcessorApiAdvice.class);
                addClassToJar(jos, SocketAdvice.SocketConnectAdvice.class);
                addClassToJar(jos, SocketAdvice.SocketChannelConnectAdvice.class);
                addClassToJar(jos, org.apache.kafka.security.agent.bootstrap.internal.SocketPolicyBootstrap.class);
                addClassToJar(jos, org.apache.kafka.security.agent.bootstrap.internal.SocketPolicyBootstrap.KafkaClientType.class);
            }

            inst.appendToBootstrapClassLoaderSearch(new JarFile(tempJar));

            AgentBuilder agentBuilder = new AgentBuilder.Default().ignore(
                    nameStartsWith("net.bytebuddy.")
                            .or(nameStartsWith("sun.reflect"))
                            .or(nameStartsWith("jdk.internal.reflect"))
            );

            // ========================================================
            // KafkaProducer
            // ========================================================
            agentBuilder =
                    agentBuilder.type(named("org.apache.kafka.clients.producer.KafkaProducer"))
                            .transform((b, td, cl, m, pd) ->
                                    b.visit(net.bytebuddy.asm.Advice.to(SocketAdvice.KafkaClientCtorAdvice.class).on(isConstructor())
                                    )
                            );

            // ========================================================
            // KafkaConsumer
            // ========================================================
            agentBuilder = agentBuilder.type(named("org.apache.kafka.clients.consumer.KafkaConsumer"))
                    .transform((b, td, cl, m, pd) ->
                            b.visit(net.bytebuddy.asm.Advice.to(SocketAdvice.KafkaClientCtorAdvice.class).on(isConstructor()))
                    );

            // ========================================================
            // KafkaStreams runtime boundary
            // ========================================================
            agentBuilder = agentBuilder.type(named("org.apache.kafka.streams.KafkaStreams"))
                    .transform((b, td, cl, m, pd) ->

                            b
                                    // STREAMS logical authority
                                    .visit(
                                            net.bytebuddy.asm.Advice.to(
                                                            SocketAdvice
                                                                    .StreamsLogicalClientAdvice.class
                                                    )
                                                    .on(named("start"))
                                    )
                                    // STREAMS topology extraction
                                    .visit(
                                            net.bytebuddy.asm.Advice.to(
                                                            SocketAdvice
                                                                    .StreamsTopologyAdvice.class
                                                    )
                                                    .on(isConstructor())
                                    )
                                    // STREAMS internal runtime region
                                    .visit(
                                            net.bytebuddy.asm.Advice.to(
                                                            SocketAdvice
                                                                    .StreamsInternalRegionAdvice.class
                                                    )
                                                    .on(named("start"))
                                    )
                    );

            // ========================================================
            // Streams internal client supplier
            // ========================================================

            agentBuilder = agentBuilder.type(named("org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier"))
                    .transform((b, td, cl, m, pd) ->
                            b.visit(
                                    net.bytebuddy.asm.Advice.to(
                                                    SocketAdvice.StreamsInternalRegionAdvice.class
                                            )
                                            .on(named("getProducer")
                                                    .or(named("getConsumer"))
                                                    .or(named("getRestoreConsumer"))
                                                    .or(named("getGlobalConsumer"))
                                            )
                            )
                    );

            // ========================================================
            // ❌ FORBID PROCESSOR API
            // ========================================================
            agentBuilder = agentBuilder.type(named("org.apache.kafka.streams.Topology"))
                    .transform((b, td, cl, m, pd) ->
                            b.visit(net.bytebuddy.asm.Advice.to(SocketAdvice.ForbidProcessorApiAdvice.class)
                                    .on(named("addProcessor").or(named("addSource")).or(named("addSink")))
                            )
                    );

            // ========================================================
            // java.net.Socket
            // ========================================================
            agentBuilder = agentBuilder.type(named("java.net.Socket"))
                    .transform((b, td, cl, m, pd) ->
                            b.visit(net.bytebuddy.asm.Advice.to(SocketAdvice.SocketConnectAdvice.class).on(named("connect")))
                    );

            // ========================================================
            // SocketChannel (concrete JDK implementations)
            // ========================================================
            agentBuilder = agentBuilder.type(named("java.nio.channels.SocketChannel"))
                    .transform((b, td, cl, m, pd) ->
                            b.visit(net.bytebuddy.asm.Advice.to(SocketAdvice.SocketChannelConnectAdvice.class).on(named("connect")))
                    );

            agentBuilder = agentBuilder.type(named("sun.nio.ch.SocketChannelImpl"))
                    .transform((b, td, cl, m, pd) ->
                            b.visit(net.bytebuddy.asm.Advice.to(SocketAdvice.SocketChannelConnectAdvice.class).on(named("connect")))
                    );

            agentBuilder.installOn(inst);

        }
        catch(Exception e)
        {
            e.printStackTrace();
            Runtime.getRuntime().halt(1);
        }
    }

    private static void addClassToJar(JarOutputStream jos, Class<?> clazz) throws Exception
    {
        String classFile = clazz.getName().replace('.', '/') + ".class";
        jos.putNextEntry(new JarEntry(classFile));

        try(InputStream is = clazz.getClassLoader().getResourceAsStream(classFile))
        {
            if (is == null)
            {
                throw new RuntimeException("Class not found: " + classFile);
            }

            is.transferTo(jos);
        }

        jos.closeEntry();
    }
}