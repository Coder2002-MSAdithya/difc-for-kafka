package org.apache.kafka.security.agent;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.matcher.ElementMatchers;

import net.bytebuddy.dynamic.loading.ClassInjector;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.utility.RandomString;
import net.bytebuddy.dynamic.ClassFileLocator;

import java.io.File;
import java.lang.instrument.Instrumentation;
import java.nio.file.Files;
import java.util.Map;

public final class PolicyAgent {

    private PolicyAgent() {}

    public static void premain(String args, Instrumentation instrumentation) {

        System.err.println("[policy-agent] premain loaded. args=" + args);

        try {
            File temp = Files.createTempDirectory("bb-bootstrap").toFile();

            // ---- FORCE bootstrap injection ----
            ClassInjector injector =
                    ClassInjector.UsingInstrumentation.of(
                            temp,
                            ClassInjector.UsingInstrumentation.Target.BOOTSTRAP,
                            instrumentation
                    );

            byte[] bootstrapBytes =
                    ClassFileLocator.ForClassLoader.read(
                            org.apache.kafka.security.agent.bootstrap.SocketPolicyBootstrap.class
                    );

            injector.inject(
                    Map.of(
                            TypeDescription.ForLoadedType.of(
                                    org.apache.kafka.security.agent.bootstrap.SocketPolicyBootstrap.class
                            ),
                            bootstrapBytes
                    )
            );

            // ---- AgentBuilder ----
            AgentBuilder builder = new AgentBuilder.Default()
                    .with(new AgentBuilder.InjectionStrategy.UsingInstrumentation(instrumentation, temp))
                    .ignore(ElementMatchers.none())
                    .disableClassFormatChanges()
                    .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
                    .with(AgentBuilder.Listener.StreamWriting.toSystemError().withErrorsOnly());

            // ---- Socket enforcement ----
            builder = builder

                    // java.net.Socket
                    .type(ElementMatchers.named("java.net.Socket"))
                    .transform((b, td, cl, module, pd) ->
                            b.visit(Advice.to(SocketAdvice.SocketConnectAdvice.class)
                                    .on(ElementMatchers.named("connect")))
                    )

                    // SocketChannel (API layer)
                    .type(ElementMatchers.named("java.nio.channels.SocketChannel"))
                    .transform((b, td, cl, module, pd) ->
                            b.visit(Advice.to(SocketAdvice.SocketConnectAdvice.class)
                                            .on(ElementMatchers.named("connect")))
                                    .visit(Advice.to(SocketAdvice.SocketNoArgAdvice.class)
                                            .on(
                                                    ElementMatchers.named("finishConnect")
                                                            .or(ElementMatchers.named("open"))
                                            ))
                    )

                    // SocketChannelImpl (actual JDK implementation)
                    .type(ElementMatchers.named("sun.nio.ch.SocketChannelImpl"))
                    .transform((b, td, cl, module, pd) ->
                            b.visit(Advice.to(SocketAdvice.SocketConnectAdvice.class)
                                            .on(ElementMatchers.named("connect")))
                                    .visit(Advice.to(SocketAdvice.SocketNoArgAdvice.class)
                                            .on(ElementMatchers.named("finishConnect")))
                    );

            // ---- Kafka entrypoints ----
            builder = builder
                    .type(ElementMatchers.nameStartsWith("org.apache.kafka.clients.producer.KafkaProducer"))
                    .transform((b, td, cl, module, pd) ->
                            b.visit(Advice.to(SocketAdvice.KafkaEntrypointAdvice.class)
                                    .on(ElementMatchers.named("send")))
                    )
                    .type(ElementMatchers.nameStartsWith("org.apache.kafka.clients.consumer.KafkaConsumer"))
                    .transform((b, td, cl, module, pd) ->
                            b.visit(Advice.to(SocketAdvice.KafkaEntrypointAdvice.class)
                                    .on(ElementMatchers.named("poll")))
                    )
                    .type(ElementMatchers.nameStartsWith("org.apache.kafka.streams.KafkaStreams"))
                    .transform((b, td, cl, module, pd) ->
                            b.visit(Advice.to(SocketAdvice.KafkaEntrypointAdvice.class)
                                    .on(ElementMatchers.named("start")))
                    );

            builder.installOn(instrumentation);

            System.err.println("[policy-agent] socket policy instrumentation installed");

        } catch (Exception e) {
            throw new RuntimeException("[policy-agent] Failed to initialize agent", e);
        }
    }
}