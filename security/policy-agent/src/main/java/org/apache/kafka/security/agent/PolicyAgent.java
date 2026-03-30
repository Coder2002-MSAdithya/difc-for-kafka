package org.apache.kafka.security.agent;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.matcher.ElementMatchers;

import net.bytebuddy.dynamic.loading.ClassInjector;
import net.bytebuddy.description.type.TypeDescription;
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

            // ---- Inject bootstrap class ----
            ClassInjector injector =
                    ClassInjector.UsingInstrumentation.of(
                            temp,
                            ClassInjector.UsingInstrumentation.Target.BOOTSTRAP,
                            instrumentation
                    );

            Map<TypeDescription, byte[]> toInject = new java.util.HashMap<>();

            // ---- Bootstrap policy class ----
                        toInject.put(
                                TypeDescription.ForLoadedType.of(
                                        org.apache.kafka.security.agent.bootstrap.internal.SocketPolicyBootstrap.class
                                ),
                                ClassFileLocator.ForClassLoader.read(
                                        org.apache.kafka.security.agent.bootstrap.internal.SocketPolicyBootstrap.class
                                )
                        );

            // ---- 🔥 ALSO inject SocketAdvice ----
                        toInject.put(
                                TypeDescription.ForLoadedType.of(
                                        org.apache.kafka.security.agent.SocketAdvice.class
                                ),
                                ClassFileLocator.ForClassLoader.read(
                                        org.apache.kafka.security.agent.SocketAdvice.class
                                )
                        );

            injector.inject(toInject);

            // ---- AgentBuilder ----
            AgentBuilder builder = new AgentBuilder.Default()
                    .with(new AgentBuilder.InjectionStrategy.UsingInstrumentation(instrumentation, temp))
                    .ignore(ElementMatchers.none())
                    .disableClassFormatChanges()
                    .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
                    .with(AgentBuilder.Listener.StreamWriting.toSystemError().withErrorsOnly());

            // ================= SOCKET ENFORCEMENT =================

            builder = builder

                    // java.net.Socket
                    .type(ElementMatchers.named("java.net.Socket"))
                    .transform((b, td, cl, module, pd) ->
                            b.visit(Advice.to(SocketAdvice.SocketConnectAdvice.class)
                                    .on(ElementMatchers.named("connect")))
                    )

                    // SocketChannel
                    .type(ElementMatchers.named("java.nio.channels.SocketChannel"))
                    .transform((b, td, cl, module, pd) ->
                            b.visit(Advice.to(SocketAdvice.SocketConnectAdvice.class)
                                            .on(ElementMatchers.named("connect")))
                                    .visit(Advice.to(SocketAdvice.SocketNoArgAdvice.class)
                                            .on(ElementMatchers.named("finishConnect")
                                                    .or(ElementMatchers.named("open"))))
                    )

                    // SocketChannelImpl
                    .type(ElementMatchers.named("sun.nio.ch.SocketChannelImpl"))
                    .transform((b, td, cl, module, pd) ->
                            b.visit(Advice.to(SocketAdvice.SocketConnectAdvice.class)
                                            .on(ElementMatchers.named("connect")))
                                    .visit(Advice.to(SocketAdvice.SocketNoArgAdvice.class)
                                            .on(ElementMatchers.named("finishConnect")))
                    );

            // ================= KAFKA ENTRYPOINTS =================

            builder = builder
                    .type(ElementMatchers.nameStartsWith("org.apache.kafka.clients.producer.KafkaProducer"))
                    .transform((b, td, cl, module, pd) ->
                            b.visit(Advice.to(SocketAdvice.KafkaEntrypointAdvice.class)
                                    .on(ElementMatchers.named("send")))
                    );

            // ================= KAFKA NETWORK HOOK =================

            builder = builder
                    .type(ElementMatchers.named("org.apache.kafka.clients.NetworkClient"))
                    .transform((b, td, cl, module, pd) ->
                            b.visit(Advice.to(SocketAdvice.KafkaNetworkAdvice.class)
                                    .on(ElementMatchers.named("initiateConnect")
                                            .and(ElementMatchers.takesArguments(2))))
                    );

            // ================= UDP ENFORCEMENT =================
            builder = builder

                    // ---- java.net.DatagramSocket ----
                    .type(ElementMatchers.named("java.net.DatagramSocket"))
                    .transform((b, td, cl, module, pd) ->
                            b.visit(Advice.to(SocketAdvice.SocketConnectAdvice.class)
                                    .on(ElementMatchers.named("send")
                                            .or(ElementMatchers.named("connect"))))
                    )

                    // ---- DatagramChannel ----
                    .type(ElementMatchers.named("java.nio.channels.DatagramChannel"))
                    .transform((b, td, cl, module, pd) ->
                            b.visit(Advice.to(SocketAdvice.SocketConnectAdvice.class)
                                    .on(ElementMatchers.named("send")
                                            .or(ElementMatchers.named("connect"))))
                    )

                    // ---- Internal implementation ----
                    .type(ElementMatchers.named("sun.nio.ch.DatagramChannelImpl"))
                    .transform((b, td, cl, module, pd) ->
                            b.visit(Advice.to(SocketAdvice.SocketConnectAdvice.class)
                                    .on(ElementMatchers.named("send")
                                            .or(ElementMatchers.named("connect"))))
                    );

            builder.installOn(instrumentation);

            System.err.println("[policy-agent] socket policy instrumentation installed");

        } catch (Exception e) {
            throw new RuntimeException("[policy-agent] Failed to initialize agent", e);
        }
    }
}