package org.apache.kafka.security.agent;

import net.bytebuddy.asm.Advice;

public class SocketAdvice {

    private static Class<?> bootstrapClass;

    public static Class<?> getBootstrapClass() {
        if (bootstrapClass == null) {
            try {
                bootstrapClass = Class.forName(
                        "org.apache.kafka.security.agent.bootstrap.internal.SocketPolicyBootstrap",
                        true,
                        null // 🔥 bootstrap classloader
                );
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return bootstrapClass;
    }

    public static void validate(Object endpoint) {
        try {
            getBootstrapClass()
                    .getMethod("validate", Object.class)
                    .invoke(null, endpoint);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void enterTrusted() {
        try {
            getBootstrapClass()
                    .getMethod("enterTrusted")
                    .invoke(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ---- methods WITH arguments ----
    public static class SocketConnectAdvice {

        @Advice.OnMethodEnter
        public static void onEnter(@Advice.AllArguments Object[] args) {
            Object endpoint = (args != null && args.length > 0) ? args[0] : null;
            validate(endpoint);
        }
    }

    // ---- methods WITHOUT arguments ----
    public static class SocketNoArgAdvice {

        @Advice.OnMethodEnter
        public static void onEnter() {
            validate(null);
        }
    }

    // ---- Kafka entrypoint marking ----
    public static class KafkaEntrypointAdvice {

        @Advice.OnMethodEnter
        public static void enter() {
            enterTrusted();
        }

        @Advice.OnMethodExit(onThrowable = Throwable.class)
        public static void exit() {
            // no-op
        }
    }

    // ---- Kafka network hook (CRITICAL) ----
    public static class KafkaNetworkAdvice {

        @Advice.OnMethodEnter
        public static void enter() {
            System.err.println("[policy-agent] ENTER NetworkClient.initiateConnect");
            enterTrusted();
        }
    }
}