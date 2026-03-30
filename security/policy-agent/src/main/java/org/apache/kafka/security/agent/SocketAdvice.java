package org.apache.kafka.security.agent;

import net.bytebuddy.asm.Advice;
import java.net.SocketAddress;
import org.apache.kafka.security.agent.bootstrap.SocketPolicyBootstrap;

public class SocketAdvice {

    // ---- methods WITH arguments ----
    public static class SocketConnectAdvice {

        @Advice.OnMethodEnter
        public static void onEnter(@Advice.Argument(0) SocketAddress endpoint) {
            SocketPolicyBootstrap.validate(endpoint);
        }
    }

    // ---- methods WITHOUT arguments ----
    public static class SocketNoArgAdvice {

        @Advice.OnMethodEnter
        public static void onEnter() {
            SocketPolicyBootstrap.validate(null);
        }
    }

    // ---- Kafka entrypoint marking ----
    public static class KafkaEntrypointAdvice {

        @Advice.OnMethodEnter
        public static void enter() {
            SocketPolicyBootstrap.enterTrusted();
        }

        @Advice.OnMethodExit(onThrowable = Throwable.class)
        public static void exit() {
            SocketPolicyBootstrap.exitTrusted();
        }
    }
}