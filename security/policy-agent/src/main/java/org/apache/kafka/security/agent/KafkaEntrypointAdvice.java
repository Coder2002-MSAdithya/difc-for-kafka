package org.apache.kafka.security.agent;

import net.bytebuddy.asm.Advice;
import org.apache.kafka.security.agent.bootstrap.SocketPolicyBootstrap;

public class KafkaEntrypointAdvice {

    @Advice.OnMethodEnter
    public static void enter() {
        SocketPolicyBootstrap.enterTrusted();
    }

    @Advice.OnMethodExit(onThrowable = Throwable.class)
    public static void exit() {
        SocketPolicyBootstrap.exitTrusted();
    }
}
