package org.apache.kafka.security.agent.bootstrap;

import java.net.InetSocketAddress;

public final class SocketPolicyBootstrap {

    // ✅ NO anonymous class → no $1 class generated
    private static final InheritableThreadLocal<Boolean> TRUSTED =
            new InheritableThreadLocal<>();

    private SocketPolicyBootstrap() {}

    public static void enterTrusted() {
        TRUSTED.set(true);
    }

    public static void exitTrusted() {
        TRUSTED.set(false);
    }

    private static boolean isKafkaInternalCall() {
        return StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE)
                .walk(frames ->
                        frames.map(StackWalker.StackFrame::getDeclaringClass)
                                .anyMatch(c ->
                                        c.getName().startsWith("org.apache.kafka.clients")
                                )
                );
    }

    public static void validate(Object endpoint) {

        boolean trusted = Boolean.TRUE.equals(TRUSTED.get());

        if (!trusted && !isKafkaInternalCall()) {
            throw new SecurityException(
                    "[policy-agent] DENY: socket connect outside Kafka trusted context"
            );
        }

        if (endpoint instanceof InetSocketAddress) {
            int port = ((InetSocketAddress) endpoint).getPort();

            if (port < 9091 || port > 9099) {
                throw new SecurityException(
                        "[policy-agent] DENY: port out of allowed range: " + port
                );
            }
        }
    }
}