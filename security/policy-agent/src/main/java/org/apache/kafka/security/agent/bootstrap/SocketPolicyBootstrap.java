package org.apache.kafka.security.agent.bootstrap;

import java.net.InetSocketAddress;

public final class SocketPolicyBootstrap {

    private static final ThreadLocal<Boolean> TRUSTED =
            ThreadLocal.withInitial(() -> false);

    private SocketPolicyBootstrap() {}

    // ---- trusted context ----
    public static void enterTrusted() {
        TRUSTED.set(true);
    }

    public static void exitTrusted() {
        TRUSTED.set(false);
    }

    private static boolean hasTrustedCallStack() {
        return StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE)
                .walk(frames ->
                        frames.map(StackWalker.StackFrame::getDeclaringClass)
                                .anyMatch(c ->
                                        c.getName().startsWith("org.apache.kafka.clients.producer.KafkaProducer") ||
                                                c.getName().startsWith("org.apache.kafka.clients.consumer.KafkaConsumer") ||
                                                c.getName().startsWith("org.apache.kafka.streams.KafkaStreams")
                                )
                );
    }

    // ---- enforcement ----
    public static void validate(Object endpoint)
    {
        if (!Boolean.TRUE.equals(TRUSTED.get())) {
            throw new SecurityException(
                    "[policy-agent] DENY: socket connect outside Kafka trusted context"
            );
        }

        // Optional: only check port if endpoint exists
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