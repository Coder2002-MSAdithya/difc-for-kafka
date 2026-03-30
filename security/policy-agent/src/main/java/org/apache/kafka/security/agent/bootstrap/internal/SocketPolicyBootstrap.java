package org.apache.kafka.security.agent.bootstrap.internal;

import java.net.InetSocketAddress;

public final class SocketPolicyBootstrap {

    private static final ThreadLocal<Boolean> TRUSTED =
            ThreadLocal.withInitial(() -> false);

    private SocketPolicyBootstrap() {}

    public static void enterTrusted() {
        TRUSTED.set(true);
    }

    public static void exitTrusted() {
        TRUSTED.set(false);
    }

    public static boolean isTrusted() {
        return Boolean.TRUE.equals(TRUSTED.get());
    }

    public static void validate(Object endpoint) {

        if (!isTrusted()) {
            throw new SecurityException(
                    "[policy-agent] DENY: socket connect outside trusted context"
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