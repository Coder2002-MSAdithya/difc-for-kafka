package org.apache.kafka.security.agent;

import java.lang.instrument.Instrumentation;

public final class PolicyAgent {
    public static void premain(String args, Instrumentation inst) {
        System.setProperty("policy.agent.loaded", "true");
        // TODO: register transformers/interceptors (Socket.connect, SocketChannel.connect, etc.)
    }
}