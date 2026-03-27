package org.apache.kafka.security.agent;

import java.lang.instrument.Instrumentation;

public final class PolicyAgent {
    public static void premain(String args, Instrumentation inst) {
        System.out.println("[policy-agent] premain loaded. args=" + args);
        System.setProperty("policy.agent.loaded", "true");
    }
}
