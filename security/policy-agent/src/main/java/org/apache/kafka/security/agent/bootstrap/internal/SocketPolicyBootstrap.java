package org.apache.kafka.security.agent.bootstrap.internal;

import java.net.InetSocketAddress;
import java.security.CodeSource;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

public final class SocketPolicyBootstrap {

    public enum KafkaClientType {
        NONE,
        PRODUCER,
        CONSUMER,
        STREAMS
    }

    private static final ThreadLocal<Boolean> TRUSTED =
            ThreadLocal.withInitial(() -> false);

    // ============================================================
    // 🔥 STREAMS INTERNAL PROVENANCE
    // ============================================================

    private static final ThreadLocal<Boolean> STREAMS_INTERNAL =
            ThreadLocal.withInitial(() -> false);

    private static final Object CLIENT_LOCK = new Object();

    private static final Set<Object> SEEN_CLIENTS =
            Collections.newSetFromMap(new IdentityHashMap<>());

    private static volatile Object logicalClient = null;

    private static volatile KafkaClientType activeClientType =
            KafkaClientType.NONE;

    private static final Set<String> ALLOWED_PREFIX =
            Set.of(
                    "org.apache.kafka.clients",
                    "org.apache.kafka.streams"
            );

    private static final Set<String> ALLOWED_JARS =
            Set.of(
                    "kafka-clients",
                    "kafka-streams"
            );

    private SocketPolicyBootstrap() {}

    // ============================================================
    // 🔐 TRUST CONTROL
    // ============================================================

    public static void enterTrusted() {
        TRUSTED.set(true);
    }

    public static void exitTrusted() {
        TRUSTED.set(false);
    }

    // ============================================================
    // 🔥 STREAMS INTERNAL CONTROL
    // ============================================================

    public static void enterStreamsInternal() {
        STREAMS_INTERNAL.set(true);
    }

    public static void exitStreamsInternal() {
        STREAMS_INTERNAL.set(false);
    }

    public static boolean insideStreamsInternal() {
        return Boolean.TRUE.equals(STREAMS_INTERNAL.get());
    }

    // ============================================================
    // 🔥 FAIL STOP
    // ============================================================

    private static void failStop(String msg) {

        System.err.println(msg);

        Runtime.getRuntime().halt(1);
    }

    // ============================================================
    // 🔥 CLIENT TRACKING
    // ============================================================

    public static void registerClient(
            Object client,
            String typeStr) {

        // Ignore Streams infrastructure clients
        if (insideStreamsInternal()) {
            return;
        }

        KafkaClientType type = mapType(typeStr);

        synchronized (CLIENT_LOCK) {

            if (!SEEN_CLIENTS.add(client)) {
                return;
            }

            // First logical client
            if (logicalClient == null) {

                logicalClient = client;
                activeClientType = type;

                System.out.println(
                        "[policy-agent] Detected Kafka client: "
                                + typeStr);

                System.out.println(
                        "[POLICY] Active Kafka Client = "
                                + activeClientType);

                return;
            }

            // Same object
            if (logicalClient == client) {
                return;
            }

            failStop(
                    "[POLICY] Multiple Kafka client instances/types detected. "
                            + "Existing=" + activeClientType
                            + ", New=" + type
            );
        }
    }

    private static KafkaClientType mapType(String t) {

        if (t.contains("KafkaProducer")) {
            return KafkaClientType.PRODUCER;
        }

        if (t.contains("KafkaConsumer")) {
            return KafkaClientType.CONSUMER;
        }

        if (t.contains("KafkaStreams")) {
            return KafkaClientType.STREAMS;
        }

        return KafkaClientType.NONE;
    }

    // ============================================================
    // 🌐 NETWORK ENFORCEMENT
    // ============================================================

    public static void checkSocketConnect(
            InetSocketAddress addr) {

        if (Boolean.TRUE.equals(TRUSTED.get())) {
            return;
        }

        boolean allowedStack = false;
        boolean allowedJar = false;

        for (StackTraceElement e :
                Thread.currentThread().getStackTrace()) {

            String cls = e.getClassName();

            for (String prefix : ALLOWED_PREFIX) {

                if (cls.startsWith(prefix)) {

                    allowedStack = true;

                    try {

                        Class<?> c = Class.forName(cls);

                        CodeSource src =
                                c.getProtectionDomain()
                                        .getCodeSource();

                        if (src != null) {

                            String path =
                                    src.getLocation().toString();

                            for (String jar : ALLOWED_JARS) {

                                if (path.contains(jar)) {
                                    allowedJar = true;
                                }
                            }
                        }

                    } catch (Throwable ignored) {
                    }
                }
            }
        }

        if (!allowedStack || !allowedJar) {

            failStop(
                    "[POLICY] Unauthorized network access to "
                            + addr
            );
        }
    }
}