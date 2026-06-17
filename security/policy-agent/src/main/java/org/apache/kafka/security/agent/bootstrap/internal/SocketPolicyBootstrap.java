package org.apache.kafka.security.agent.bootstrap.internal;

import java.net.InetSocketAddress;
import java.security.CodeSource;
import java.security.cert.Certificate;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.Set;
import java.lang.StackWalker;

public final class SocketPolicyBootstrap {

    public enum KafkaClientType {
        NONE,
        PRODUCER,
        CONSUMER,
        STREAMS
    }

    private static final ThreadLocal<Boolean> TRUSTED = ThreadLocal.withInitial(() -> false);

    // ============================================================
    // 🔥 STREAMS INTERNAL PROVENANCE
    // ============================================================

    private static final ThreadLocal<Boolean> STREAMS_INTERNAL = ThreadLocal.withInitial(() -> false);

    private static final Object CLIENT_LOCK = new Object();

    private static final Set<Object> SEEN_CLIENTS = Collections.newSetFromMap(new IdentityHashMap<>());

    private static volatile Object logicalClient = null;

    private static volatile KafkaClientType activeClientType = KafkaClientType.NONE;

    private static final Set<KafkaClientType> REGISTERED_CLIENT_TYPES = new LinkedHashSet<>();

    private static final Set<String> ALLOWED_PREFIX =
            Set.of(
                    "org.apache.kafka.clients",
                    "org.apache.kafka.streams"
            );

    private SocketPolicyBootstrap() {}

    // ============================================================
    // 🔐 TRUST CONTROL
    // ============================================================
    public static void enterTrusted()
    {
        TRUSTED.set(true);
    }

    public static void exitTrusted()
    {
        TRUSTED.set(false);
    }

    // ============================================================
    // 🔥 STREAMS INTERNAL CONTROL
    // ============================================================
    public static void enterStreamsInternal()
    {
        STREAMS_INTERNAL.set(true);
    }

    public static void exitStreamsInternal()
    {
        STREAMS_INTERNAL.set(false);
    }

    public static boolean insideStreamsInternal()
    {
        return Boolean.TRUE.equals(STREAMS_INTERNAL.get());
    }

    // ============================================================
    // 🔥 FAIL STOP
    // ============================================================

    private static void failStop(String msg)
    {

        System.err.println(msg);
        Runtime.getRuntime().halt(1);
    }

    // ============================================================
    // 🔥 CLIENT TRACKING
    // ============================================================

    public static void registerClient(Object client, String typeStr)
    {
        // Ignore Streams infrastructure clients
        if (insideStreamsInternal())
        {
            return;
        }

        KafkaClientType type = mapType(typeStr);

        synchronized (CLIENT_LOCK)
        {

            if (!SEEN_CLIENTS.add(client))
            {
                return;
            }

            // First logical client
            if (logicalClient == null)
            {
                logicalClient = client;
                activeClientType = type;
                REGISTERED_CLIENT_TYPES.add(type);
                System.out.println("[policy-agent] Detected Kafka client: " + typeStr);
                System.out.println("[POLICY] Active Kafka Client = " + activeClientType);
                return;
            }

            // Same object
            if (logicalClient == client)
            {
                return;
            }

            if (isAllowedAdditionalClientType(type))
            {
                REGISTERED_CLIENT_TYPES.add(type);
                System.out.println("[policy-agent] Additional Kafka client allowed: " + typeStr);
                return;
            }

            failStop("[POLICY] Multiple Kafka client instances/types detected. "
                    + "Registered=" + REGISTERED_CLIENT_TYPES + ", New=" + type);
        }
    }

    /**
     * Microservices such as OrdersService combine an app-level {@code KafkaProducer} (REST ingress)
     * with a {@code KafkaStreams} runtime in one JVM. Pipeline republishers (stock, validation, payment)
     * combine a {@code KafkaConsumer} with a transactional {@code KafkaProducer} in one JVM.
     */
    private static boolean isAllowedAdditionalClientType(final KafkaClientType type)
    {
        if (type == KafkaClientType.PRODUCER && REGISTERED_CLIENT_TYPES.contains(KafkaClientType.STREAMS))
        {
            return true;
        }
        if (type == KafkaClientType.STREAMS && REGISTERED_CLIENT_TYPES.contains(KafkaClientType.PRODUCER))
        {
            return true;
        }
        if (type == KafkaClientType.PRODUCER && REGISTERED_CLIENT_TYPES.contains(KafkaClientType.CONSUMER))
        {
            return true;
        }
        if (type == KafkaClientType.CONSUMER && REGISTERED_CLIENT_TYPES.contains(KafkaClientType.PRODUCER))
        {
            return true;
        }
        return false;
    }

    private static KafkaClientType mapType(String t)
    {
        if(t.contains("KafkaProducer"))
        {
            return KafkaClientType.PRODUCER;
        }

        if(t.contains("KafkaConsumer"))
        {
            return KafkaClientType.CONSUMER;
        }

        if(t.contains("KafkaStreams"))
        {
            return KafkaClientType.STREAMS;
        }

        return KafkaClientType.NONE;
    }

    // ============================================================
    // 🌐 NETWORK ENFORCEMENT
    // ============================================================

    public static void checkSocketConnect(InetSocketAddress addr)
    {

    if (Boolean.TRUE.equals(TRUSTED.get()))
    {
        return;
    }

    if (!isSocketConnectCausedByTrustedSignedKafkaApi())
    {
        recordExternalConnect(addr);
        if (isNetworkEnforcementEnabled() && !isExternalConnectAllowed(addr))
        {
            failStop("[POLICY] Unauthorized network access to " + addr
                    + " (declare via -Dpolicy.agent.allowed.external.hosts=host:port[,...])");
        }
        return;
    }
    }

    private static void recordExternalConnect(final InetSocketAddress addr) {
        invokeAgentStatic(
                "org.apache.kafka.security.agent.policy.ExternalConnectionTracker",
                "recordConnect",
                new Class<?>[] {InetSocketAddress.class},
                addr);
    }

    private static boolean isExternalConnectAllowed(final InetSocketAddress addr) {
        final Object result =
                invokeAgentStatic(
                        "org.apache.kafka.security.agent.policy.ExternalConnectionAllowlist",
                        "isAllowed",
                        new Class<?>[] {InetSocketAddress.class},
                        addr);
        return result instanceof Boolean && (Boolean) result;
    }

    private static Object invokeAgentStatic(
            final String className,
            final String methodName,
            final Class<?>[] paramTypes,
            final Object... args) {
        try {
            final Class<?> type = Class.forName(className, true, agentPolicyClassLoader());
            return type.getMethod(methodName, paramTypes).invoke(null, args);
        } catch (ReflectiveOperationException ignored) {
            return null;
        }
    }

    private static ClassLoader agentPolicyClassLoader() {
        try {
            return Class.forName("org.apache.kafka.security.agent.PolicyAgent")
                    .getClassLoader();
        } catch (ClassNotFoundException ignored) {
            return Thread.currentThread().getContextClassLoader();
        }
    }

    /** Exposed for {@link org.apache.kafka.security.agent.policy.ExternalConnectionTracker}. */
    public static boolean isTrustedKafkaConnectStack()
    {
        return isSocketConnectCausedByTrustedSignedKafkaApi();
    }

    private static boolean isSocketConnectCausedByTrustedSignedKafkaApi()
    {
        boolean foundKafkaApiFrame = false;

        for (Class<?> frameClass : StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE)
                .walk(stream -> stream.map(StackWalker.StackFrame::getDeclaringClass).toList()))
        {
            String className = frameClass.getName();
            if (!isKafkaClass(className))
            {
                continue;
            }

            foundKafkaApiFrame = true;

            // The Kafka API classes on the stack that trigger a socket connect must come
            // from JARs signed by a trusted certificate (mkcert CA chain or optional leaf pin).
            try
            {
                CodeSource source = frameClass.getProtectionDomain().getCodeSource();
                if (!isSignedByTrustedCertificate(source))
                {
                    System.err.println("[POLICY][DEBUG] Kafka API class rejected: " + className +
                            ", codeSource=" + (source == null ? "null" : source.getLocation()));
                    return false;
                }
            }
            catch (Throwable ignored)
            {
                System.err.println("[POLICY][DEBUG] Failed to signature-check Kafka API class: " + className);
                return false;
            }
        }

        return foundKafkaApiFrame;
    }

    private static boolean isKafkaClass(String className)
    {
        for (String prefix : ALLOWED_PREFIX)
        {
            if (className.startsWith(prefix))
            {
                return true;
            }
        }

        return false;
    }

    private static boolean isNetworkEnforcementEnabled()
    {
        return Boolean.parseBoolean(System.getProperty("policy.agent.network.enforcement", "true"));
    }

    private static boolean isSignedByTrustedCertificate(CodeSource source)
    {
        // Null source means we cannot establish provenance/signer information for the class,
        // so this must be treated as untrusted for fail-closed behavior.
        if (source == null)
        {
            System.err.println("[POLICY][DEBUG] Class has null CodeSource");
            return false;
        }

        // Certificates are populated for signed code sources once classes are verified/loaded.
        // Absence of certs means this class did not come from a trusted signed artifact.
        Certificate[] certs = source.getCertificates();
        if (certs == null)
        {
            System.err.println("[POLICY][DEBUG] CodeSource has no certificates: " + source.getLocation());
            return false;
        }

        if (!PolicyCertificateTrust.isTrustedSignerChain(certs))
        {
            System.err.println("[POLICY][DEBUG] Untrusted signer chain for CodeSource: " + source.getLocation()
                    + ", certCount=" + certs.length);
            return false;
        }

        return true;
    }
}