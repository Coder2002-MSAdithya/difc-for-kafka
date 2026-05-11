package org.apache.kafka.security.agent.bootstrap.internal;

import java.net.InetSocketAddress;
import java.security.CodeSource;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.IdentityHashMap;
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

    private static final Set<String> ALLOWED_PREFIX =
            Set.of(
                    "org.apache.kafka.clients",
                    "org.apache.kafka.streams"
            );

    private static final String TRUSTED_CERT_PATH = System.getProperty("policy.agent.trusted.cert.path", "kafka-signing-cert.pem").trim();
    private static final X509Certificate TRUSTED_CERT = loadTrustedCertificate();

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
                System.out.println("[policy-agent] Detected Kafka client: " + typeStr);
                System.out.println("[POLICY] Active Kafka Client = " + activeClientType);
                return;
            }

            // Same object
            if (logicalClient == client)
            {
                return;
            }

            failStop("[POLICY] Multiple Kafka client instances/types detected. " + "Existing=" + activeClientType + ", New=" + type);
        }
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
            failStop("[POLICY] Unauthorized network access to " + addr);
        }
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
            // from JARs signed with the trusted certificate.
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

        for (Certificate cert : certs)
        {
            if (cert instanceof X509Certificate)
            {
                if (sameCertificate((X509Certificate) cert, TRUSTED_CERT))
                {
                    return true;
                }
            }
        }

        System.err.println("[POLICY][DEBUG] No trusted certificate match for CodeSource: " + source.getLocation()
                + ", certCount=" + certs.length);

        return false;
    }

    private static boolean sameCertificate(X509Certificate candidate, X509Certificate trusted)
    {
        try
        {
            return java.util.Arrays.equals(candidate.getEncoded(), trusted.getEncoded());
        }
        catch (Exception e)
        {
            throw new RuntimeException("Unable to compare X.509 certificates", e);
        }
    }

    private static X509Certificate loadTrustedCertificate()
    {
        if (TRUSTED_CERT_PATH.isEmpty())
        {
            failStop("[POLICY] Missing trusted certificate path. Set -Dpolicy.agent.trusted.cert.path=<path-to-kafka-signing-cert.pem>");
        }

        Path certPath = Paths.get(TRUSTED_CERT_PATH);

        try (InputStream in = Files.newInputStream(certPath))
        {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            return (X509Certificate) cf.generateCertificate(in);
        }
        catch (Exception e)
        {
            failStop("[POLICY] Unable to load trusted certificate from " + certPath + ": " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}