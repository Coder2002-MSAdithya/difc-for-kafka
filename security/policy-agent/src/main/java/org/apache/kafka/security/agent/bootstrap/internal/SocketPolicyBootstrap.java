package org.apache.kafka.security.agent.bootstrap.internal;

import java.net.InetSocketAddress;

public final class SocketPolicyBootstrap
{
    private static final ThreadLocal<Boolean> TRUSTED =
            ThreadLocal.withInitial(() -> false);

    private SocketPolicyBootstrap() {}

    private static volatile String MODE = "DEV";

    private static final java.util.Set<String> TRUSTED_HASHES =
            java.util.Set.of(
                    // 🔥 fill later
                    // "abc123..."
            );

    private static final java.util.Set<String> TRUSTED_CERT_FINGERPRINTS =
            java.util.Set.of(
                    // SHA-256 of your signing cert
                    // "ab12cd34..."
            );

    private static String sha256(byte[] data) throws java.security.NoSuchAlgorithmException
    {
        java.security.MessageDigest md =
                java.security.MessageDigest.getInstance("SHA-256");

        byte[] digest = md.digest(data);

        StringBuilder sb = new StringBuilder();
        for (byte b : digest) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private static boolean isSignatureTrusted(Class<?> cls)
    {
        try
        {
            var pd = cls.getProtectionDomain();
            if (pd == null) return false;

            var cs = pd.getCodeSource();
            if (cs == null) return false;

            java.security.cert.Certificate[] certs = cs.getCertificates();

            if (certs == null || certs.length == 0)
            {
                return false; // not signed
            }

            for (java.security.cert.Certificate cert : certs)
            {
                byte[] encoded = cert.getEncoded();
                String fingerprint = sha256(encoded);

                if (TRUSTED_CERT_FINGERPRINTS.contains(fingerprint))
                {
                    return true;
                }
            }

            return false;

        }
        catch (java.security.cert.CertificateEncodingException |
                 java.security.NoSuchAlgorithmException e)
        {
            return false;
        }
    }

    private static boolean isHashTrusted(String jarPath)
    {
        try
        {
            java.nio.file.Path path = java.nio.file.Paths.get(jarPath);

            byte[] bytes = java.nio.file.Files.readAllBytes(path);

            java.security.MessageDigest md =
                    java.security.MessageDigest.getInstance("SHA-256");

            byte[] digest = md.digest(bytes);

            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }

            return TRUSTED_HASHES.contains(sb.toString());

        }
        catch (java.io.IOException | java.security.NoSuchAlgorithmException e)
        {
            return false;
        }
    }

    public static void setMode(String mode)
    {
        MODE = mode;
    }

    public static boolean isDevMode()
    {
        return "DEV".equals(MODE);
    }

    public static boolean isProdMode()
    {
        return "PROD".equals(MODE);
    }

    public static void enterTrusted()
    {
        TRUSTED.set(true);
    }

    public static void exitTrusted()
    {
        TRUSTED.set(false);
    }

    public static boolean isTrusted()
    {
        return Boolean.TRUE.equals(TRUSTED.get());
    }

    public static void validate(Object endpoint)
    {
        if (!isTrusted())
        {
            // optional: still enforce caller restriction HERE
            if (!isAuthorizedCaller())
            {
                throw new SecurityException("[policy-agent] DENY: unauthorized Kafka caller");
            }
        }

        if (!isTrusted())
        {
            // ---- SPECIAL CASE: allow DNS (UDP port 53) ----
            if (endpoint instanceof InetSocketAddress)
            {
                int port = ((InetSocketAddress) endpoint).getPort();

                if (port == 53)
                {
                    return; // ✅ allow DNS
                }
            }

            throw new SecurityException("[policy-agent] DENY: socket connect outside trusted context");
        }

        if (endpoint instanceof InetSocketAddress)
        {
            int port = ((InetSocketAddress) endpoint).getPort();

            if (port < 9091 || port > 9099) {
                throw new SecurityException("[policy-agent] DENY: port out of allowed range: " + port);
            }
        }
    }

    private static boolean isSystemCaller()
    {
        try
        {
            return java.lang.StackWalker.getInstance(
                            java.lang.StackWalker.Option.RETAIN_CLASS_REFERENCE)
                    .walk(frames ->
                            frames
                                    .skip(2)
                                    .map(f -> f.getClassName())
                                    .anyMatch(name ->
                                            name.startsWith("java.") ||
                                                    name.startsWith("jdk.") ||
                                                    name.startsWith("sun.") ||
                                                    name.startsWith("javax.management") ||
                                                    name.startsWith("com.sun.")
                                    )
                    );
        }
        catch (Exception e)
        {
            return false;
        }
    }

    public static void validateBind(Object endpoint)
    {
        // ✅ Allow trusted context (Kafka, agent-marked)
        if (isTrusted())
        {
            return;
        }

        // ✅ Allow JVM / system classes
        if (isSystemCaller())
        {
            return;
        }

        throw new SecurityException("[policy-agent] DENY: listening sockets are not allowed");
    }

    private static boolean isAuthorizedClass(Class<?> cls)
    {
        try
        {
            var cs = cls.getProtectionDomain().getCodeSource();
            if (cs == null) return false;

            String path = cs.getLocation().getPath();

            // 🟡 DEV MODE
            if (isDevMode())
            {
                return path.contains("/build/libs/");
            }

            // 🔴 PROD MODE
            if (isProdMode())
            {
                return isSignatureTrusted(cls) || isHashTrusted(path);
            }

            return false;

        }
        catch(Exception e)
        {
            return false;
        }
    }

    private static boolean isAuthorizedCaller()
    {
        try
        {
            return java.lang.StackWalker.getInstance(
                            java.lang.StackWalker.Option.RETAIN_CLASS_REFERENCE)
                    .walk(frames ->
                            frames
                                    .skip(2) // skip validate() + advice
                                    .filter(f -> !f.getClassName().startsWith("org.apache.kafka"))
                                    .findFirst()
                                    .map(f -> isAuthorizedClass(f.getDeclaringClass()))
                                    .orElse(false)
                    );
        }
        catch (Exception e)
        {
            return false;
        }
    }
}