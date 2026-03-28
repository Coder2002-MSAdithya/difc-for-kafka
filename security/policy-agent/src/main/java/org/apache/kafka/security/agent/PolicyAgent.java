package org.apache.kafka.security.agent;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.matcher.ElementMatchers;

import java.io.IOException;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.lang.instrument.Instrumentation;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarFile;

public final class PolicyAgent {
    private static final int MIN_ALLOWED_PORT = 9091;
    private static final int MAX_ALLOWED_PORT = 9099;
    /**
     * Hard-coded MD5 whitelist of trusted JAR contents.
     * Populate this set with hashes generated from build/libs/*.jar.
     */
    private static final Set<String> TRUSTED_JAR_HASHES = Set.of(
            "03c5514d835c07a2819d95391b148c92",
            "079608517ca6dabf413d1df613623ad1",
            "15cb9cbf0d410cf5cd0e31dfb3d33b5b",
            "1c0cc33ca5ca80075ec8e38a4d9a3243",
            "230add60707d94fbea9d8e7cdc874390",
            "28d16e2b34887faa68f04835c1660047",
            "33afcdcfbbcebc32702f2fa59686ecf9",
            "35c929f6cc7de79ca66ea7651941376f",
            "41d3fb0558cc022867b09d2c7721992e",
            "462800212d4e4679966fd883c09be629",
            "47ca0cc633306508398368e6b68a62c2",
            "4ac34b4cda7f321e139c87dc30800eba",
            "4f064ac9744ac1034da9591906f3085a",
            "505bccbd619598ec38f3d15f67c5603a",
            "5e017c116821a00476729bfb008708f7",
            "5e8b67912d6d4e4cbf161013d713db8f",
            "61adb8091c90f2e9d964b0aaf0251076",
            "6248265b6e0aaa6a91bea6996b92de10",
            "6a1b1effa35ae198181118253f8b0fa2",
            "6eaa50be20aab1e16f992828b7e5714f",
            "71b942b858ad0d49123c847183a4ae97",
            "720609775780460a0d25b3ce2abae84b",
            "81fd45269b79567d9ed28f0f5595fcee",
            "82c72453cc4d7a95ed6e3c39fc461ee1",
            "8699af2b8ffdef460847d06d9acb5091",
            "8c8d4c069d7ec5b6f26e7d6484443bc9",
            "a3e750823c5a754c3ee81e1a39ab5764",
            "b1e6cf469ee08b901b4b21fd12706d74",
            "be24d74d6f934e9a300b287083090b14",
            "beeebc352f7e06be0d2c1a8c2ef08181",
            "ce2a7e8a1210562cb3258f9b981eaa42",
            "ce7006dd5844d25b463ae34155026435",
            "d4a5a10122504e17910a8bc7981a1655",
            "d8baa3fe02f288ebb1123a6348c41bb7",
            "de01d9bfb8d0a2494a19db4a82aa5bf2",
            "e5980f96bd7eea8d6325835418a7ac9b",
            "eba8e1698e0b7143395e98cc463febf1",
            "f04b0d3cf14a63b59372ad6b259b86be",
            "f2fdd343ad1040c633bda5daada0d4e4",
            "f544cd140735d3f6c330ccc3f3975dbc",
            "f794daf2926e1ae2dd4f76b1682ef75c",
            "fa6ddb1e52de2cf5489e8054b3827fa9"
    );
    
    private static final String KAFKA_CLASS_PREFIX = "org.apache.kafka.";
    private static final Set<String> TRUSTED_SOCKET_ENTRYPOINT_PREFIXES = Set.of(
            "org.apache.kafka.clients.producer.KafkaProducer",
            "org.apache.kafka.clients.consumer.KafkaConsumer",
            "org.apache.kafka.streams.KafkaStreams",
            "org.apache.kafka.connect.connector.Connector"
    );
    private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);
    private static final ThreadLocal<Boolean> GUARD = ThreadLocal.withInitial(() -> false);
    private static final Map<Path, String> JAR_HASH_CACHE = new ConcurrentHashMap<>();

    private PolicyAgent() {
    }

    public static void premain(String args, Instrumentation instrumentation) {
        if (!INITIALIZED.compareAndSet(false, true)) {
            return;
        }

        System.err.println("[policy-agent] premain loaded. args=" + args);
        System.setProperty("policy.agent.loaded", "true");
        validateWhitelistConfigured();
        instrumentation.addTransformer(new TrustedClassSourceTransformer(), false);

        AgentBuilder builder = new AgentBuilder.Default()
                .ignore(ElementMatchers.none())
                .disableClassFormatChanges()
                .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
                .with(AgentBuilder.Listener.StreamWriting.toSystemError().withErrorsOnly());

        builder = builder
                .type(ElementMatchers.named("java.net.Socket"))
                .transform((b, td, cl, module, pd) ->
                        b.visit(Advice.to(SocketConnectAdvice.class)
                                .on(ElementMatchers.named("connect")
                                        .and(ElementMatchers.takesArguments(SocketAddress.class, int.class)))))
                .type(ElementMatchers.named("java.nio.channels.SocketChannel"))
                .transform((b, td, cl, module, pd) ->
                        b.visit(Advice.to(ChannelConnectAdvice.class)
                                .on(ElementMatchers.named("connect")
                                        .and(ElementMatchers.takesArguments(SocketAddress.class)))))
                .type(ElementMatchers.named("java.nio.channels.AsynchronousSocketChannel"))
                .transform((b, td, cl, module, pd) ->
                        b.visit(Advice.to(AsyncChannelConnectAdvice.class)
                                .on(ElementMatchers.named("connect")
                                        .and(ElementMatchers.takesArguments(SocketAddress.class)))))
                .type(ElementMatchers.named("java.net.DatagramSocket"))
                .transform((b, td, cl, module, pd) ->
                        b.visit(Advice.to(DatagramDenyAdvice.class)
                                .on(ElementMatchers.named("connect")
                                        .and(ElementMatchers.takesArguments(SocketAddress.class)))
                        ).visit(Advice.to(DatagramDenyAdvice.class)
                                .on(ElementMatchers.named("send"))));

        builder.installOn(instrumentation);
        appendAgentJarToBootstrapSearch(instrumentation);
        System.err.println("[policy-agent] socket connections are allowed only when call path stays in trusted jars");
        System.err.println("[policy-agent] socket call path must include trusted API entrypoint prefixes="
                + TRUSTED_SOCKET_ENTRYPOINT_PREFIXES);
        System.err.println("[policy-agent] socket policy instrumentation installed");
    }

    private static void validateConnection(String api, Object endpoint) {
        if (Boolean.TRUE.equals(GUARD.get())) {
            return;
        }

        GUARD.set(true);
        try {
            validateEndpointRestrictions(api, endpoint);
            Class<?> trustedCaller = findTrustedCallerInStack();
            Class<?> trustedEntrypoint = findTrustedEntrypointInStack();
            if (trustedCaller != null && trustedEntrypoint != null) {
                System.err.println("[policy-agent] ALLOW socket connect via " + api
                        + ", endpoint=" + endpoint
                        + ", trusted-caller=" + trustedCaller.getName()
                        + ", trusted-entrypoint=" + trustedEntrypoint.getName());
                return;
            }

            throw new SecurityException("[policy-agent] DENY socket connect via " + api
                    + ", endpoint=" + endpoint
                    + ", missing trusted call-path and/or approved entrypoint");
        } finally {
            GUARD.set(false);
        }
    }

    private static void validateEndpointRestrictions(String api, Object endpoint) {
        if (!(endpoint instanceof InetSocketAddress)) {
            throw new SecurityException("[policy-agent] DENY socket connect via " + api
                    + ", unsupported endpoint type=" + endpoint);
        }

        InetSocketAddress inet = (InetSocketAddress) endpoint;
        int port = inet.getPort();
        if (port < MIN_ALLOWED_PORT || port > MAX_ALLOWED_PORT) {
            throw new SecurityException("[policy-agent] DENY socket connect via " + api
                    + ", endpoint=" + endpoint + ", port must be in [" + MIN_ALLOWED_PORT + "," + MAX_ALLOWED_PORT + "]");
        }
    }

    private static void validateWhitelistConfigured() {
        System.err.println("[policy-agent] trusted hard-coded jar hash count=" + TRUSTED_JAR_HASHES.size());
    }

    private static void appendAgentJarToBootstrapSearch(Instrumentation instrumentation) {
        try {
            URI locationUri = PolicyAgent.class.getProtectionDomain().getCodeSource().getLocation().toURI();
            Path agentJar = Paths.get(locationUri).normalize().toAbsolutePath();
            if (!isJarPath(agentJar)) {
                throw new IllegalStateException("[policy-agent] agent location is not a jar: " + agentJar);
            }
            instrumentation.appendToBootstrapClassLoaderSearch(new JarFile(agentJar.toFile()));
            System.err.println("[policy-agent] appended agent jar to bootstrap classpath: " + agentJar);
        } catch (Exception e) {
            throw new IllegalStateException("[policy-agent] failed to append agent jar to bootstrap classpath", e);
        }
    }

    private static Class<?> findTrustedCallerInStack() {
        return StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE)
                .walk(frames -> frames
                        .map(StackWalker.StackFrame::getDeclaringClass)
                        .filter(PolicyAgent::isTrustedRuntimeClass)
                        .findFirst()
                        .orElse(null));
    }

    private static Class<?> findTrustedEntrypointInStack() {
        return StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE)
                .walk(frames -> frames
                        .map(StackWalker.StackFrame::getDeclaringClass)
                        .filter(Objects::nonNull)
                        .filter(klass -> TRUSTED_SOCKET_ENTRYPOINT_PREFIXES.stream()
                                .anyMatch(prefix -> {
                                    String name = klass.getName();
                                    return name.equals(prefix) || name.startsWith(prefix + "$");
                                }))
                        .filter(PolicyAgent::isTrustedRuntimeClass)
                        .findFirst()
                        .orElse(null));
    }

    private static boolean isTrustedRuntimeClass(Class<?> klass) {
        String className = klass.getName();
        if (!className.startsWith(KAFKA_CLASS_PREFIX)) {
            return false;
        }

        java.security.ProtectionDomain protectionDomain = klass.getProtectionDomain();
        if (protectionDomain == null || protectionDomain.getCodeSource() == null || protectionDomain.getCodeSource().getLocation() == null) {
            return false;
        }

        URI uri;
        try {
            uri = protectionDomain.getCodeSource().getLocation().toURI();
        } catch (Exception e) {
            return false;
        }

        if (!"file".equalsIgnoreCase(uri.getScheme())) {
            return false;
        }

        Path sourcePath = Paths.get(uri).normalize().toAbsolutePath();
        if (Files.isDirectory(sourcePath) || !isJarPath(sourcePath)) {
            return false;
        }

        String hash = JAR_HASH_CACHE.computeIfAbsent(sourcePath, PolicyAgent::md5Hex);
        return TRUSTED_JAR_HASHES.contains(hash);
    }

    private static void validateLoadedClassSource(String className, java.security.ProtectionDomain protectionDomain)
            throws IllegalClassFormatException {
        if (className == null || !className.replace('/', '.').startsWith(KAFKA_CLASS_PREFIX)) {
            // Third-party application classes are allowed; we only verify Kafka runtime classes are loaded
            // from trusted Kafka jars.
            return;
        }

        if (protectionDomain == null || protectionDomain.getCodeSource() == null || protectionDomain.getCodeSource().getLocation() == null) {
            return;
        }

        URI uri;
        try {
            uri = protectionDomain.getCodeSource().getLocation().toURI();
        } catch (Exception e) {
            throw new IllegalClassFormatException("[policy-agent] DENY class load " + className
                    + ", invalid code source URI: " + protectionDomain.getCodeSource().getLocation());
        }

        if (!"file".equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalClassFormatException("[policy-agent] DENY class load " + className
                    + ", non-file code source: " + uri);
        }

        Path sourcePath = Paths.get(uri).normalize().toAbsolutePath();
        if (Files.isDirectory(sourcePath)) {
            throw new IllegalClassFormatException("[policy-agent] DENY class load " + className
                    + ", classes must be loaded from trusted jars only. source=" + sourcePath);
        }

        if (!isJarPath(sourcePath)) {
            throw new IllegalClassFormatException("[policy-agent] DENY class load " + className
                    + ", source is not a jar: " + sourcePath);
        }

        String hash = JAR_HASH_CACHE.computeIfAbsent(sourcePath, PolicyAgent::md5Hex);
        if (!TRUSTED_JAR_HASHES.contains(hash)) {
            throw new IllegalClassFormatException("[policy-agent] DENY class load " + className
                    + ", jar hash not trusted: " + sourcePath);
        }
    }

    private static boolean isJarPath(Path sourcePath) {
        Path fileName = sourcePath.getFileName();
        return fileName != null && fileName.toString().endsWith(".jar");
    }

    private static String md5Hex(Path file) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] bytes = Files.readAllBytes(file);
            byte[] hash = digest.digest(bytes);
            StringBuilder sb = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException | IOException e) {
            throw new IllegalStateException("[policy-agent] Failed to hash jar: " + file, e);
        }
    }

    public static class SocketConnectAdvice {
        @Advice.OnMethodEnter
        public static void onEnter(@Advice.Argument(0) SocketAddress endpoint) {
            validateConnection("java.net.Socket#connect", endpoint);
        }
    }

    public static class ChannelConnectAdvice {
        @Advice.OnMethodEnter
        public static void onEnter(@Advice.Argument(0) SocketAddress endpoint) {
            validateConnection(SocketChannel.class.getName() + "#connect", endpoint);
        }
    }

    public static class AsyncChannelConnectAdvice {
        @Advice.OnMethodEnter
        public static void onEnter(@Advice.Argument(0) SocketAddress endpoint) {
            validateConnection(AsynchronousSocketChannel.class.getName() + "#connect", endpoint);
        }
    }

    public static class DatagramDenyAdvice {
        @Advice.OnMethodEnter
        public static void onEnter() {
            throw new SecurityException("[policy-agent] DENY non-TCP networking API use (DatagramSocket)");
        }
    }

    private static final class TrustedClassSourceTransformer implements ClassFileTransformer {
        @Override
        public byte[] transform(Module module,
                                ClassLoader loader,
                                String className,
                                Class<?> classBeingRedefined,
                                java.security.ProtectionDomain protectionDomain,
                                byte[] classfileBuffer) throws IllegalClassFormatException {
            validateLoadedClassSource(className, protectionDomain);
            return null;
        }
    }
}