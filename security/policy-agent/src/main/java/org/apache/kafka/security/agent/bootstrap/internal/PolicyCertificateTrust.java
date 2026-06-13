package org.apache.kafka.security.agent.bootstrap.internal;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.CertPath;
import java.security.cert.CertPathValidator;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.PKIXParameters;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Trust policy for JAR code-signer certificates.
 * <p>
 * Default: PKIX chain validation against the mkcert local root CA
 * ({@code $CAROOT/rootCA.pem} or {@code ~/.local/share/mkcert/rootCA.pem}),
 * with optional overrides via:
 * <ul>
 *   <li>{@code -Dpolicy.agent.trusted.ca.path=/path/to/rootCA.pem}</li>
 *   <li>{@code -Dpolicy.agent.trusted.cert.path=/path/to/leaf.pem} (optional exact leaf pin)</li>
 *   <li>{@code -Dpolicy.agent.verify.chain=false} (leaf pin only, no PKIX)</li>
 *   <li>{@code -Dpolicy.agent.trust.jdk=true} (also trust JDK {@code cacerts})</li>
 * </ul>
 */
public final class PolicyCertificateTrust
{
    private static final String TRUSTED_CA_PATH_PROP = "policy.agent.trusted.ca.path";
    private static final String TRUSTED_CERT_PATH_PROP = "policy.agent.trusted.cert.path";
    private static final String VERIFY_CHAIN_PROP = "policy.agent.verify.chain";
    private static final String TRUST_JDK_PROP = "policy.agent.trust.jdk";

    private static final boolean VERIFY_CHAIN =
            Boolean.parseBoolean(System.getProperty(VERIFY_CHAIN_PROP, "true"));

    private static final boolean TRUST_JDK =
            Boolean.parseBoolean(System.getProperty(TRUST_JDK_PROP, "false"));

    private static final Set<X509Certificate> PINNED_LEAVES = loadPinnedLeaves();
    private static final Set<TrustAnchor> TRUST_ANCHORS = loadTrustAnchors();
    private static final CertPathValidator PATH_VALIDATOR = createPathValidator();

    static
    {
        System.out.println(
                "[POLICY][TRUST] verify.chain="
                        + VERIFY_CHAIN
                        + " trust.jdk="
                        + TRUST_JDK
                        + " anchors="
                        + TRUST_ANCHORS.size()
                        + " pinnedLeaves="
                        + PINNED_LEAVES.size());

        if (VERIFY_CHAIN && TRUST_ANCHORS.isEmpty())
        {
            failStop(
                    "[POLICY] No trusted CA anchors loaded. Install mkcert (mkcert -install), "
                            + "set -D"
                            + TRUSTED_CA_PATH_PROP
                            + "=<path-to-rootCA.pem>, "
                            + "or place trusted-ca.pem in the working directory.");
        }
    }

    private PolicyCertificateTrust()
    {

    }

    public static boolean isTrustedSignerChain(final Certificate[] chain)
    {
        if (chain == null || chain.length == 0)
        {
            return false;
        }

        final List<X509Certificate> x509Chain = toX509Chain(chain);
        if (x509Chain.isEmpty())
        {
            return false;
        }

        if (matchesPinnedLeaf(x509Chain))
        {
            return true;
        }

        if (!VERIFY_CHAIN)
        {
            return false;
        }

        if (TRUST_ANCHORS.isEmpty())
        {
            return false;
        }

        try
        {
            final CertificateFactory factory = CertificateFactory.getInstance("X.509");
            final CertPath certPath = factory.generateCertPath(new ArrayList<>(x509Chain));
            final PKIXParameters params = new PKIXParameters(TRUST_ANCHORS);
            params.setRevocationEnabled(false);
            PATH_VALIDATOR.validate(certPath, params);
            return true;
        }
        catch (Exception e)
        {
            System.err.println("[POLICY][DEBUG] PKIX chain validation failed: " + e.getMessage());
            return false;
        }
    }

    private static boolean matchesPinnedLeaf(final List<X509Certificate> chain)
    {
        if (PINNED_LEAVES.isEmpty())
        {
            return false;
        }

        for (X509Certificate cert : chain)
        {
            for (X509Certificate pinned : PINNED_LEAVES)
            {
                if (sameCertificate(cert, pinned))
                {
                    return true;
                }
            }
        }

        return false;
    }

    private static List<X509Certificate> toX509Chain(final Certificate[] chain)
    {
        final List<X509Certificate> x509Chain = new ArrayList<>(chain.length);

        for (Certificate cert : chain)
        {
            if (cert instanceof X509Certificate x509)
            {
                x509Chain.add(x509);
            }
        }

        return x509Chain;
    }

    private static Set<X509Certificate> loadPinnedLeaves()
    {
        final Set<X509Certificate> pinned = new LinkedHashSet<>();
        final String explicitPath = System.getProperty(TRUSTED_CERT_PATH_PROP, "").trim();

        if (!explicitPath.isEmpty())
        {
            loadCertificateFile(Paths.get(explicitPath)).ifPresent(pinned::add);
        }

        // Backward-compatible default leaf pin when present in the working directory.
        loadCertificateFile(Paths.get("kafka-signing-cert.pem")).ifPresent(pinned::add);
        loadCertificateFile(Paths.get("kafka-signer-cert.pem")).ifPresent(pinned::add);

        return Collections.unmodifiableSet(pinned);
    }

    private static Set<TrustAnchor> loadTrustAnchors()
    {
        final Set<TrustAnchor> anchors = new LinkedHashSet<>();

        final String explicitCaPath = System.getProperty(TRUSTED_CA_PATH_PROP, "").trim();
        if (!explicitCaPath.isEmpty())
        {
            loadCertificateFile(Paths.get(explicitCaPath)).ifPresent(cert -> addAnchor(anchors, cert));
        }

        final Path mkcertRoot = resolveMkcertRootCaPath();
        if (mkcertRoot != null)
        {
            loadCertificateFile(mkcertRoot).ifPresent(cert -> addAnchor(anchors, cert));
        }

        loadCertificateFile(Paths.get("trusted-ca.pem")).ifPresent(cert -> addAnchor(anchors, cert));

        if (TRUST_JDK)
        {
            addJdkTrustAnchors(anchors);
        }

        return Collections.unmodifiableSet(anchors);
    }

    private static void addAnchor(final Set<TrustAnchor> anchors, final X509Certificate cert)
    {
        for (TrustAnchor existing : anchors)
        {
            if (sameCertificate(existing.getTrustedCert(), cert))
            {
                return;
            }
        }

        anchors.add(new TrustAnchor(cert, null));
    }

    private static Path resolveMkcertRootCaPath()
    {
        final String caroot = System.getenv("CAROOT");
        if (caroot != null && !caroot.isBlank())
        {
            final Path rootCa = Paths.get(caroot.trim(), "rootCA.pem");
            if (Files.isRegularFile(rootCa))
            {
                return rootCa;
            }
        }

        final Path xdgDefault =
                Paths.get(System.getProperty("user.home"), ".local", "share", "mkcert", "rootCA.pem");

        if (Files.isRegularFile(xdgDefault))
        {
            return xdgDefault;
        }

        return null;
    }

    private static void addJdkTrustAnchors(final Set<TrustAnchor> anchors)
    {
        try
        {
            final KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            final Path cacerts =
                    Paths.get(System.getProperty("java.home"), "lib", "security", "cacerts");

            try (InputStream in = Files.newInputStream(cacerts))
            {
                keyStore.load(in, "changeit".toCharArray());
            }

            for (String alias : Collections.list(keyStore.aliases()))
            {
                if (!keyStore.isCertificateEntry(alias))
                {
                    continue;
                }

                final Certificate cert = keyStore.getCertificate(alias);
                if (cert instanceof X509Certificate x509)
                {
                    addAnchor(anchors, x509);
                }
            }
        }
        catch (Exception e)
        {
            System.err.println("[POLICY][TRUST] Unable to load JDK cacerts: " + e.getMessage());
        }
    }

    private static java.util.Optional<X509Certificate> loadCertificateFile(final Path certPath)
    {
        if (certPath == null || !Files.isRegularFile(certPath))
        {
            return java.util.Optional.empty();
        }

        try (InputStream in = Files.newInputStream(certPath))
        {
            final CertificateFactory factory = CertificateFactory.getInstance("X.509");
            return java.util.Optional.of((X509Certificate) factory.generateCertificate(in));
        }
        catch (Exception e)
        {
            System.err.println("[POLICY][TRUST] Unable to load certificate " + certPath + ": " + e.getMessage());
            return java.util.Optional.empty();
        }
    }

    private static CertPathValidator createPathValidator()
    {
        try
        {
            return CertPathValidator.getInstance("PKIX");
        }
        catch (Exception e)
        {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static boolean sameCertificate(final X509Certificate left, final X509Certificate right)
    {
        try
        {
            return java.util.Arrays.equals(left.getEncoded(), right.getEncoded());
        }
        catch (Exception e)
        {
            throw new RuntimeException("Unable to compare X.509 certificates", e);
        }
    }

    private static void failStop(final String msg)
    {
        System.err.println(msg);
        Runtime.getRuntime().halt(1);
    }
}
