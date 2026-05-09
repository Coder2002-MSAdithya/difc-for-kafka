package org.apache.kafka.security.agent;

import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.CodeSigner;
import java.security.CodeSource;
import java.security.cert.CertPath;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public final class TrustedJarVerifier
{

    private TrustedJarVerifier()
    {

    }

    public static X509Certificate loadTrustedCertificate() throws Exception
    {

        try (InputStream in = TrustedJarVerifier.class.getResourceAsStream("/kafka-signing-cert.pem"))
        {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            return (X509Certificate) cf.generateCertificate(in);
        }
    }

    public static void verifyClass(Class<?> cls) throws Exception
    {
        CodeSource source = cls.getProtectionDomain().getCodeSource();

        if (source == null)
        {
            throw new SecurityException("No code source for class: " + cls.getName());
        }

        URL location = source.getLocation();
        Path jar = Paths.get(location.toURI());
        verifyJar(jar, loadTrustedCertificate());
    }

    public static void verifyJar(Path jarPath, X509Certificate trusted) throws Exception
    {
        try (JarFile jar = new JarFile(
                jarPath.toFile(),
                true)) {

            byte[] buffer = new byte[8192];

            Enumeration<JarEntry> entries = jar.entries();

            while (entries.hasMoreElements())
            {

                JarEntry entry = entries.nextElement();

                if (entry.isDirectory())
                {
                    continue;
                }

                try (InputStream in = jar.getInputStream(entry))
                {
                    while(in.read(buffer) != -1)
                    {

                    }
                }

                CodeSigner[] signers = entry.getCodeSigners();

                if (signers == null)
                {
                    throw new SecurityException("Unsigned entry: " + entry.getName());
                }

                boolean trustedSigner = false;

                for (CodeSigner signer : signers)
                {

                    CertPath path = signer.getSignerCertPath();

                    for (Certificate cert : path.getCertificates())
                    {

                        X509Certificate x509 = (X509Certificate) cert;

                        if(x509.equals(trusted))
                        {
                            trustedSigner = true;
                        }
                    }
                }

                if (!trustedSigner)
                {
                    throw new SecurityException("Untrusted signer for: " + entry.getName());
                }
            }
        }
    }
}