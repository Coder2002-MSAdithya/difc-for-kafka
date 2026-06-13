package org.apache.kafka.security.agent;

import org.apache.kafka.security.agent.bootstrap.internal.PolicyCertificateTrust;

import java.nio.file.Path;
import java.security.CodeSigner;
import java.security.CodeSource;
import java.security.cert.CertPath;
import java.security.cert.Certificate;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public final class TrustedJarVerifier
{

    private TrustedJarVerifier()
    {

    }

    public static void verifyClass(final Class<?> cls) throws Exception
    {
        CodeSource source = cls.getProtectionDomain().getCodeSource();

        if (source == null)
        {
            throw new SecurityException("No code source for class: " + cls.getName());
        }

        final Certificate[] chain = source.getCertificates();
        if (!PolicyCertificateTrust.isTrustedSignerChain(chain))
        {
            throw new SecurityException("Untrusted signer chain for class: " + cls.getName());
        }
    }

    public static void verifyJar(final Path jarPath) throws Exception
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

                try (java.io.InputStream in = jar.getInputStream(entry))
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
                    Certificate[] chain = path.getCertificates().toArray(new Certificate[0]);

                    if (PolicyCertificateTrust.isTrustedSignerChain(chain))
                    {
                        trustedSigner = true;
                        break;
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
