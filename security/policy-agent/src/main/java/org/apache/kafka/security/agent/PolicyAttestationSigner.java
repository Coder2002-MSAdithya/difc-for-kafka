package org.apache.kafka.security.agent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;

/**
 * Signs canonical processing-policy payloads with an ECDSA key for grant-time verification.
 */
public final class PolicyAttestationSigner {

    public static final String FORMAT = "difc-processing-policy-attestation-v1";
    public static final String SIGNATURE_ALGORITHM = "SHA256withECDSA";

    private static final String KEY_PATH_PROP = "policy.agent.signing.key.path";
    private static final String CERT_PATH_PROP = "policy.agent.signing.cert.path";

    private PolicyAttestationSigner() {}

    public static String signAndWrap(final String canonicalPolicyJson)
    {
        final PrivateKey privateKey = loadSigningKey();
        final String certificatePem = loadCertificatePem();
        if (privateKey == null || certificatePem == null || certificatePem.isEmpty())
        {
            System.out.println("[POLICY][ATTEST] signing skipped (no key/cert); exporting unsigned policy");
            return unsignedEnvelope(canonicalPolicyJson);
        }

        try
        {
            final byte[] payload = canonicalPolicyJson.getBytes(StandardCharsets.UTF_8);
            final Signature signature = Signature.getInstance(SIGNATURE_ALGORITHM);
            signature.initSign(privateKey);
            signature.update(payload);
            final String signatureBase64 = Base64.getEncoder().encodeToString(signature.sign());
            final String payloadBase64 = Base64.getEncoder().encodeToString(payload);

            final StringBuilder sb = new StringBuilder();
            sb.append("{\n");
            sb.append("  \"format\": \"").append(FORMAT).append("\",\n");
            sb.append("  \"signatureAlgorithm\": \"").append(SIGNATURE_ALGORITHM).append("\",\n");
            sb.append("  \"signedPayloadBase64\": \"").append(payloadBase64).append("\",\n");
            sb.append("  \"signatureBase64\": \"").append(signatureBase64).append("\",\n");
            sb.append("  \"certificatePem\": ");
            sb.append(jsonString(certificatePem));
            sb.append("\n");
            sb.append("}\n");
            System.out.println("[POLICY][ATTEST] policy signed algorithm=" + SIGNATURE_ALGORITHM);
            return sb.toString();
        }
        catch (Exception e)
        {
            throw new IllegalStateException("Failed to sign processing policy", e);
        }
    }

    private static String unsignedEnvelope(final String canonicalPolicyJson)
    {
        return "{\n"
                + "  \"format\": \"difc-processing-policy-unsigned-v1\",\n"
                + "  \"signedPayloadBase64\": \""
                + Base64.getEncoder().encodeToString(canonicalPolicyJson.getBytes(StandardCharsets.UTF_8))
                + "\"\n"
                + "}\n";
    }

    public static String buildCanonicalPolicyJson(
            final int version,
            final String generatedAt,
            final String topologyDigest,
            final String principal,
            final String service,
            final String sourcesJson,
            final String aggregationsJson,
            final String sinksJson)
    {
        return buildCanonicalPolicyJson(
                version,
                generatedAt,
                topologyDigest,
                principal,
                service,
                "[]",
                sourcesJson,
                aggregationsJson,
                sinksJson,
                "[]",
                "{\"nodes\":[],\"edges\":[]}");
    }

    public static String buildCanonicalPolicyJson(
            final int version,
            final String generatedAt,
            final String topologyDigest,
            final String principal,
            final String service,
            final String componentsJson,
            final String sourcesJson,
            final String aggregationsJson,
            final String sinksJson,
            final String egressPathsJson,
            final String graphJson)
    {
        return buildCanonicalPolicyJson(
                version,
                generatedAt,
                topologyDigest,
                principal,
                service,
                componentsJson,
                sourcesJson,
                aggregationsJson,
                sinksJson,
                egressPathsJson,
                graphJson,
                "{}");
    }

    public static String buildCanonicalPolicyJson(
            final int version,
            final String generatedAt,
            final String topologyDigest,
            final String principal,
            final String service,
            final String componentsJson,
            final String sourcesJson,
            final String aggregationsJson,
            final String sinksJson,
            final String egressPathsJson,
            final String graphJson,
            final String aggregationAnalysisJson)
    {
        return buildCanonicalPolicyJson(
                version,
                generatedAt,
                topologyDigest,
                principal,
                service,
                componentsJson,
                sourcesJson,
                aggregationsJson,
                sinksJson,
                egressPathsJson,
                graphJson,
                aggregationAnalysisJson,
                "{}");
    }

    public static String buildCanonicalPolicyJson(
            final int version,
            final String generatedAt,
            final String topologyDigest,
            final String principal,
            final String service,
            final String componentsJson,
            final String sourcesJson,
            final String aggregationsJson,
            final String sinksJson,
            final String egressPathsJson,
            final String graphJson,
            final String aggregationAnalysisJson,
            final String relationalAlgebraAnalysisJson)
    {
        return "{"
                + "\"version\":" + version + ","
                + "\"generatedAt\":\"" + escapeJson(generatedAt) + "\","
                + "\"topologyDigest\":\"" + escapeJson(topologyDigest) + "\","
                + "\"principal\":\"" + escapeJson(principal) + "\","
                + "\"service\":\"" + escapeJson(service) + "\","
                + "\"components\":" + componentsJson + ","
                + "\"sources\":" + sourcesJson + ","
                + "\"aggregations\":" + aggregationsJson + ","
                + "\"sinks\":" + sinksJson + ","
                + "\"egressPaths\":" + egressPathsJson + ","
                + "\"graph\":" + graphJson + ","
                + "\"aggregationAnalysis\":" + aggregationAnalysisJson + ","
                + "\"relationalAlgebraAnalysis\":" + relationalAlgebraAnalysisJson
                + "}";
    }

    private static PrivateKey loadSigningKey()
    {
        final String path = System.getProperty(KEY_PATH_PROP);
        if (path == null || path.isEmpty())
        {
            return null;
        }
        try
        {
            final byte[] der = decodePem(Files.readString(Paths.get(path)), "PRIVATE KEY", "EC PRIVATE KEY");
            return KeyFactory.getInstance("EC").generatePrivate(new PKCS8EncodedKeySpec(der));
        }
        catch (Exception e)
        {
            throw new IllegalStateException("Failed to load policy signing key from " + path, e);
        }
    }

    private static String loadCertificatePem()
    {
        final String path = System.getProperty(CERT_PATH_PROP);
        if (path == null || path.isEmpty())
        {
            return null;
        }
        try
        {
            return Files.readString(Path.of(path));
        }
        catch (IOException e)
        {
            throw new IllegalStateException("Failed to load policy signing certificate from " + path, e);
        }
    }

    private static byte[] decodePem(final String pem, final String... labels) throws IOException
    {
        String body = pem;
        for (String label : labels)
        {
            final String begin = "-----BEGIN " + label + "-----";
            final String end = "-----END " + label + "-----";
            final int start = body.indexOf(begin);
            final int finish = body.indexOf(end);
            if (start >= 0 && finish > start)
            {
                body = body.substring(start + begin.length(), finish);
                break;
            }
        }
        return Base64.getMimeDecoder().decode(body.replaceAll("\\s", ""));
    }

    private static String jsonString(final String value)
    {
        return "\"" + escapeJson(value) + "\"";
    }

    private static String escapeJson(final String value)
    {
        if (value == null)
        {
            return "";
        }
        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\r", "\\r")
                .replace("\n", "\\n");
    }
}
