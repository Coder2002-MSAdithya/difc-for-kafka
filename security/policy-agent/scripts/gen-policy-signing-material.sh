#!/usr/bin/env bash
# Generate ECDSA policy-signing key + certificate for the DIFC processing-policy agent.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AGENT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUT_DIR="${AGENT_DIR}/policy-signing"
MKCERT_CA="${AGENT_DIR}/mkcert-ca/rootCA.pem"
MKCERT_KEY="${AGENT_DIR}/mkcert-ca/rootCA-key.pem"

mkdir -p "${OUT_DIR}"

openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "${OUT_DIR}/policy-signing-key.pem"

CSR="${OUT_DIR}/policy-signing.csr"
openssl req -new -key "${OUT_DIR}/policy-signing-key.pem" -out "${CSR}" \
  -subj "/CN=difc-policy-agent/O=Apache Kafka Policy Agent"

if [[ -f "${MKCERT_CA}" && -f "${MKCERT_KEY}" ]]; then
  openssl x509 -req -in "${CSR}" -CA "${MKCERT_CA}" -CAkey "${MKCERT_KEY}" \
    -CAcreateserial -out "${OUT_DIR}/policy-signing-cert.pem" -days 825 \
    -sha256 -extfile <(printf "subjectAltName=DNS:difc-policy-agent")
  echo "Issued policy signing cert with mkcert CA."
else
  openssl req -x509 -key "${OUT_DIR}/policy-signing-key.pem" \
    -out "${OUT_DIR}/policy-signing-cert.pem" -days 3650 -sha256 \
    -subj "/CN=difc-policy-agent/O=Apache Kafka Policy Agent"
  echo "Self-signed policy signing cert (no mkcert CA found)."
fi

rm -f "${CSR}"
echo "Wrote ${OUT_DIR}/policy-signing-key.pem and policy-signing-cert.pem"
