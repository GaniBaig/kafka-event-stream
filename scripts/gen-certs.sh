#!/usr/bin/env bash
# gen-certs.sh — Generate CA, broker, and client certificates for Kafka mTLS demo.
#
# What this script produces:
#   certs/ca.key              — CA private key (never share)
#   certs/ca.crt              — CA certificate (shared with broker + client)
#   certs/broker.key          — Broker private key
#   certs/broker.csr          — Broker certificate signing request
#   certs/broker.crt          — Broker certificate (signed by CA)
#   certs/broker.keystore.jks — Broker Java keystore (Kafka broker reads this)
#   certs/broker.truststore.jks — Broker Java truststore (contains CA cert)
#   certs/client.key          — Client private key
#   certs/client.csr          — Client certificate signing request
#   certs/client.crt          — Client certificate (signed by CA)
#
# Prerequisites: openssl, keytool (Java JDK)
# Usage: bash scripts/gen-certs.sh
# All output goes to certs/ — this directory is git-ignored (never commit private keys).

set -euo pipefail

CERTS_DIR="$(dirname "$0")/../certs"
mkdir -p "$CERTS_DIR"

KEYSTORE_PASS="kafka-dev-pass"   # Dev-only password — never use in production
CA_DAYS=365
CERT_DAYS=365

echo "==> Step 1: Generate CA private key and self-signed certificate"
openssl genrsa -out "$CERTS_DIR/ca.key" 4096
openssl req -new -x509 \
    -days $CA_DAYS \
    -key "$CERTS_DIR/ca.key" \
    -out "$CERTS_DIR/ca.crt" \
    -subj "/C=US/ST=Dev/L=Local/O=KafkaDevCA/CN=KafkaDevCA"

echo "==> Step 2: Generate broker private key and CSR"
openssl genrsa -out "$CERTS_DIR/broker.key" 2048
openssl req -new \
    -key "$CERTS_DIR/broker.key" \
    -out "$CERTS_DIR/broker.csr" \
    -subj "/C=US/ST=Dev/L=Local/O=KafkaBroker/CN=kafka"

echo "==> Step 3: Sign broker CSR with CA (SAN covers localhost and kafka Docker hostname)"
cat > "$CERTS_DIR/broker-ext.cnf" <<EOF
[req]
req_extensions = v3_req
[v3_req]
subjectAltName = @alt_names
[alt_names]
DNS.1 = localhost
DNS.2 = kafka
IP.1  = 127.0.0.1
EOF

openssl x509 -req \
    -days $CERT_DAYS \
    -in "$CERTS_DIR/broker.csr" \
    -CA "$CERTS_DIR/ca.crt" \
    -CAkey "$CERTS_DIR/ca.key" \
    -CAcreateserial \
    -out "$CERTS_DIR/broker.crt" \
    -extfile "$CERTS_DIR/broker-ext.cnf" \
    -extensions v3_req

echo "==> Step 4: Generate client private key and CSR"
openssl genrsa -out "$CERTS_DIR/client.key" 2048
openssl req -new \
    -key "$CERTS_DIR/client.key" \
    -out "$CERTS_DIR/client.csr" \
    -subj "/C=US/ST=Dev/L=Local/O=KafkaClient/CN=kafka-rs-client"

echo "==> Step 5: Sign client CSR with CA"
openssl x509 -req \
    -days $CERT_DAYS \
    -in "$CERTS_DIR/client.csr" \
    -CA "$CERTS_DIR/ca.crt" \
    -CAkey "$CERTS_DIR/ca.key" \
    -CAcreateserial \
    -out "$CERTS_DIR/client.crt"

echo "==> Step 6: Create broker PKCS12 bundle and import into JKS keystore"
# Confluent's Kafka broker reads JKS (Java KeyStore) format for SSL config.
openssl pkcs12 -export \
    -in "$CERTS_DIR/broker.crt" \
    -inkey "$CERTS_DIR/broker.key" \
    -chain -CAfile "$CERTS_DIR/ca.crt" \
    -name kafka \
    -out "$CERTS_DIR/broker.p12" \
    -passout pass:$KEYSTORE_PASS

keytool -importkeystore \
    -deststorepass $KEYSTORE_PASS \
    -destkeypass $KEYSTORE_PASS \
    -destkeystore "$CERTS_DIR/broker.keystore.jks" \
    -srckeystore "$CERTS_DIR/broker.p12" \
    -srcstoretype PKCS12 \
    -srcstorepass $KEYSTORE_PASS \
    -alias kafka \
    -noprompt

echo "==> Step 7: Create broker truststore containing the CA certificate"
keytool -import \
    -file "$CERTS_DIR/ca.crt" \
    -alias CARoot \
    -keystore "$CERTS_DIR/broker.truststore.jks" \
    -storepass $KEYSTORE_PASS \
    -noprompt

echo ""
echo "Certificates generated in $CERTS_DIR"
echo ""
echo "Next steps:"
echo "  1. podman-compose -f docker/docker-compose-ssl.yml up -d"
echo "  2. cargo run -- produce --bootstrap localhost:9093 --tls \\"
echo "         --ca-cert certs/ca.crt \\"
echo "         --client-cert certs/client.crt \\"
echo "         --client-key certs/client.key"
