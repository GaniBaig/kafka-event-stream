//! Maps `SecurityConfig` variants to rdkafka `ClientConfig` string properties.
//!
//! `rdkafka` is configured via string key-value pairs on `ClientConfig`.
//! This function is the single place that translates proto's typed security
//! config into the rdkafka wire-level property names.

use proto::{SecurityConfig, TlsMode};
use rdkafka::config::ClientConfig;

/// Apply a `SecurityConfig` to a rdkafka `ClientConfig`.
///
/// Called by both `KafkaProducer::new` and `KafkaConsumer::new`.
/// No-op for `SecurityConfig::Plain`.
pub fn apply_security_config(client_config: &mut ClientConfig, security: &SecurityConfig) {
    match security {
        SecurityConfig::Plain => {
            // Plaintext — no additional properties needed.
        }

        SecurityConfig::Ssl(TlsMode::Disabled) => {
            // Caller requested Ssl(Disabled) — treat as plaintext.
        }

        SecurityConfig::Ssl(TlsMode::Tls { ca_cert_path }) => {
            client_config
                .set("security.protocol", "SSL")
                .set("ssl.ca.location", ca_cert_path.to_string_lossy().as_ref());
        }

        SecurityConfig::Ssl(TlsMode::MutualTls {
            ca_cert_path,
            client_cert_path,
            client_key_path,
        }) => {
            // mTLS: broker verifies client cert + client verifies broker cert.
            // Both parties present X.509 certificates — proves mutual identity.
            client_config
                .set("security.protocol", "SSL")
                .set("ssl.ca.location", ca_cert_path.to_string_lossy().as_ref())
                .set(
                    "ssl.certificate.location",
                    client_cert_path.to_string_lossy().as_ref(),
                )
                .set(
                    "ssl.key.location",
                    client_key_path.to_string_lossy().as_ref(),
                );
        }

        SecurityConfig::SaslPlain { username, password } => {
            // SASL/PLAIN — username+password over SASL protocol.
            // Password comes from runtime config, never hardcoded.
            client_config
                .set("security.protocol", "SASL_PLAINTEXT")
                .set("sasl.mechanism", "PLAIN")
                .set("sasl.username", username)
                .set("sasl.password", password);
        }

        SecurityConfig::SaslScramSha256 { username, password } => {
            client_config
                .set("security.protocol", "SASL_SSL")
                .set("sasl.mechanism", "SCRAM-SHA-256")
                .set("sasl.username", username)
                .set("sasl.password", password);
        }

        SecurityConfig::SaslScramSha512 { username, password } => {
            client_config
                .set("security.protocol", "SASL_SSL")
                .set("sasl.mechanism", "SCRAM-SHA-512")
                .set("sasl.username", username)
                .set("sasl.password", password);
        }
    }
}
