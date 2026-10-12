//! Builds `SecurityConfig` from CLI TLS/SASL flags.

use std::path::PathBuf;

use proto::{SecurityConfig, TlsMode};

use crate::args::{ConsumeArgs, ProduceArgs};

/// Build a `SecurityConfig` from TLS/SASL flags common to produce and consume.
///
/// Password is read from `KAFKA_SASL_PASSWORD` env var — never from a CLI arg
/// (CLI args appear in shell history, violating the secrets policy).
pub fn build_security_config(
    tls: bool,
    ca_cert: Option<&str>,
    client_cert: Option<&str>,
    client_key: Option<&str>,
    sasl_username: Option<&str>,
    sasl_mechanism: Option<&str>,
) -> Option<SecurityConfig> {
    if let Some(username) = sasl_username {
        let password = std::env::var("KAFKA_SASL_PASSWORD").unwrap_or_default();
        return Some(match sasl_mechanism.unwrap_or("PLAIN") {
            "SCRAM-SHA-256" => SecurityConfig::SaslScramSha256 {
                username: username.to_string(),
                password,
            },
            "SCRAM-SHA-512" => SecurityConfig::SaslScramSha512 {
                username: username.to_string(),
                password,
            },
            _ => SecurityConfig::SaslPlain {
                username: username.to_string(),
                password,
            },
        });
    }

    if tls {
        let ca = ca_cert.map(PathBuf::from);
        let cert = client_cert.map(PathBuf::from);
        let key = client_key.map(PathBuf::from);

        let mode = match (ca, cert, key) {
            (Some(ca_cert_path), Some(client_cert_path), Some(client_key_path)) => {
                TlsMode::MutualTls {
                    ca_cert_path,
                    client_cert_path,
                    client_key_path,
                }
            }
            (Some(ca_cert_path), _, _) => TlsMode::Tls { ca_cert_path },
            _ => TlsMode::Disabled,
        };
        return Some(SecurityConfig::Ssl(mode));
    }

    None
}

pub fn security_from_produce(args: &ProduceArgs) -> Option<SecurityConfig> {
    build_security_config(
        args.tls,
        args.ca_cert.as_deref(),
        args.client_cert.as_deref(),
        args.client_key.as_deref(),
        args.sasl_username.as_deref(),
        args.sasl_mechanism.as_deref(),
    )
}

pub fn security_from_consume(args: &ConsumeArgs) -> Option<SecurityConfig> {
    build_security_config(
        args.tls,
        args.ca_cert.as_deref(),
        args.client_cert.as_deref(),
        args.client_key.as_deref(),
        args.sasl_username.as_deref(),
        args.sasl_mechanism.as_deref(),
    )
}
