//! TLS support using rustls.
//!
//! This module provides TLS connection support for Kafka brokers.
//! Security features (PLAIN, SASL/SSL, mTLS) will be implemented in a future version.

use std::net::SocketAddr;
use std::sync::Arc;

use rustls::pki_types::ServerName;
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;

use crate::error::{ConnectionError, Error};

/// TLS configuration for Kafka connections.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// CA certificate in PEM format.
    pub ca_cert_pem: Option<Vec<u8>>,
    /// Client certificate in PEM format (for mTLS).
    pub client_cert_pem: Option<Vec<u8>>,
    /// Client key in PEM format (for mTLS).
    pub client_key_pem: Option<Vec<u8>>,
    /// Skip server certificate verification (dangerous, testing only).
    pub danger_accept_invalid_certs: bool,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            ca_cert_pem: None,
            client_cert_pem: None,
            client_key_pem: None,
            danger_accept_invalid_certs: false,
        }
    }
}

impl TlsConfig {
    /// Create a new TLS config with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set CA certificate from PEM bytes.
    pub fn with_ca_cert(mut self, pem: Vec<u8>) -> Self {
        self.ca_cert_pem = Some(pem);
        self
    }

    /// Set client certificate and key for mTLS.
    pub fn with_client_cert(mut self, cert_pem: Vec<u8>, key_pem: Vec<u8>) -> Self {
        self.client_cert_pem = Some(cert_pem);
        self.client_key_pem = Some(key_pem);
        self
    }

    /// Skip server certificate verification (dangerous!).
    pub fn danger_accept_invalid_certs(mut self) -> Self {
        self.danger_accept_invalid_certs = true;
        self
    }

    /// Build a TLS connector from this configuration.
    pub fn build_connector(&self) -> Result<TlsConnector, Error> {
        let config = self.build_client_config()?;
        Ok(TlsConnector::from(Arc::new(config)))
    }

    fn build_client_config(&self) -> Result<rustls::ClientConfig, Error> {
        let builder = rustls::ClientConfig::builder();

        // Configure root certificates
        let root_store = if let Some(ca_pem) = &self.ca_cert_pem {
            let mut root_store = rustls::RootCertStore::empty();
            let certs = rustls_pemfile::certs(&mut ca_pem.as_slice())
                .filter_map(|r| r.ok())
                .collect::<Vec<_>>();
            for cert in certs {
                root_store
                    .add(cert)
                    .map_err(|e| ConnectionError::Tls(e.to_string()))?;
            }
            root_store
        } else {
            // Use system root certificates
            let mut root_store = rustls::RootCertStore::empty();
            root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            root_store
        };

        let config = builder.with_root_certificates(root_store);

        // Configure client authentication if provided
        let config = if let (Some(cert_pem), Some(key_pem)) =
            (&self.client_cert_pem, &self.client_key_pem)
        {
            let certs = rustls_pemfile::certs(&mut cert_pem.as_slice())
                .filter_map(|r| r.ok())
                .collect::<Vec<_>>();

            let key = rustls_pemfile::private_key(&mut key_pem.as_slice())
                .map_err(|e| ConnectionError::Tls(e.to_string()))?
                .ok_or_else(|| ConnectionError::Tls("No private key found in PEM".to_string()))?;

            config
                .with_client_auth_cert(certs, key)
                .map_err(|e| ConnectionError::Tls(e.to_string()))?
        } else {
            config.with_no_client_auth()
        };

        Ok(config)
    }
}

/// Connect to a broker with TLS.
pub async fn connect_tls(
    addr: SocketAddr,
    hostname: &str,
    config: &TlsConfig,
) -> Result<TlsStream<TcpStream>, Error> {
    let connector = config.build_connector()?;
    let stream = TcpStream::connect(addr)
        .await
        .map_err(ConnectionError::Io)?;

    let server_name = ServerName::try_from(hostname.to_owned())
        .map_err(|e| ConnectionError::Tls(format!("Invalid server name: {}", e)))?;

    let tls_stream = connector
        .connect(server_name, stream)
        .await
        .map_err(|e| ConnectionError::Tls(e.to_string()))?;

    Ok(tls_stream)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tls_config_default() {
        let config = TlsConfig::default();
        assert!(config.ca_cert_pem.is_none());
        assert!(config.client_cert_pem.is_none());
        assert!(!config.danger_accept_invalid_certs);
    }
}
