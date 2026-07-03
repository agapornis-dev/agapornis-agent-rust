//! mTLS identity validation, installation, rollback, and reload signaling.

use crate::config::DaemonConfig;
use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use subtle::ConstantTimeEq;
use tokio::{fs, sync::watch};
use tonic::transport::{Certificate, Identity, ServerTlsConfig};
use x509_parser::{
    extensions::ParsedExtension, pem::parse_x509_pem, prelude::FromDer, time::ASN1Time,
};

const DEFAULT_CERTIFICATE_CLOCK_SKEW_SECONDS: i64 = 300;

#[derive(Clone)]
pub struct CertificateManager {
    inner: Arc<Inner>,
}
struct Inner {
    config: DaemonConfig,
    reload: watch::Sender<u64>,
}

impl CertificateManager {
    pub fn new(config: DaemonConfig) -> Self {
        let (reload, _) = watch::channel(0);
        Self {
            inner: Arc::new(Inner { config, reload }),
        }
    }
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.inner.reload.subscribe()
    }
    pub async fn tls(&self) -> Result<ServerTlsConfig> {
        let cert_path = &self.inner.config.agent_cert_path;
        let key_path = &self.inner.config.agent_key_path;
        let ca_path = &self.inner.config.ca_cert_path;

        tracing::info!(
            cert = %cert_path.display(),
            key = %key_path.display(),
            ca = %ca_path.display(),
            "loading agent mTLS certificate bundle"
        );

        let cert = fs::read(cert_path)
            .await
            .with_context(|| format!("read agent certificate {}", cert_path.display()))?;

        let key = fs::read(key_path)
            .await
            .with_context(|| format!("read agent private key {}", key_path.display()))?;

        let ca = fs::read(ca_path)
            .await
            .with_context(|| format!("read master CA certificate {}", ca_path.display()))?;

        Ok(ServerTlsConfig::new()
            .identity(Identity::from_pem(cert, key))
            .client_ca_root(Certificate::from_pem(ca)))
    }
    pub async fn install(&self, cert: &str, key: &str, ca: &str, expected: &str) -> Result<String> {
        let fingerprint = validate_certificate(cert, ca, &self.inner.config.node_id)?;
        if !constant_fingerprint(&fingerprint, expected) {
            bail!("certificate fingerprint did not match expected fingerprint")
        }
        // Parsing as a tonic identity catches malformed PEM; rustls checks key/cert compatibility on reload.
        let tls = ServerTlsConfig::new()
            .identity(Identity::from_pem(cert.as_bytes(), key.as_bytes()))
            .client_ca_root(Certificate::from_pem(ca.as_bytes()));
        let _ = tonic::transport::Server::builder()
            .tls_config(tls)
            .context("certificate and private key did not form a valid TLS identity")?;
        replace_with_previous(&self.inner.config.agent_cert_path, cert.as_bytes()).await?;
        replace_with_previous(&self.inner.config.agent_key_path, key.as_bytes()).await?;
        replace_with_previous(&self.inner.config.ca_cert_path, ca.as_bytes()).await?;
        schedule_reload(self.inner.reload.clone());
        Ok(fingerprint)
    }
    pub async fn rollback(&self) -> Result<String> {
        for path in [
            &self.inner.config.agent_cert_path,
            &self.inner.config.agent_key_path,
            &self.inner.config.ca_cert_path,
        ] {
            let previous = previous(path);
            if !previous.exists() {
                bail!("previous certificate bundle is not available")
            }
        }
        for path in [
            &self.inner.config.agent_cert_path,
            &self.inner.config.agent_key_path,
            &self.inner.config.ca_cert_path,
        ] {
            let previous = previous(path);
            let current = fs::read(path).await?;
            let old = fs::read(&previous).await?;
            fs::write(path, &old).await?;
            fs::write(previous, current).await?;
        }
        let cert = fs::read_to_string(&self.inner.config.agent_cert_path).await?;
        let ca = fs::read_to_string(&self.inner.config.ca_cert_path).await?;
        let fp = validate_certificate(&cert, &ca, &self.inner.config.node_id)?;
        schedule_reload(self.inner.reload.clone());
        Ok(fp)
    }
}
fn schedule_reload(tx: watch::Sender<u64>) {
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let next = *tx.borrow() + 1;
        let _ = tx.send(next);
    });
}
fn validate_certificate(pem: &str, ca_pem: &str, node_id: &str) -> Result<String> {
    let (_, pem) =
        parse_x509_pem(pem.as_bytes()).map_err(|_| anyhow::anyhow!("invalid certificate PEM"))?;
    let (_, cert) = x509_parser::certificate::X509Certificate::from_der(&pem.contents)
        .map_err(|_| anyhow::anyhow!("invalid X.509 certificate"))?;
    let (_, ca_pem) = parse_x509_pem(ca_pem.as_bytes())
        .map_err(|_| anyhow::anyhow!("invalid CA certificate PEM"))?;
    let (_, ca) = x509_parser::certificate::X509Certificate::from_der(&ca_pem.contents)
        .map_err(|_| anyhow::anyhow!("invalid CA certificate"))?;
    cert.verify_signature(Some(ca.public_key()))
        .map_err(|_| anyhow::anyhow!("certificate is not signed by the configured Agapornis CA"))?;
    let now = ASN1Time::now().timestamp();
    let clock_skew = std::env::var("AGAPORNIS_CERTIFICATE_CLOCK_SKEW_SECONDS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| (0..=3600).contains(value))
        .unwrap_or(DEFAULT_CERTIFICATE_CLOCK_SKEW_SECONDS);
    if now + clock_skew < cert.validity().not_before.timestamp()
        || now - clock_skew > cert.validity().not_after.timestamp()
    {
        bail!("certificate is not currently valid")
    }
    let cn = cert
        .subject()
        .iter_common_name()
        .next()
        .and_then(|x| x.as_str().ok())
        .unwrap_or("");
    if !node_id.is_empty() && cn != node_id {
        bail!("agent certificate common name must equal node id '{node_id}'")
    }
    let mut server_auth = false;
    for ext in cert.extensions() {
        if let ParsedExtension::ExtendedKeyUsage(eku) = ext.parsed_extension() {
            server_auth = eku.server_auth;
        }
    }
    if !server_auth {
        bail!("agent certificate does not permit TLS server authentication")
    }
    Ok(hex::encode(Sha256::digest(&pem.contents)))
}
fn constant_fingerprint(a: &str, b: &str) -> bool {
    let clean = |v: &str| v.replace(':', "").trim().to_ascii_lowercase();
    let (a, b) = (clean(a), clean(b));
    a.len() == b.len() && a.as_bytes().ct_eq(b.as_bytes()).into()
}
fn previous(path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.previous", path.display()))
}
async fn replace_with_previous(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    if path.exists() {
        fs::copy(path, previous(path))
            .await
            .context("preserve previous certificate")?;
    }
    let temp = PathBuf::from(format!("{}.new", path.display()));
    fs::write(&temp, bytes).await?;
    fs::rename(temp, path).await?;
    Ok(())
}
