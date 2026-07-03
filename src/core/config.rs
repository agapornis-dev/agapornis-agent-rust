//! Agent configuration loading and first-run provisioning.

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::{
    io::{self, Write},
    path::{Path, PathBuf},
};
use tokio::fs;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DaemonConfig {
    #[serde(default)]
    pub node_id: String,
    #[serde(default = "default_agent_cert")]
    pub agent_cert_path: PathBuf,
    #[serde(default = "default_agent_key")]
    pub agent_key_path: PathBuf,
    #[serde(default = "default_ca_cert")]
    pub ca_cert_path: PathBuf,
    #[serde(default)]
    pub crowd_sec_telemetry: CrowdSecTelemetryConfig,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct CrowdSecTelemetryConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_cscli")]
    pub cscli_path: String,
    #[serde(default = "default_max_alerts")]
    pub max_alerts: usize,
}

fn default_agent_cert() -> PathBuf {
    "certs/agent.crt".into()
}
fn default_agent_key() -> PathBuf {
    "certs/agent.key".into()
}
fn default_ca_cert() -> PathBuf {
    "certs/ca.crt".into()
}
fn default_cscli() -> String {
    "cscli".into()
}
fn default_max_alerts() -> usize {
    100
}

impl DaemonConfig {
    pub async fn load_or_setup() -> Result<Self> {
        if Path::new("config.json").exists() {
            let bytes = fs::read("config.json").await.context("read config.json")?;
            return serde_json::from_slice(&bytes).context("parse config.json");
        }
        Self::setup().await
    }

    async fn setup() -> Result<Self> {
        println!("\n  agapornis agent  ·  node setup — mTLS provisioning\n");
        let master = prompt("  master url · ")?.trim_end_matches('/').to_owned();
        if !(master.starts_with("https://")
            || master.starts_with("http://127.0.0.1")
            || master.starts_with("http://localhost"))
        {
            bail!("remote provisioning requires HTTPS (plain HTTP is only allowed for loopback)");
        }
        let node_id = prompt("  node id    · ")?;
        let token = prompt("  boot token · ")?;
        let client = reqwest::Client::builder()
            .https_only(master.starts_with("https://"))
            .build()?;
        let response = client
            .post(format!("{master}/api/provision/agent"))
            .json(&serde_json::json!({"nodeId": node_id, "bootstrapToken": token}))
            .send()
            .await
            .context("contact master")?;
        if !response.status().is_success() {
            bail!(
                "provisioning failed ({}): {}",
                response.status(),
                response.text().await.unwrap_or_default()
            );
        }
        let payload: serde_json::Value = response
            .json()
            .await
            .context("parse provisioning response")?;
        fs::create_dir_all("certs").await?;
        write_secret("certs/agent.key", required(&payload, "key")?).await?;
        write_secret("certs/agent.crt", required(&payload, "cert")?).await?;
        write_secret("certs/ca.crt", required(&payload, "ca")?).await?;
        let config = Self {
            node_id,
            agent_cert_path: absolute("certs/agent.crt")?,
            agent_key_path: absolute("certs/agent.key")?,
            ca_cert_path: absolute("certs/ca.crt")?,
            crowd_sec_telemetry: CrowdSecTelemetryConfig::default(),
        };
        fs::write("config.json", serde_json::to_vec_pretty(&config)?).await?;
        println!("  mTLS certificates saved · daemon starting");
        Ok(config)
    }
}

pub fn load_dotenv() {
    let _ = dotenvy::dotenv();
}

fn required<'a>(value: &'a serde_json::Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .and_then(|v| v.as_str())
        .with_context(|| format!("provisioning response omitted {key}"))
}
fn absolute(value: &str) -> Result<PathBuf> {
    Ok(std::env::current_dir()?.join(value))
}
fn prompt(label: &str) -> Result<String> {
    print!("{label}");
    io::stdout().flush()?;
    let mut value = String::new();
    io::stdin().read_line(&mut value)?;
    let value = value.trim().to_owned();
    if value.is_empty() {
        bail!("value cannot be empty")
    }
    Ok(value)
}
async fn write_secret(path: &str, value: &str) -> Result<()> {
    fs::write(path, value).await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).await?;
    }
    Ok(())
}
