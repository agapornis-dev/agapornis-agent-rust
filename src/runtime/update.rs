//! Staged, checksum-verified agent binary updates.

use anyhow::{Context, Result, bail};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use subtle::ConstantTimeEq;
use tokio::{fs, io::AsyncWriteExt};

#[derive(Clone, Default)]
pub struct UpdateManager;
#[derive(Debug)]
pub struct UpdateStatus {
    pub version: String,
    pub runtime: String,
    pub executable: String,
    pub staging: String,
    pub pending: String,
    pub restart_required: bool,
}
#[derive(Debug)]
pub struct UpdateResult {
    pub message: String,
    pub staged: String,
    pub restart_required: bool,
}
#[derive(Debug, Serialize, Deserialize)]
struct ActivationState {
    previous: String,
    activated: String,
    activated_at: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ActivationOutcome {
    NothingPending,
    Activated,
    RolledBack,
}
impl UpdateManager {
    pub fn status(&self) -> UpdateStatus {
        let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("agapornis-agent"));
        let staging = staging(&exe);
        let pending = std::fs::read_to_string(staging.join("pending-artifact"))
            .unwrap_or_default()
            .trim()
            .to_owned();
        UpdateStatus {
            version: option_env!("AGAPORNIS_BUILD_VERSION")
                .unwrap_or(env!("CARGO_PKG_VERSION"))
                .into(),
            runtime: format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
            executable: exe.display().to_string(),
            staging: staging.display().to_string(),
            restart_required: !pending.is_empty(),
            pending,
        }
    }
    pub async fn stage(&self, url: &str, sha: &str) -> Result<UpdateResult> {
        if !url.starts_with("https://") {
            bail!("update artifact URL must use HTTPS")
        }
        let expected = hex::decode(sha.trim()).context("SHA-256 must be hexadecimal")?;
        if expected.len() != 32 {
            bail!("SHA-256 checksum is required")
        }
        let client = reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::limited(3))
            .build()?;
        let response = client.get(url).send().await?.error_for_status()?;
        if response.url().scheme() != "https" {
            bail!("update redirects must remain on HTTPS")
        }
        if response
            .content_length()
            .is_some_and(|v| v > 512 * 1024 * 1024)
        {
            bail!("update artifact exceeds 512 MiB limit")
        }
        let exe = std::env::current_exe()?;
        let dir = staging(&exe);
        fs::create_dir_all(&dir).await?;
        let target = dir.join(format!(
            "agapornis-agent-{}.pending",
            chrono::Utc::now().format("%Y%m%d%H%M%S")
        ));
        let mut file = fs::File::create(&target).await?;
        let mut hash = Sha256::new();
        let mut size = 0u64;
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            size += chunk.len() as u64;
            if size > 512 * 1024 * 1024 {
                let _ = fs::remove_file(&target).await;
                bail!("update artifact exceeds 512 MiB limit")
            }
            hash.update(&chunk);
            file.write_all(&chunk).await?;
        }
        file.flush().await?;
        let actual = hash.finalize();
        if !bool::from(actual.as_slice().ct_eq(&expected)) {
            let _ = fs::remove_file(&target).await;
            bail!("update artifact checksum mismatch")
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755)).await?;
        }
        fs::write(dir.join("pending-artifact"), target.display().to_string()).await?;
        schedule_service_restart();
        Ok(UpdateResult {
            message: if automatic_restart_enabled() {
                "Update staged. The agent service restart has been scheduled.".into()
            } else {
                "Update staged. Restart the agent service to activate it.".into()
            },
            staged: target.display().to_string(),
            restart_required: true,
        })
    }

    pub fn activate_pending(&self) -> Result<ActivationOutcome> {
        activate_at(&std::env::current_exe()?)
    }

    pub fn rollback(&self) -> Result<ActivationOutcome> {
        rollback_at(&std::env::current_exe()?)
    }

    pub fn schedule_health_commit(&self) {
        let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("agapornis-agent"));
        if !activation_state_path(&exe).exists() {
            return;
        }
        let seconds = std::env::var("AGAPORNIS_UPDATE_HEALTH_SECONDS")
            .ok()
            .and_then(|value| value.parse().ok())
            .filter(|value| *value > 0)
            .unwrap_or(30);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(seconds)).await;
            if let Err(error) = commit_healthy_at(&exe) {
                tracing::warn!(error=%error, "failed to commit healthy agent update");
            } else {
                tracing::info!("agent update passed its health window and was committed");
            }
        });
    }
}
fn staging(exe: &Path) -> PathBuf {
    exe.parent()
        .unwrap_or_else(|| Path::new("."))
        .join("updates")
}

fn activate_at(exe: &Path) -> Result<ActivationOutcome> {
    let dir = staging(exe);
    let marker = dir.join("pending-artifact");
    if !marker.exists() {
        if activation_state_path(exe).exists() {
            return rollback_at(exe);
        }
        return Ok(ActivationOutcome::NothingPending);
    }
    let pending = PathBuf::from(std::fs::read_to_string(&marker)?.trim());
    let canonical_dir = std::fs::canonicalize(&dir)?;
    let canonical_pending = std::fs::canonicalize(&pending)?;
    if canonical_pending.parent() != Some(canonical_dir.as_path()) {
        bail!("pending agent update is outside the update staging directory")
    }
    let previous = dir.join("previous-agent");
    let previous_temp = dir.join("previous-agent.tmp");
    std::fs::copy(exe, &previous_temp).context("preserve previous agent binary")?;
    std::fs::rename(&previous_temp, &previous).context("commit previous agent binary")?;
    let state = ActivationState {
        previous: previous.display().to_string(),
        activated: exe.display().to_string(),
        activated_at: chrono::Utc::now().to_rfc3339(),
    };
    std::fs::write(
        activation_state_path(exe),
        serde_json::to_vec_pretty(&state)?,
    )?;
    replace_binary(&canonical_pending, exe).context("activate staged agent binary")?;
    std::fs::remove_file(marker)?;
    Ok(ActivationOutcome::Activated)
}

fn rollback_at(exe: &Path) -> Result<ActivationOutcome> {
    let state_path = activation_state_path(exe);
    if !state_path.exists() {
        return Ok(ActivationOutcome::NothingPending);
    }
    let state: ActivationState = serde_json::from_slice(&std::fs::read(&state_path)?)?;
    let previous = PathBuf::from(state.previous);
    if !previous.exists() {
        bail!("previous agent binary is unavailable for rollback")
    }
    let failed = staging(exe).join(format!(
        "failed-agent-{}",
        chrono::Utc::now().format("%Y%m%d%H%M%S")
    ));
    std::fs::rename(exe, &failed).context("preserve failed agent binary")?;
    if let Err(error) = std::fs::rename(&previous, exe) {
        let _ = std::fs::rename(&failed, exe);
        return Err(error).context("restore previous agent binary");
    }
    std::fs::remove_file(state_path)?;
    Ok(ActivationOutcome::RolledBack)
}

fn commit_healthy_at(exe: &Path) -> Result<()> {
    let state_path = activation_state_path(exe);
    if !state_path.exists() {
        return Ok(());
    }
    let state: ActivationState = serde_json::from_slice(&std::fs::read(&state_path)?)?;
    let _ = std::fs::remove_file(state.previous);
    std::fs::remove_file(state_path)?;
    Ok(())
}

fn replace_binary(source: &Path, target: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(source, std::fs::Permissions::from_mode(0o755))?;
    }
    std::fs::rename(source, target)?;
    Ok(())
}

fn activation_state_path(exe: &Path) -> PathBuf {
    staging(exe).join("activation-state.json")
}

fn automatic_restart_enabled() -> bool {
    std::env::var("AGAPORNIS_UPDATE_AUTO_RESTART")
        .is_ok_and(|value| value.eq_ignore_ascii_case("true") || value == "1")
}

fn schedule_service_restart() {
    if !automatic_restart_enabled() {
        return;
    }
    /*
     * The Linux systemd unit is the long-lived supervisor. The running
     * process cannot replace itself in memory, so staging writes the new
     * executable first and asks systemd to restart the unit. At the next
     * launch main activates the pending binary; systemd then continues to
     * provide restart policy, logging, and service lifetime management.
     */
    let service = std::env::var("AGAPORNIS_UPDATE_SYSTEMD_SERVICE")
        .unwrap_or_else(|_| "agapornis-agent.service".into());
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        match tokio::process::Command::new("systemctl")
            .args(["restart", &service])
            .spawn()
        {
            Ok(_) => tracing::info!(
                service,
                "scheduled agent service restart for update activation"
            ),
            Err(error) => {
                tracing::error!(service, error=%error, "failed to restart agent service after staging update")
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activation_and_rollback_preserve_previous_binary() {
        let root =
            std::env::temp_dir().join(format!("agapornis-update-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(root.join("updates")).unwrap();
        let exe = root.join("agapornis-agent");
        let pending = root.join("updates/new.pending");
        std::fs::write(&exe, b"old binary").unwrap();
        std::fs::write(&pending, b"new binary").unwrap();
        std::fs::write(
            root.join("updates/pending-artifact"),
            pending.display().to_string(),
        )
        .unwrap();

        assert_eq!(activate_at(&exe).unwrap(), ActivationOutcome::Activated);
        assert_eq!(std::fs::read(&exe).unwrap(), b"new binary");
        assert_eq!(activate_at(&exe).unwrap(), ActivationOutcome::RolledBack);
        assert_eq!(std::fs::read(&exe).unwrap(), b"old binary");
        let _ = std::fs::remove_dir_all(root);
    }
}
