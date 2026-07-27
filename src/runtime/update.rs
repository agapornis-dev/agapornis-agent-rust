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
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).await?;
        }
        let marker = dir.join("pending-artifact");
        if pending_update_at(&exe)?.is_some() {
            bail!("a verified agent update is already staged")
        }
        if marker.exists() {
            fs::remove_file(&marker)
                .await
                .context("remove empty pending agent update marker")?;
        }
        let target = dir.join(format!("agapornis-agent-{}.pending", uuid::Uuid::new_v4()));
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&target)
            .await?;
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
        if size == 0 {
            let _ = fs::remove_file(&target).await;
            bail!("update artifact is empty")
        }
        file.sync_all().await?;
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
        let marker_temp = dir.join(format!(".pending-artifact-{}.tmp", uuid::Uuid::new_v4()));
        let mut marker_file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&marker_temp)
            .await?;
        marker_file
            .write_all(target.display().to_string().as_bytes())
            .await?;
        marker_file.sync_all().await?;
        drop(marker_file);
        if let Err(error) = fs::rename(&marker_temp, &marker).await {
            let _ = fs::remove_file(&marker_temp).await;
            let _ = fs::remove_file(&target).await;
            return Err(error).context("commit pending agent update marker");
        }
        Ok(UpdateResult {
            message: "Update staged. Restart the agent service to activate it.".into(),
            staged: target.display().to_string(),
            restart_required: true,
        })
    }

    pub fn activate_pending(&self) -> Result<ActivationOutcome> {
        activate_at(&std::env::current_exe()?)
    }

    pub fn restart_pending(&self) -> Result<UpdateResult> {
        let exe = std::env::current_exe()?;
        let pending =
            pending_update_at(&exe)?.context("no verified agent update is staged for restart")?;
        let service = validate_systemd_service()?;
        schedule_service_restart(service);
        Ok(UpdateResult {
            message: "Agent restart scheduled. The staged update will be activated safely.".into(),
            staged: pending.display().to_string(),
            restart_required: true,
        })
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
    let canonical_pending =
        pending_update_at(exe)?.context("pending agent update marker is empty")?;
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

fn pending_update_at(exe: &Path) -> Result<Option<PathBuf>> {
    let dir = staging(exe);
    let marker = dir.join("pending-artifact");
    if !marker.exists() {
        return Ok(None);
    }
    let value = std::fs::read_to_string(&marker)?;
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    let canonical_dir = std::fs::canonicalize(&dir)?;
    let canonical_pending = std::fs::canonicalize(value)?;
    if canonical_pending.parent() != Some(canonical_dir.as_path()) || !canonical_pending.is_file() {
        bail!("pending agent update is outside the update staging directory")
    }
    Ok(Some(canonical_pending))
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

fn schedule_service_restart(service: String) {
    /*
     * The Linux systemd unit is the long-lived supervisor. The running
     * process cannot replace itself in memory, so staging writes the new
     * executable first and asks systemd to restart the unit. At the next
     * launch main activates the pending binary; systemd then continues to
     * provide restart policy, logging, and service lifetime management.
     */
    tokio::spawn(async move {
        // Leave enough time for the successful gRPC response to traverse the
        // API and panel proxy before systemd terminates this process.
        tokio::time::sleep(Duration::from_secs(5)).await;
        match tokio::process::Command::new("systemctl")
            .args(["--no-block", "restart", &service])
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

fn validate_systemd_service() -> Result<String> {
    #[cfg(not(unix))]
    bail!("safe update restart is supported only by the systemd service");

    #[cfg(unix)]
    {
        let service = std::env::var("AGAPORNIS_UPDATE_SYSTEMD_SERVICE")
            .unwrap_or_else(|_| "agapornis-agent.service".into());
        if service.is_empty()
            || service.len() > 255
            || !service
                .bytes()
                .all(|value| value.is_ascii_alphanumeric() || b"@_.:-".contains(&value))
        {
            bail!("configured agent systemd service name is invalid")
        }
        let output = std::process::Command::new("systemctl")
            .args(["show", "--property=LoadState", "--value", &service])
            .output()
            .context("systemctl is unavailable for safe update restart")?;
        if !output.status.success() || String::from_utf8_lossy(&output.stdout).trim() != "loaded" {
            bail!("agent systemd service is not loaded; restart the staged update manually")
        }
        Ok(service)
    }
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

    #[test]
    fn pending_restart_requires_a_confined_staged_artifact() {
        let root = std::env::temp_dir().join(format!(
            "agapornis-update-guard-test-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(root.join("updates")).unwrap();
        let exe = root.join("agapornis-agent");
        let outside = root.join("outside.pending");
        std::fs::write(&exe, b"old binary").unwrap();
        std::fs::write(&outside, b"new binary").unwrap();
        std::fs::write(
            root.join("updates/pending-artifact"),
            outside.display().to_string(),
        )
        .unwrap();

        assert!(pending_update_at(&exe).is_err());

        let staged = root.join("updates/agent.pending");
        std::fs::write(&staged, b"new binary").unwrap();
        std::fs::write(
            root.join("updates/pending-artifact"),
            staged.display().to_string(),
        )
        .unwrap();
        assert_eq!(
            pending_update_at(&exe).unwrap(),
            Some(staged.canonicalize().unwrap())
        );
        let _ = std::fs::remove_dir_all(root);
    }
}
