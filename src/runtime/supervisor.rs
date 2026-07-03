//! Background disk enforcement and container observation loops.

use crate::{docker::DockerManager, process, protection::ProtectionState, services::ConsoleHub};
use serde_json::Value;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};
use tracing::{info, warn};

struct Observation {
    last_restart: i64,
    was_running: bool,
    running_since: Option<Instant>,
    failures: VecDeque<Instant>,
    quarantined: bool,
}
pub fn spawn(
    docker: Arc<DockerManager>,
    protection: Arc<ProtectionState>,
    console: Arc<ConsoleHub>,
) {
    spawn_image_cleanup();
    tokio::spawn(async move {
        let scan = env_secs("AGAPORNIS_PROTECTION_SCAN_SECONDS", 5);
        let disk_every = Duration::from_secs(env_secs("AGAPORNIS_DISK_CHECK_SECONDS", 150));
        let mut observations: HashMap<String, Observation> = HashMap::new();
        let mut disk_due: HashMap<String, Instant> = HashMap::new();
        loop {
            if let Err(e) = scan_once(
                &docker,
                &protection,
                &console,
                &mut observations,
                &mut disk_due,
                disk_every,
            )
            .await
            {
                warn!(error=%e,"runtime protection scan failed")
            }
            tokio::time::sleep(Duration::from_secs(scan)).await;
        }
    });
}

fn spawn_image_cleanup() {
    if std::env::var("AGAPORNIS_DOCKER_IMAGE_CLEANUP_ENABLED")
        .is_ok_and(|value| value.eq_ignore_ascii_case("false") || value == "0")
    {
        info!("automatic dangling Docker image cleanup disabled");
        return;
    }
    tokio::spawn(async move {
        let initial_delay = env_secs("AGAPORNIS_DOCKER_IMAGE_CLEANUP_INITIAL_DELAY_SECONDS", 60);
        let interval = env_secs(
            "AGAPORNIS_DOCKER_IMAGE_CLEANUP_INTERVAL_SECONDS",
            6 * 60 * 60,
        );
        let minimum_age_hours = env_secs("AGAPORNIS_DOCKER_IMAGE_CLEANUP_MIN_AGE_HOURS", 24);
        tokio::time::sleep(Duration::from_secs(initial_delay)).await;
        loop {
            let arguments = image_cleanup_arguments(minimum_age_hours);
            let references: Vec<&str> = arguments.iter().map(String::as_str).collect();
            match process::docker(references).await {
                Ok(output) => {
                    info!(minimum_age_hours, output=%output.trim(), "dangling Docker image cleanup completed")
                }
                Err(error) => warn!(error=%error, "dangling Docker image cleanup failed"),
            }
            tokio::time::sleep(Duration::from_secs(interval)).await;
        }
    });
}

fn image_cleanup_arguments(minimum_age_hours: u64) -> Vec<String> {
    vec![
        "image".into(),
        "prune".into(),
        "--force".into(),
        "--filter".into(),
        "dangling=true".into(),
        "--filter".into(),
        format!("until={minimum_age_hours}h"),
    ]
}
async fn scan_once(
    docker: &Arc<DockerManager>,
    protection: &Arc<ProtectionState>,
    console: &Arc<ConsoleHub>,
    observations: &mut HashMap<String, Observation>,
    disk_due: &mut HashMap<String, Instant>,
    disk_every: Duration,
) -> anyhow::Result<()> {
    let raw = process::docker([
        "ps",
        "-a",
        "--filter",
        "label=agapornis.server_id",
        "--format",
        "{{.Names}}",
    ])
    .await?;
    for id in raw.lines().filter(|v| !v.trim().is_empty()) {
        let inspect = docker.inspect(id).await?;
        let restart = inspect
            .get("RestartCount")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let running = inspect
            .pointer("/State/Running")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let exit = inspect
            .pointer("/State/ExitCode")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        observe(
            id,
            restart,
            running,
            exit,
            docker,
            protection,
            console,
            observations,
        )
        .await?;
        let due = disk_due.entry(id.into()).or_insert_with(Instant::now);
        if *due <= Instant::now() {
            *due = Instant::now() + disk_every;
            let (usage, limit) = docker.disk_force(id).await?;
            if limit > 0 && usage > limit {
                let _ = process::docker(["update", "--restart", "no", id]).await;
                let _ = process::docker(["stop", "--time", "5", id]).await;
                protection.mark(id, "disk-limit-exceeded");
                console.publish(id,format!("[agent] Disk limit exceeded ({usage} / {limit} bytes). The server was stopped and cannot start until files are deleted.")).await
            } else {
                protection.clear_disk(id)
            }
        }
    }
    Ok(())
}
#[allow(clippy::too_many_arguments)] // mirrors the complete Docker observation tuple plus shared monitor state.
async fn observe(
    id: &str,
    restart: i64,
    running: bool,
    exit: i64,
    docker: &Arc<DockerManager>,
    protection: &Arc<ProtectionState>,
    console: &Arc<ConsoleHub>,
    all: &mut HashMap<String, Observation>,
) -> anyhow::Result<()> {
    let now = Instant::now();
    let state = all.entry(id.into()).or_insert_with(|| Observation {
        last_restart: restart,
        was_running: running,
        running_since: running.then_some(now),
        failures: VecDeque::new(),
        quarantined: false,
    });
    if protection.in_manual_recovery(id) {
        state.last_restart = restart;
        state.was_running = running;
        state.running_since = running.then_some(now);
        state.failures.clear();
        state.quarantined = false;
        return Ok(());
    }
    for _ in 0..(restart - state.last_restart).clamp(0, 3) {
        state.failures.push_back(now)
    }
    if state.was_running && !running && exit != 0 {
        state.failures.push_back(now)
    }
    while state
        .failures
        .front()
        .is_some_and(|v| v.elapsed() > Duration::from_secs(300))
    {
        state.failures.pop_front();
    }
    if running {
        let since = state.running_since.get_or_insert(now);
        if since.elapsed() >= Duration::from_secs(120) {
            state.failures.clear();
        }
    } else {
        state.running_since = None
    }
    state.last_restart = restart;
    state.was_running = running;
    if !state.quarantined && state.failures.len() >= 3 {
        state.quarantined = true;
        let _ = process::docker(["update", "--restart", "no", id]).await;
        let _ = docker.stop(id).await;
        protection.mark(id, "crash-loop-protected");
        console.publish(id,"[agent] Crash-loop protection engaged after 3 failures in 5 minutes. Automatic restarts are disabled; use Start or Restart after correcting the cause.".into()).await
    }
    Ok(())
}
fn env_secs(name: &str, fallback: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(fallback)
}

#[cfg(test)]
mod tests {
    use super::image_cleanup_arguments;

    #[test]
    fn dangling_image_cleanup_is_scoped_and_age_gated() {
        assert_eq!(
            image_cleanup_arguments(24),
            [
                "image",
                "prune",
                "--force",
                "--filter",
                "dangling=true",
                "--filter",
                "until=24h"
            ]
        );
    }
}
