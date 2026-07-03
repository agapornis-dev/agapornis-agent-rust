//! Host telemetry and optional CrowdSec observations.

use crate::{
    config::DaemonConfig,
    process,
    proto::{CrowdSecAlert, CrowdSecAlertsResponse},
};
use anyhow::Result;
use chrono::Utc;
use serde_json::Value;
use tokio::fs;

pub struct NodeStats {
    pub cpu: f64,
    pub memory_used: i64,
    pub memory_total: i64,
    pub disk_used: i64,
    pub disk_total: i64,
    pub uptime: i64,
    pub cpus: i32,
}
pub async fn stats() -> Result<NodeStats> {
    let first = cpu_sample().await?;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let second = cpu_sample().await?;
    let cpu = if second.1 > first.1 {
        (1.0 - (second.0 - first.0) as f64 / (second.1 - first.1) as f64) * 100.0
    } else {
        0.0
    };
    let text = fs::read_to_string("/proc/meminfo")
        .await
        .unwrap_or_default();
    let mut total = 0;
    let mut available = 0;
    for line in text.lines() {
        if line.starts_with("MemTotal:") {
            total = parse_kb(line)
        }
        if line.starts_with("MemAvailable:") {
            available = parse_kb(line)
        }
    }
    let (disk_total, disk_used) = disk_stats().await;
    let uptime = fs::read_to_string("/proc/uptime")
        .await
        .ok()
        .and_then(|v| v.split_whitespace().next()?.parse::<f64>().ok())
        .unwrap_or(0.0) as i64;
    Ok(NodeStats {
        cpu: cpu.clamp(0.0, 100.0),
        memory_used: (total - available).max(0),
        memory_total: total,
        disk_used,
        disk_total,
        uptime,
        cpus: std::thread::available_parallelism()
            .map(|v| v.get() as i32)
            .unwrap_or(1),
    })
}
async fn cpu_sample() -> Result<(u64, u64)> {
    let text = fs::read_to_string("/proc/stat").await.unwrap_or_default();
    let values: Vec<u64> = text
        .lines()
        .next()
        .unwrap_or("")
        .split_whitespace()
        .skip(1)
        .filter_map(|v| v.parse().ok())
        .collect();
    let total = values.iter().sum();
    let idle = values.get(3).copied().unwrap_or(0);
    Ok((idle, total))
}
fn parse_kb(line: &str) -> i64 {
    line.split_whitespace()
        .nth(1)
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0)
        * 1024
}
async fn disk_stats() -> (i64, i64) {
    let out = process::run("df", ["-B1", "--output=size,used", "/"])
        .await
        .unwrap_or_default();
    let nums: Vec<i64> = out
        .lines()
        .last()
        .unwrap_or("")
        .split_whitespace()
        .filter_map(|v| v.parse().ok())
        .collect();
    (
        nums.first().copied().unwrap_or(0),
        nums.get(1).copied().unwrap_or(0),
    )
}

pub async fn crowdsec(config: &DaemonConfig) -> CrowdSecAlertsResponse {
    let enabled = std::env::var("AGAPORNIS_CROWDSEC_ENABLED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(config.crowd_sec_telemetry.enabled);
    if !enabled {
        return response(false, cfg!(target_os = "linux"), "disabled", "");
    };
    if !cfg!(target_os = "linux") {
        return response(
            true,
            false,
            "unsupported",
            "CrowdSec telemetry is only supported on Linux",
        );
    }
    let cli = std::env::var("AGAPORNIS_CROWDSEC_CLI_PATH")
        .unwrap_or_else(|_| config.crowd_sec_telemetry.cscli_path.clone());
    let raw = match tokio::time::timeout(
        std::time::Duration::from_secs(10),
        process::run(&cli, ["alerts", "list", "-o", "json"]),
    )
    .await
    {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => return response(true, true, "error", &e.to_string()),
        Err(_) => return response(true, true, "error", "CrowdSec query timed out"),
    };
    let values: Vec<Value> = serde_json::from_str(&raw).unwrap_or_default();
    let max = std::env::var("AGAPORNIS_CROWDSEC_MAX_ALERTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(config.crowd_sec_telemetry.max_alerts)
        .clamp(1, 500);
    let alerts = values.into_iter().take(max).map(map_alert).collect();
    CrowdSecAlertsResponse {
        enabled: true,
        supported: true,
        status: "available".into(),
        error_message: "".into(),
        collected_at: Utc::now().to_rfc3339(),
        alerts,
    }
}
fn response(enabled: bool, supported: bool, status: &str, error: &str) -> CrowdSecAlertsResponse {
    CrowdSecAlertsResponse {
        enabled,
        supported,
        status: status.into(),
        error_message: error.into(),
        collected_at: Utc::now().to_rfc3339(),
        alerts: vec![],
    }
}
fn text(v: &Value, names: &[&str]) -> String {
    names
        .iter()
        .find_map(|n| v.get(*n).and_then(|x| x.as_str()))
        .unwrap_or("")
        .chars()
        .take(512)
        .collect()
}
fn map_alert(v: Value) -> CrowdSecAlert {
    let source = v.get("source").unwrap_or(&Value::Null);
    let decisions = v
        .get("decisions")
        .and_then(Value::as_array)
        .and_then(|v| v.first())
        .unwrap_or(&Value::Null);
    CrowdSecAlert {
        id: text(&v, &["id"]),
        created_at: text(&v, &["created_at", "createdAt"]),
        scenario: text(&v, &["scenario"]),
        message: text(&v, &["message"]),
        source_scope: text(source, &["scope"]),
        source_value: text(source, &["value"]),
        source_ip: text(source, &["ip"]),
        source_country: text(source, &["cn", "country"]),
        source_as_name: text(source, &["as_name", "asName"]),
        events_count: v
            .get("events_count")
            .or_else(|| v.get("eventsCount"))
            .and_then(Value::as_i64)
            .unwrap_or(0) as i32,
        simulated: v.get("simulated").and_then(Value::as_bool).unwrap_or(false),
        remediation: !decisions.is_null(),
        decision_type: text(decisions, &["type"]),
        decision_duration: text(decisions, &["duration"]),
    }
}
