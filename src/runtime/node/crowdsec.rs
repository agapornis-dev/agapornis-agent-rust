use super::*;

use anyhow::Context;
use chrono::Utc;
use serde_json::Value;
use std::path::Path;
use tokio::process::Command;

const MAXIMUM_OUTPUT_BYTES: usize = 4 * 1024 * 1024;

pub async fn crowdsec(config: &DaemonConfig) -> CrowdSecAlertsResponse {
    let enabled = std::env::var("AGAPORNIS_CROWDSEC_ENABLED")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(config.crowd_sec_telemetry.enabled);
    if !enabled {
        return response(false, cfg!(target_os = "linux"), "disabled", "");
    }
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
    let maximum = std::env::var("AGAPORNIS_CROWDSEC_MAX_ALERTS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(config.crowd_sec_telemetry.max_alerts)
        .clamp(1, 500);
    let raw = match tokio::time::timeout(
        std::time::Duration::from_secs(10),
        read_alerts(&cli, maximum),
    )
    .await
    {
        Ok(Ok(value)) => value,
        Ok(Err(error)) => return response(true, true, "unavailable", &error.to_string()),
        Err(_) => return response(true, true, "unavailable", "CrowdSec query timed out"),
    };

    if raw.len() > MAXIMUM_OUTPUT_BYTES {
        return response(
            true,
            true,
            "unavailable",
            "CrowdSec output exceeded the 4 MiB safety limit",
        );
    }

    let values = match parse_alerts(&raw) {
        Ok(value) => value,
        Err(error) => return response(true, true, "unavailable", &error.to_string()),
    };
    CrowdSecAlertsResponse {
        enabled: true,
        supported: true,
        status: "active".into(),
        error_message: String::new(),
        collected_at: Utc::now().to_rfc3339(),
        alerts: values.into_iter().take(maximum).map(map_alert).collect(),
    }
}

async fn read_alerts(cli: &str, maximum: usize) -> anyhow::Result<String> {
    let limit = maximum.to_string();
    let args = ["alerts", "list", "-o", "json", "--limit", limit.as_str()];
    let candidates = cscli_candidates(cli);
    let mut last_error = None;

    for candidate in candidates {
        match run_candidate(&candidate, args).await {
            Ok(value) => return Ok(value),
            Err(error) => last_error = Some(error),
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("cscli path is empty")))
}

async fn run_candidate(program: &str, args: [&str; 6]) -> anyhow::Result<String> {
    let output = Command::new(program)
        .args(args)
        .kill_on_drop(true)
        .output()
        .await
        .with_context(|| format!("start {program}"))?;
    if !output.status.success() {
        let error = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        anyhow::bail!(
            "{program} exited with {}: {error}",
            output.status.code().unwrap_or(-1)
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

fn cscli_candidates(cli: &str) -> Vec<String> {
    let cli = cli.trim();
    if cli.is_empty() {
        return vec![];
    }
    let has_separator = cli.contains('/') || cli.contains('\\');
    if has_separator || Path::new(cli).is_absolute() || cli != "cscli" {
        return vec![cli.to_owned()];
    }
    vec![
        "cscli".into(),
        "/usr/bin/cscli".into(),
        "/usr/local/bin/cscli".into(),
        "/snap/bin/cscli".into(),
    ]
}

fn parse_alerts(raw: &str) -> anyhow::Result<Vec<Value>> {
    let value: Value = serde_json::from_str(if raw.trim().is_empty() { "[]" } else { raw })
        .context("parse CrowdSec JSON")?;
    if let Some(alerts) = value.as_array() {
        return Ok(alerts.clone());
    }
    Ok(value
        .get("alerts")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default())
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

fn text(value: &Value, names: &[&str]) -> String {
    names
        .iter()
        .find_map(|name| value.get(*name).and_then(Value::as_str))
        .unwrap_or("")
        .chars()
        .take(512)
        .collect()
}

fn map_alert(value: Value) -> CrowdSecAlert {
    let source = value.get("source").unwrap_or(&Value::Null);
    let decision = value
        .get("decisions")
        .and_then(Value::as_array)
        .and_then(|decisions| decisions.first())
        .unwrap_or(&Value::Null);
    CrowdSecAlert {
        id: text(&value, &["id"]),
        created_at: text(&value, &["created_at", "createdAt"]),
        scenario: text(&value, &["scenario"]),
        message: text(&value, &["message"]),
        source_scope: text(source, &["scope"]),
        source_value: text(source, &["value"]),
        source_ip: text(source, &["ip"]),
        source_country: text(source, &["cn", "country"]),
        source_as_name: text(source, &["as_name", "asName"]),
        events_count: value
            .get("events_count")
            .or_else(|| value.get("eventsCount"))
            .and_then(Value::as_i64)
            .unwrap_or(0) as i32,
        simulated: value
            .get("simulated")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        remediation: value
            .get("remediation")
            .and_then(Value::as_bool)
            .unwrap_or(!decision.is_null()),
        decision_type: text(decision, &["type"]),
        decision_duration: text(decision, &["duration"]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_top_level_alert_array() {
        let alerts = parse_alerts(r#"[{"id":"1"}]"#).unwrap();
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].get("id").and_then(Value::as_str), Some("1"));
    }

    #[test]
    fn parses_nested_alert_array() {
        let alerts = parse_alerts(r#"{"alerts":[{"id":"2"}]}"#).unwrap();
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].get("id").and_then(Value::as_str), Some("2"));
    }

    #[test]
    fn rejects_invalid_json() {
        assert!(parse_alerts("not json").is_err());
    }

    #[test]
    fn expands_default_cscli_candidates() {
        let candidates = cscli_candidates("cscli");
        assert!(candidates.contains(&"cscli".to_owned()));
        assert!(candidates.contains(&"/usr/bin/cscli".to_owned()));
    }

    #[test]
    fn preserves_explicit_cscli_path() {
        assert_eq!(
            cscli_candidates("/opt/crowdsec/bin/cscli"),
            vec!["/opt/crowdsec/bin/cscli"]
        );
    }
}
