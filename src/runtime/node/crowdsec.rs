use super::*;

use anyhow::Context;
use chrono::Utc;
use serde_json::Value;
use std::path::Path;
use tokio::process::Command;

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

    let values = match parse_alerts(&raw, maximum) {
        Ok(value) => value,
        Err(error) => return response(true, true, "unavailable", &error.to_string()),
    };
    CrowdSecAlertsResponse {
        enabled: true,
        supported: true,
        status: "active".into(),
        error_message: String::new(),
        collected_at: Utc::now().to_rfc3339(),
        alerts: map_alerts(values, maximum),
    }
}

async fn read_alerts(cli: &str, maximum: usize) -> anyhow::Result<String> {
    let limit = maximum.to_string();
    let args = [
        "alerts",
        "list",
        "-o",
        "json",
        "-a",
        "--limit",
        limit.as_str(),
    ];
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

async fn run_candidate(program: &str, args: [&str; 7]) -> anyhow::Result<String> {
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

fn parse_alerts(raw: &str, maximum: usize) -> anyhow::Result<Vec<Value>> {
    let value: Value = serde_json::from_str(if raw.trim().is_empty() { "[]" } else { raw })
        .context("parse CrowdSec JSON")?;
    let alerts = value
        .as_array()
        .or_else(|| value.get("alerts").and_then(Value::as_array));

    Ok(alerts
        .into_iter()
        .flatten()
        .take(maximum)
        .cloned()
        .collect())
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
        .find_map(|name| match value.get(*name) {
            Some(Value::String(value)) => Some(value.clone()),
            Some(Value::Number(value)) => Some(value.to_string()),
            _ => None,
        })
        .unwrap_or_default()
        .chars()
        .take(512)
        .collect()
}

fn first_text(primary: String, fallback: String) -> String {
    if primary.is_empty() {
        fallback
    } else {
        primary
    }
}

fn map_alerts(values: Vec<Value>, maximum: usize) -> Vec<CrowdSecAlert> {
    let mut mapped = Vec::with_capacity(maximum.min(values.len()));

    for value in values {
        let decisions = value.get("decisions").and_then(Value::as_array);
        if let Some(decisions) = decisions.filter(|decisions| !decisions.is_empty()) {
            for decision in decisions {
                mapped.push(map_alert(&value, decision));
                if mapped.len() == maximum {
                    return mapped;
                }
            }
        } else {
            mapped.push(map_alert(&value, &Value::Null));
            if mapped.len() == maximum {
                return mapped;
            }
        }
    }

    mapped
}

fn map_alert(value: &Value, decision: &Value) -> CrowdSecAlert {
    let source = value.get("source").unwrap_or(&Value::Null);
    let source_scope = first_text(text(source, &["scope"]), text(decision, &["scope"]));
    let source_value = first_text(text(source, &["value"]), text(decision, &["value"]));
    let source_ip = first_text(
        text(source, &["ip"]),
        if source_scope.eq_ignore_ascii_case("ip") {
            source_value.clone()
        } else {
            String::new()
        },
    );

    CrowdSecAlert {
        id: first_text(text(decision, &["id"]), text(value, &["id"])),
        created_at: first_text(
            text(decision, &["created_at", "createdAt"]),
            text(value, &["created_at", "createdAt"]),
        ),
        scenario: first_text(text(value, &["scenario"]), text(decision, &["scenario"])),
        message: first_text(text(value, &["message"]), text(decision, &["origin"])),
        source_scope,
        source_value,
        source_ip,
        source_country: text(source, &["cn", "country"]),
        source_as_name: text(source, &["as_name", "asName"]),
        events_count: value
            .get("events_count")
            .or_else(|| value.get("eventsCount"))
            .and_then(Value::as_i64)
            .unwrap_or(i64::from(!decision.is_null())) as i32,
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
        let alerts = parse_alerts(r#"[{"id":"1"}]"#, 100).unwrap();
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].get("id").and_then(Value::as_str), Some("1"));
    }

    #[test]
    fn parses_nested_alert_array() {
        let alerts = parse_alerts(r#"{"alerts":[{"id":"2"}]}"#, 100).unwrap();
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].get("id").and_then(Value::as_str), Some("2"));
    }

    #[test]
    fn rejects_invalid_json() {
        assert!(parse_alerts("not json", 100).is_err());
    }

    #[test]
    fn limits_crowdsec_json_before_mapping() {
        let alerts = parse_alerts(r#"[{"id":"1"},{"id":"2"},{"id":"3"}]"#, 2).unwrap();

        assert_eq!(alerts.len(), 2);
        assert_eq!(alerts[1].get("id").and_then(Value::as_str), Some("2"));
    }

    #[test]
    fn expands_capi_decisions_and_uses_their_sources() {
        let values = parse_alerts(
            r#"[{"id":10,"scenario":"crowdsecurity/community-blocklist","message":"update : +2/-0 IPs","decisions":[{"id":21,"origin":"CAPI","scope":"Ip","value":"192.0.2.1","type":"ban","duration":"24h"},{"id":22,"origin":"CAPI","scope":"Ip","value":"198.51.100.2","type":"ban","duration":"24h"}]}]"#,
            100,
        )
        .unwrap();
        let alerts = map_alerts(values, 100);

        assert_eq!(alerts.len(), 2);
        assert_eq!(alerts[0].id, "21");
        assert_eq!(alerts[0].source_scope, "Ip");
        assert_eq!(alerts[0].source_value, "192.0.2.1");
        assert_eq!(alerts[0].source_ip, "192.0.2.1");
        assert_eq!(alerts[0].events_count, 1);
        assert_eq!(alerts[0].decision_type, "ban");
        assert_eq!(alerts[1].id, "22");
        assert_eq!(alerts[1].source_ip, "198.51.100.2");
    }

    #[test]
    fn cuts_off_expanded_capi_decisions_at_the_configured_limit() {
        let values = parse_alerts(
            r#"[{"decisions":[{"scope":"Ip","value":"192.0.2.1"},{"scope":"Ip","value":"198.51.100.2"}]}]"#,
            100,
        )
        .unwrap();

        assert_eq!(map_alerts(values, 1).len(), 1);
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
