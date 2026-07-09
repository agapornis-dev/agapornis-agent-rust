use super::*;

use chrono::Utc;
use serde_json::Value;

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
    let raw = match tokio::time::timeout(
        std::time::Duration::from_secs(10),
        process::run(&cli, ["alerts", "list", "-o", "json"]),
    )
    .await
    {
        Ok(Ok(value)) => value,
        Ok(Err(error)) => return response(true, true, "error", &error.to_string()),
        Err(_) => return response(true, true, "error", "CrowdSec query timed out"),
    };

    let values: Vec<Value> = serde_json::from_str(&raw).unwrap_or_default();
    let maximum = std::env::var("AGAPORNIS_CROWDSEC_MAX_ALERTS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(config.crowd_sec_telemetry.max_alerts)
        .clamp(1, 500);
    CrowdSecAlertsResponse {
        enabled: true,
        supported: true,
        status: "available".into(),
        error_message: String::new(),
        collected_at: Utc::now().to_rfc3339(),
        alerts: values.into_iter().take(maximum).map(map_alert).collect(),
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
        remediation: !decision.is_null(),
        decision_type: text(decision, &["type"]),
        decision_duration: text(decision, &["duration"]),
    }
}
