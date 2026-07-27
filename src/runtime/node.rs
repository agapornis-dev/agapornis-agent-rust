//! Host telemetry and optional CrowdSec observations.

use crate::{
    config::DaemonConfig,
    proto::{CrowdSecAlert, CrowdSecAlertsResponse},
};
use anyhow::{Result, bail};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::fs;
use tokio::sync::watch;
use tracing::{debug, warn};

#[path = "node/crowdsec.rs"]
mod crowdsec;
#[path = "node/linux.rs"]
mod linux;
#[path = "node/linux_packages.rs"]
mod linux_packages;

pub use crowdsec::crowdsec;
pub use linux::stats;
pub use linux_packages::{LinuxPackageUpdater, LinuxUpdateResult};

const DEFAULT_STATS_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
const MAX_STATS_SAMPLE_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct NodeStats {
    pub cpu: f64,
    pub memory_used: i64,
    pub memory_total: i64,
    pub disk_used: i64,
    pub disk_total: i64,
    pub uptime: i64,
    pub cpus: i32,
}

struct StatsSnapshot {
    stats: NodeStats,
    sampled_at: Instant,
}

/// Periodically collects host telemetry so gRPC health checks never wait for
/// CPU sampling or filesystem inspection. A failed refresh leaves the last
/// complete snapshot in place instead of publishing partial values.
#[derive(Clone)]
pub struct NodeTelemetry {
    snapshot: watch::Receiver<Arc<StatsSnapshot>>,
    maximum_age: Duration,
}

impl NodeTelemetry {
    pub async fn start() -> Result<Self> {
        let initial = Arc::new(StatsSnapshot {
            stats: stats().await?,
            sampled_at: Instant::now(),
        });
        let (sender, snapshot) = watch::channel(initial);
        let interval = stats_sample_interval();
        let maximum_age = interval.saturating_mul(3).max(Duration::from_secs(5));

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // Tokio intervals tick immediately once. Consume that tick because
            // the initial snapshot was collected immediately above.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let started = Instant::now();
                match stats().await {
                    Ok(stats) => {
                        let elapsed = started.elapsed();
                        if sender
                            .send(Arc::new(StatsSnapshot {
                                stats,
                                sampled_at: Instant::now(),
                            }))
                            .is_err()
                        {
                            return;
                        }
                        debug!(
                            elapsed_ms = elapsed.as_millis(),
                            "host telemetry snapshot refreshed"
                        );
                    }
                    Err(error) => {
                        warn!(error = %error, "host telemetry refresh failed; retaining last snapshot");
                    }
                }
            }
        });

        Ok(Self {
            snapshot,
            maximum_age,
        })
    }

    pub fn snapshot(&self) -> Result<NodeStats> {
        let snapshot = self.snapshot.borrow();
        let age = snapshot.sampled_at.elapsed();
        if age > self.maximum_age {
            bail!(
                "host telemetry snapshot is stale ({} ms old)",
                age.as_millis()
            );
        }
        debug!(
            sample_age_ms = age.as_millis(),
            "serving cached host telemetry"
        );
        Ok(snapshot.stats.clone())
    }
}

fn stats_sample_interval() -> Duration {
    bounded_stats_sample_interval(
        std::env::var("AGAPORNIS_NODE_STATS_SAMPLE_SECONDS")
            .ok()
            .and_then(|value| value.parse().ok()),
    )
}

fn bounded_stats_sample_interval(seconds: Option<u64>) -> Duration {
    seconds
        .filter(|value| *value > 0)
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_STATS_SAMPLE_INTERVAL)
        .min(MAX_STATS_SAMPLE_INTERVAL)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_stats() -> NodeStats {
        NodeStats {
            cpu: 12.5,
            memory_used: 10,
            memory_total: 20,
            disk_used: 30,
            disk_total: 40,
            uptime: 50,
            cpus: 2,
        }
    }

    fn telemetry_with_age(age: Duration, maximum_age: Duration) -> NodeTelemetry {
        let (_sender, snapshot) = watch::channel(Arc::new(StatsSnapshot {
            stats: test_stats(),
            sampled_at: Instant::now() - age,
        }));
        NodeTelemetry {
            snapshot,
            maximum_age,
        }
    }

    #[test]
    fn telemetry_sample_interval_is_bounded() {
        assert_eq!(
            bounded_stats_sample_interval(None),
            DEFAULT_STATS_SAMPLE_INTERVAL
        );
        assert_eq!(
            bounded_stats_sample_interval(Some(0)),
            DEFAULT_STATS_SAMPLE_INTERVAL
        );
        assert_eq!(
            bounded_stats_sample_interval(Some(600)),
            MAX_STATS_SAMPLE_INTERVAL
        );
    }

    #[test]
    fn telemetry_returns_the_last_fresh_complete_snapshot() {
        let telemetry = telemetry_with_age(Duration::from_secs(1), Duration::from_secs(5));
        let snapshot = telemetry.snapshot().expect("fresh telemetry snapshot");

        assert_eq!(snapshot.cpu, 12.5);
        assert_eq!(snapshot.memory_total, 20);
        assert_eq!(snapshot.disk_total, 40);
    }

    #[test]
    fn telemetry_rejects_a_stale_snapshot() {
        let telemetry = telemetry_with_age(Duration::from_secs(6), Duration::from_secs(5));

        assert!(telemetry.snapshot().is_err());
    }
}
