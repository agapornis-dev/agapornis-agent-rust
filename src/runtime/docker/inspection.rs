use super::*;

use bollard::query_parameters::{LogsOptionsBuilder, StatsOptionsBuilder};
use futures_util::StreamExt;

mod disk;

pub use disk::self_test_disk_cache;

impl DockerManager {
    pub async fn inspect(&self, id: &str) -> Result<Value> {
        paths::validate_id(id)?;

        let inspect = self
            .docker
            .inspect_container(id, None)
            .await
            .with_context(|| format!("inspect Docker container {id}"))?;

        serde_json::to_value(inspect).context("serialize Docker inspect response")
    }

    pub async fn root(&self, id: &str) -> Result<(PathBuf, String, bool, bool)> {
        paths::validate_id(id)?;

        let fallback = paths::server_dir(id)?;

        let inspect = match self.inspect(id).await {
            Ok(value) => value,
            Err(_) => {
                return Ok((fallback, paths::HOME_CONTAINER_PATH.into(), false, true));
            }
        };

        let data = inspect
            .pointer("/Config/Labels/agapornis.data_path")
            .and_then(Value::as_str)
            .unwrap_or(paths::HOME_CONTAINER_PATH)
            .to_owned();

        let running = inspect
            .pointer("/State/Running")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        let mounts = inspect.get("Mounts").and_then(Value::as_array);

        let exact = mounts
            .and_then(|mounts| {
                mounts.iter().find(|mount| {
                    mount.get("Destination").and_then(Value::as_str) == Some(data.as_str())
                })
            })
            .and_then(|mount| mount.get("Source"))
            .and_then(Value::as_str)
            .map(PathBuf::from);

        let known = mounts
            .and_then(|mounts| {
                mounts.iter().find(|mount| {
                    matches!(
                        mount.get("Destination").and_then(Value::as_str),
                        Some("/data") | Some("/home/container")
                    )
                })
            })
            .and_then(|mount| mount.get("Source"))
            .and_then(Value::as_str)
            .map(PathBuf::from);

        let exact_mount_found = exact.is_some();

        Ok((
            exact.or(known).unwrap_or(fallback),
            data,
            running,
            exact_mount_found,
        ))
    }

    pub async fn metrics(&self, id: &str) -> Result<Metrics> {
        paths::validate_id(id)?;

        let inspect = match self.inspect(id).await {
            Ok(value) => value,
            Err(_) => {
                return Ok(Metrics {
                    status: "deleted".into(),
                    disk_limit: DEFAULT_DISK_LIMIT,
                    ..Default::default()
                });
            }
        };

        let container_running =
            inspect.pointer("/State/Running").and_then(Value::as_bool) == Some(true);
        let uptime_seconds = if container_running {
            container_uptime_seconds(&inspect)
        } else {
            0
        };
        let mut status = if container_running {
            "running".into()
        } else {
            inspect
                .pointer("/State/Status")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .into()
        };

        if let Some(value) = self.protection.status(id) {
            status = value;
        }

        if status == "running" && !self.startup_is_ready(id, &inspect).await {
            status = "starting".into();
        }

        let (disk_usage, disk_limit) = self.disk(id).await?;

        // `status` is a presentation/protection state. A container may be
        // actively consuming resources while reported as `starting` or under
        // another agent state, so gate Docker stats on the actual runtime bit.
        if let Some(metrics) = inactive_metrics(
            container_running,
            status.clone(),
            disk_usage,
            disk_limit,
            uptime_seconds,
        ) {
            return Ok(metrics);
        }

        let options = StatsOptionsBuilder::default()
            .stream(false)
            // Waiting for Docker's normal two-sample calculation gives
            // precpu_stats values suitable for calculating CPU percentage.
            .one_shot(false)
            .build();

        let mut stream = self.docker.stats(id, Some(options));

        let stat = stream
            .next()
            .await
            .context("Docker stats stream ended without a sample")?
            .with_context(|| format!("read stats for Docker container {id}"))?;

        /*
         * Docker CLI reports memory usage with inactive file cache removed.
         *
         * cgroups v1 generally exposes `total_inactive_file`.
         * cgroups v2 generally exposes `inactive_file`.
         */
        let mut resources = resource_metrics(&stat);
        let nano_cpus = inspect.pointer("/HostConfig/NanoCpus").and_then(Value::as_i64).unwrap_or(0);
        resources.cpu_percent = normalized_cpu_percent(resources.cpu_percent, nano_cpus);

        Ok(Metrics {
            memory_usage: resources.memory_usage,
            memory_limit: resources.memory_limit,
            cpu_percent: resources.cpu_percent,
            network_read: resources.network_read,
            network_write: resources.network_write,
            disk_usage,
            disk_limit,
            status,
            uptime_seconds,
        })
    }

    async fn startup_is_ready(&self, id: &str, inspect: &Value) -> bool {
        let marker = inspect
            .pointer("/Config/Labels/agapornis.startup_done")
            .and_then(Value::as_str)
            .unwrap_or("");

        if marker.is_empty() {
            return true;
        }
        if self.startup_ready.lock().await.contains(id) {
            return true;
        }

        let since = {
            let mut checks = self.startup_checks.lock().await;
            if let Some((last, since)) = checks.get(id).copied() {
                if last.elapsed() < Duration::from_secs(5) {
                    return false;
                }
                checks.insert(id.to_owned(), (Instant::now(), unix_timestamp()));
                since
            } else {
                let started = inspect
                    .pointer("/State/StartedAt")
                    .and_then(Value::as_str)
                    .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
                    .map(|value| clamp_timestamp(value.timestamp()))
                    .unwrap_or_else(|| unix_timestamp().saturating_sub(5));
                checks.insert(id.to_owned(), (Instant::now(), unix_timestamp()));
                started
            }
        };

        let options = LogsOptionsBuilder::default()
            .follow(false)
            .stdout(true)
            .stderr(true)
            .since(since)
            .tail("all")
            .build();
        let mut logs = self.docker.logs(id, Some(options));
        let mut found = false;
        while let Some(item) = logs.next().await {
            match item {
                Ok(line) => {
                    if String::from_utf8_lossy(&line.into_bytes()).contains(marker) {
                        found = true;
                        break;
                    }
                }
                Err(_) => break,
            }
        }

        if found {
            self.startup_ready.lock().await.insert(id.to_owned());
            self.startup_checks.lock().await.remove(id);
            true
        } else {
            false
        }
    }
}

#[derive(Debug, Default, PartialEq)]
struct ResourceMetrics {
    memory_usage: i64,
    memory_limit: i64,
    cpu_percent: f64,
    network_read: i64,
    network_write: i64,
}

fn inactive_metrics(
    container_running: bool,
    status: String,
    disk_usage: i64,
    disk_limit: i64,
    uptime_seconds: i64,
) -> Option<Metrics> {
    (!container_running).then_some(Metrics {
        status,
        disk_usage,
        disk_limit,
        uptime_seconds,
        ..Default::default()
    })
}

fn resource_metrics(stat: &bollard::models::ContainerStatsResponse) -> ResourceMetrics {
    let (memory_usage, memory_limit) = stat
        .memory_stats
        .as_ref()
        .map(|memory| {
            let raw_usage = memory.usage.unwrap_or(0);
            let cache = memory
                .stats
                .as_ref()
                .and_then(|stats| {
                    stats
                        .get("total_inactive_file")
                        .or_else(|| stats.get("inactive_file"))
                })
                .copied()
                .unwrap_or(0);
            (
                saturating_i64(raw_usage.saturating_sub(cache)),
                saturating_i64(memory.limit.unwrap_or(0)),
            )
        })
        .unwrap_or((0, 0));

    let (network_read, network_write) = stat
        .networks
        .as_ref()
        .map(|networks| {
            networks
                .values()
                .fold((0_u64, 0_u64), |(read, write), network| {
                    (
                        read.saturating_add(network.rx_bytes.unwrap_or(0)),
                        write.saturating_add(network.tx_bytes.unwrap_or(0)),
                    )
                })
        })
        .map(|(read, write)| (saturating_i64(read), saturating_i64(write)))
        .unwrap_or((0, 0));

    ResourceMetrics {
        memory_usage,
        memory_limit,
        cpu_percent: calculate_cpu_percent(stat),
        network_read,
        network_write,
    }
}

fn unix_timestamp() -> i32 {
    clamp_timestamp(chrono::Utc::now().timestamp())
}

fn container_uptime_seconds(inspect: &Value) -> i64 {
    inspect
        .pointer("/State/StartedAt")
        .and_then(Value::as_str)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .map(|started| {
            chrono::Utc::now()
                .signed_duration_since(started.with_timezone(&chrono::Utc))
                .num_seconds()
                .max(0)
        })
        .unwrap_or(0)
}

fn clamp_timestamp(value: i64) -> i32 {
    value.clamp(i32::MIN as i64, i32::MAX as i64) as i32
}

fn calculate_cpu_percent(stat: &bollard::models::ContainerStatsResponse) -> f64 {
    let Some(current) = stat.cpu_stats.as_ref() else {
        return 0.0;
    };

    let Some(previous) = stat.precpu_stats.as_ref() else {
        return 0.0;
    };

    let current_total = current
        .cpu_usage
        .as_ref()
        .and_then(|usage| usage.total_usage)
        .unwrap_or(0);

    let previous_total = previous
        .cpu_usage
        .as_ref()
        .and_then(|usage| usage.total_usage)
        .unwrap_or(0);

    let current_system = current.system_cpu_usage.unwrap_or(0);

    let previous_system = previous.system_cpu_usage.unwrap_or(0);

    let cpu_delta = current_total.saturating_sub(previous_total);

    let system_delta = current_system.saturating_sub(previous_system);

    if cpu_delta == 0 || system_delta == 0 {
        return 0.0;
    }

    let online_cpus = current
        .online_cpus
        .map(u64::from)
        .or_else(|| {
            current
                .cpu_usage
                .as_ref()
                .and_then(|usage| usage.percpu_usage.as_ref())
                .map(|usage| usage.len() as u64)
        })
        .filter(|count| *count > 0)
        .unwrap_or(1);

    (cpu_delta as f64 / system_delta as f64) * online_cpus as f64 * 100.0
}

fn normalized_cpu_percent(raw_percent: f64, nano_cpus: i64) -> f64 {
    let allocated_cpus = nano_cpus as f64 / 1_000_000_000.0;
    if allocated_cpus > 0.0 {
        (raw_percent / allocated_cpus).clamp(0.0, 100.0)
    } else {
        raw_percent.clamp(0.0, 100.0)
    }
}

fn saturating_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn running_startup_state_still_collects_live_resources() {
        assert!(inactive_metrics(true, "starting".into(), 10, 20, 30).is_none());
        let stopped = inactive_metrics(false, "exited".into(), 10, 20, 0).unwrap();
        assert_eq!(stopped.status, "exited");
        assert_eq!(stopped.disk_usage, 10);
    }

    #[test]
    fn docker_sample_maps_cpu_memory_and_all_network_interfaces() {
        let sample: bollard::models::ContainerStatsResponse = serde_json::from_value(json!({
            "cpu_stats": {
                "cpu_usage": { "total_usage": 300 },
                "system_cpu_usage": 2000,
                "online_cpus": 2
            },
            "precpu_stats": {
                "cpu_usage": { "total_usage": 100 },
                "system_cpu_usage": 1000
            },
            "memory_stats": {
                "usage": 1000,
                "limit": 2000,
                "stats": { "inactive_file": 100 }
            },
            "networks": {
                "eth0": { "rx_bytes": 400, "tx_bytes": 500 },
                "eth1": { "rx_bytes": 40, "tx_bytes": 50 }
            }
        }))
        .unwrap();

        assert_eq!(
            resource_metrics(&sample),
            ResourceMetrics {
                memory_usage: 900,
                memory_limit: 2000,
                cpu_percent: 40.0,
                network_read: 440,
                network_write: 550,
            }
        );
    }

    #[test]
    fn cpu_usage_is_relative_to_the_allocated_quota() {
        assert_eq!(normalized_cpu_percent(200.0, 2_000_000_000), 100.0);
        assert_eq!(normalized_cpu_percent(100.0, 2_000_000_000), 50.0);
        assert_eq!(normalized_cpu_percent(125.0, 1_000_000_000), 100.0);
    }
}
