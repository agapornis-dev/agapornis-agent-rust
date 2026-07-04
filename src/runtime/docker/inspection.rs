use super::*;

use bollard::query_parameters::StatsOptionsBuilder;
use futures_util::StreamExt;

impl DockerManager {
    pub async fn inspect(&self, id: &str) -> Result<Value> {
        paths::validate_id(id)?;

        let inspect = self
            .docker
            .inspect_container(id, None)
            .await
            .with_context(|| format!("inspect Docker container {id}"))?;

        serde_json::to_value(inspect)
            .context("serialize Docker inspect response")
    }

    pub async fn root(
        &self,
        id: &str,
    ) -> Result<(PathBuf, String, bool, bool)> {
        paths::validate_id(id)?;

        let fallback = paths::server_dir(id)?;

        let inspect = match self.inspect(id).await {
            Ok(value) => value,
            Err(_) => {
                return Ok((
                    fallback,
                    paths::HOME_CONTAINER_PATH.into(),
                    false,
                    true,
                ));
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

        let mounts = inspect
            .get("Mounts")
            .and_then(Value::as_array);

        let exact = mounts
            .and_then(|mounts| {
                mounts.iter().find(|mount| {
                    mount
                        .get("Destination")
                        .and_then(Value::as_str)
                        == Some(data.as_str())
                })
            })
            .and_then(|mount| mount.get("Source"))
            .and_then(Value::as_str)
            .map(PathBuf::from);

        let known = mounts
            .and_then(|mounts| {
                mounts.iter().find(|mount| {
                    matches!(
                        mount
                            .get("Destination")
                            .and_then(Value::as_str),
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

        let mut status = if inspect
            .pointer("/State/Running")
            .and_then(Value::as_bool)
            == Some(true)
        {
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

        let (disk_usage, disk_limit) = self.disk(id).await?;

        if status != "running" {
            return Ok(Metrics {
                status,
                disk_usage,
                disk_limit,
                ..Default::default()
            });
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
            .with_context(|| {
                format!("read stats for Docker container {id}")
            })?;

        /*
         * Docker CLI reports memory usage with inactive file cache removed.
         *
         * cgroups v1 generally exposes `total_inactive_file`.
         * cgroups v2 generally exposes `inactive_file`.
         */
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

        let cpu_percent = calculate_cpu_percent(&stat);

        let (network_read, network_write) = stat
            .networks
            .as_ref()
            .map(|networks| {
                networks.values().fold(
                    (0_u64, 0_u64),
                    |(read, write), network| {
                        (
                            read.saturating_add(
                                network.rx_bytes.unwrap_or(0),
                            ),
                            write.saturating_add(
                                network.tx_bytes.unwrap_or(0),
                            ),
                        )
                    },
                )
            })
            .map(|(read, write)| {
                (saturating_i64(read), saturating_i64(write))
            })
            .unwrap_or((0, 0));

        Ok(Metrics {
            memory_usage,
            memory_limit,
            cpu_percent,
            network_read,
            network_write,
            disk_usage,
            disk_limit,
            status,
        })
    }

    pub async fn disk(&self, id: &str) -> Result<(i64, i64)> {
        self.disk_cached(id, false).await
    }

    pub async fn disk_force(
        &self,
        id: &str,
    ) -> Result<(i64, i64)> {
        self.disk_cached(id, true).await
    }

    async fn disk_cached(
        &self,
        id: &str,
        force: bool,
    ) -> Result<(i64, i64)> {
        paths::validate_id(id)?;

        let max_age = Duration::from_secs(
            std::env::var(
                "AGAPORNIS_DISK_USAGE_CACHE_SECONDS",
            )
            .ok()
            .and_then(|value| value.parse().ok())
            .filter(|value| *value > 0)
            .unwrap_or(75),
        );

        loop {
            let calculate = {
                let mut cache = self.disk_cache.lock().await;

                match cache.get(id) {
                    Some(CacheState::Ready(
                        when,
                        usage,
                        limit,
                    )) => {
                        if !force && when.elapsed() <= max_age {
                            return Ok((*usage, *limit));
                        }
                    }

                    Some(CacheState::Calculating(notify)) => {
                        let notify = notify.clone();
                        drop(cache);

                        notify.notified().await;
                        continue;
                    }

                    None => {}
                }

                let notify = Arc::new(Notify::new());

                cache.insert(
                    id.into(),
                    CacheState::Calculating(notify),
                );

                true
            };

            if calculate {
                break;
            }
        }

        let limit = match fs::read_to_string(
            paths::disk_limit_path(id)?,
        )
        .await
        {
            Ok(value) => value
                .trim()
                .parse()
                .unwrap_or(DEFAULT_DISK_LIMIT),

            Err(_) => self
                .inspect(id)
                .await
                .ok()
                .and_then(|value| {
                    value
                        .pointer(
                            "/Config/Labels/\
                             agapornis.disk_limit_bytes",
                        )
                        .and_then(Value::as_str)
                        .and_then(|value| value.parse().ok())
                })
                .unwrap_or(DEFAULT_DISK_LIMIT),
        };

        let path = self.root(id).await?.0;

        let usage = tokio::task::spawn_blocking(move || {
            dir_size(&path)
        })
        .await
        .unwrap_or(0);

        let mut cache = self.disk_cache.lock().await;

        if let Some(CacheState::Calculating(notify)) =
            cache.get(id)
        {
            let notify = notify.clone();

            cache.insert(
                id.into(),
                CacheState::Ready(
                    Instant::now(),
                    usage,
                    limit,
                ),
            );

            notify.notify_waiters();
        } else {
            cache.insert(
                id.into(),
                CacheState::Ready(
                    Instant::now(),
                    usage,
                    limit,
                ),
            );
        }

        Ok((usage, limit))
    }
}

fn calculate_cpu_percent(
    stat: &bollard::models::ContainerStatsResponse,
) -> f64 {
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

    let current_system =
        current.system_cpu_usage.unwrap_or(0);

    let previous_system =
        previous.system_cpu_usage.unwrap_or(0);

    let cpu_delta =
        current_total.saturating_sub(previous_total);

    let system_delta =
        current_system.saturating_sub(previous_system);

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

    (cpu_delta as f64 / system_delta as f64)
        * online_cpus as f64
        * 100.0
}

fn saturating_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

pub async fn self_test_disk_cache() -> Result<()> {
    let manager = DockerManager::new(Arc::new(
        ProtectionState::default(),
    ))?;

    manager.disk_cache.lock().await.insert(
        "self-test".into(),
        CacheState::Ready(
            Instant::now(),
            1234,
            5678,
        ),
    );

    let measured = manager
        .disk_cached("self-test", false)
        .await?;

    if measured != (1234, 5678) {
        bail!(
            "disk cache did not return its fresh snapshot"
        );
    }

    manager
        .disk_cache
        .lock()
        .await
        .remove("self-test");

    println!("disk usage cache self-test: PASS");

    Ok(())
}

pub(super) fn dir_size(root: &Path) -> i64 {
    let mut total = 0;
    let mut pending = vec![root.to_owned()];

    while let Some(dir) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(dir) else {
            continue;
        };

        for entry in entries.flatten() {
            if let Ok(metadata) = entry.metadata() {
                if metadata.is_dir() {
                    pending.push(entry.path());
                } else if metadata.is_file() {
                    total += metadata.len() as i64;
                }
            }
        }
    }

    total
}

