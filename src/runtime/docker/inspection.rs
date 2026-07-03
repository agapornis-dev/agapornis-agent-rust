use super::*;

impl DockerManager {
    pub async fn inspect(&self, id: &str) -> Result<Value> {
        let text = process::docker(["inspect", id]).await?;
        let values: Vec<Value> = serde_json::from_str(&text)?;
        values
            .into_iter()
            .next()
            .context("docker inspect returned no container")
    }

    pub async fn root(&self, id: &str) -> Result<(PathBuf, String, bool, bool)> {
        let fallback = paths::server_dir(id)?;
        let inspect = match self.inspect(id).await {
            Ok(v) => v,
            Err(_) => return Ok((fallback, paths::HOME_CONTAINER_PATH.into(), false, true)),
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
            .and_then(|m| {
                m.iter()
                    .find(|x| x.get("Destination").and_then(Value::as_str) == Some(data.as_str()))
            })
            .and_then(|m| m.get("Source"))
            .and_then(Value::as_str)
            .map(PathBuf::from);
        let known = mounts
            .and_then(|m| {
                m.iter().find(|x| {
                    matches!(
                        x.get("Destination").and_then(Value::as_str),
                        Some("/data") | Some("/home/container")
                    )
                })
            })
            .and_then(|m| m.get("Source"))
            .and_then(Value::as_str)
            .map(PathBuf::from);
        Ok((
            exact.clone().or(known).unwrap_or(fallback),
            data,
            running,
            exact.is_some(),
        ))
    }

    pub async fn metrics(&self, id: &str) -> Result<Metrics> {
        let inspect = match self.inspect(id).await {
            Ok(v) => v,
            Err(_) => {
                return Ok(Metrics {
                    status: "deleted".into(),
                    disk_limit: DEFAULT_DISK_LIMIT,
                    ..Default::default()
                });
            }
        };
        let mut status = if inspect.pointer("/State/Running").and_then(Value::as_bool) == Some(true)
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
        let raw = process::docker(["stats", "--no-stream", "--format", "{{json .}}", id]).await?;
        let stat: Value = serde_json::from_str(raw.lines().next().unwrap_or("{}"))?;
        let mem = stat.get("MemUsage").and_then(Value::as_str).unwrap_or("");
        let (used, limit) = parse_pair(mem);
        let net = stat.get("NetIO").and_then(Value::as_str).unwrap_or("");
        let (read, write) = parse_pair(net);
        let cpu = stat
            .get("CPUPerc")
            .and_then(Value::as_str)
            .unwrap_or("0")
            .trim_end_matches('%')
            .parse()
            .unwrap_or(0.0);
        Ok(Metrics {
            memory_usage: used,
            memory_limit: limit,
            cpu_percent: cpu,
            network_read: read,
            network_write: write,
            disk_usage,
            disk_limit,
            status,
        })
    }

    pub async fn disk(&self, id: &str) -> Result<(i64, i64)> {
        self.disk_cached(id, false).await
    }

    pub async fn disk_force(&self, id: &str) -> Result<(i64, i64)> {
        self.disk_cached(id, true).await
    }

    async fn disk_cached(&self, id: &str, force: bool) -> Result<(i64, i64)> {
        let max_age = Duration::from_secs(
            std::env::var("AGAPORNIS_DISK_USAGE_CACHE_SECONDS")
                .ok()
                .and_then(|v| v.parse().ok())
                .filter(|v| *v > 0)
                .unwrap_or(75),
        );

        // FIX: Cache stampede mitigation via state tracking and Notify waiters.
        loop {
            let calculate = {
                let mut cache = self.disk_cache.lock().await;
                match cache.get(id) {
                    Some(CacheState::Ready(when, usage, limit)) => {
                        if !force && when.elapsed() <= max_age {
                            return Ok((*usage, *limit));
                        }
                    }
                    Some(CacheState::Calculating(notify)) => {
                        let n = notify.clone();
                        drop(cache);
                        n.notified().await;
                        continue; // Wait for the active thread, then loop to re-check
                    }
                    None => {}
                }
                let notify = Arc::new(Notify::new());
                cache.insert(id.into(), CacheState::Calculating(notify.clone()));
                true // Break scope, we are the designated calculator thread
            };
            if calculate {
                break;
            }
        }

        let limit = match fs::read_to_string(paths::disk_limit_path(id)?).await {
            Ok(v) => v.trim().parse().unwrap_or(DEFAULT_DISK_LIMIT),
            Err(_) => self
                .inspect(id)
                .await
                .ok()
                .and_then(|v| {
                    v.pointer("/Config/Labels/agapornis.disk_limit_bytes")
                        .and_then(Value::as_str)
                        .and_then(|v| v.parse().ok())
                })
                .unwrap_or(DEFAULT_DISK_LIMIT),
        };
        let path = self.root(id).await?.0;
        let usage = tokio::task::spawn_blocking(move || dir_size(&path))
            .await
            .unwrap_or(0);

        let mut cache = self.disk_cache.lock().await;
        if let Some(CacheState::Calculating(notify)) = cache.get(id) {
            let n = notify.clone();
            cache.insert(id.into(), CacheState::Ready(Instant::now(), usage, limit));
            n.notify_waiters();
        } else {
            cache.insert(id.into(), CacheState::Ready(Instant::now(), usage, limit));
        }

        Ok((usage, limit))
    }
}

pub async fn self_test_disk_cache() -> Result<()> {
    let manager = DockerManager::new(Arc::new(ProtectionState::default()));
    manager.disk_cache.lock().await.insert(
        "self-test".into(),
        CacheState::Ready(Instant::now(), 1234, 5678),
    );
    let measured = manager.disk_cached("self-test", false).await?;
    if measured != (1234, 5678) {
        bail!("disk cache did not return its fresh snapshot")
    }
    manager.disk_cache.lock().await.remove("self-test");
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
            if let Ok(meta) = entry.metadata() {
                if meta.is_dir() {
                    pending.push(entry.path())
                } else if meta.is_file() {
                    total += meta.len() as i64
                }
            }
        }
    }
    total
}

pub(super) fn parse_pair(value: &str) -> (i64, i64) {
    let mut parts = value.split('/');
    (
        parse_size(parts.next().unwrap_or("")),
        parse_size(parts.next().unwrap_or("")),
    )
}

pub(super) fn parse_size(value: &str) -> i64 {
    let value = value.trim();
    let split = value
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(value.len());
    let number: f64 = value[..split].parse().unwrap_or(0.0);
    let unit = value[split..].trim().to_ascii_lowercase();
    let factor = match unit.as_str() {
        "b" => 1.0,
        "kb" => 1_000.0,
        "kib" => 1024.0,
        "mb" => 1_000_000.0,
        "mib" => 1_048_576.0,
        "gb" => 1_000_000_000.0,
        "gib" => 1_073_741_824.0,
        "tb" => 1e12,
        "tib" => 1_099_511_627_776.0,
        _ => 1.0,
    };
    (number * factor) as i64
}
