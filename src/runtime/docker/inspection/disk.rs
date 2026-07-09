use super::*;

struct DiskCalculationGuard {
    cache: DiskCache,
    id: String,
    notify: Arc<Notify>,
    armed: bool,
}

enum DiskCalculation {
    Cached(i64, i64),
    Calculate(Arc<Notify>),
}

impl DiskCalculationGuard {
    fn complete(&mut self) {
        self.armed = false;
    }
}

impl Drop for DiskCalculationGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let cache = self.cache.clone();
        let id = self.id.clone();
        let notify = self.notify.clone();
        tokio::spawn(async move {
            let mut cache = cache.lock().await;
            if cache.get(&id).is_some_and(|state| {
                matches!(state, CacheState::Calculating(current) if Arc::ptr_eq(current, &notify))
            }) {
                cache.remove(&id);
            }
            notify.notify_waiters();
        });
    }
}

impl DockerManager {
    pub async fn disk(&self, id: &str) -> Result<(i64, i64)> {
        paths::validate_id(id)?;
        if let Some((usage, limit)) = {
            let cache = self.disk_cache.lock().await;
            match cache.get(id) {
                Some(CacheState::Ready(_, usage, limit)) => Some((*usage, *limit)),
                _ => None,
            }
        } {
            return Ok((usage, limit));
        }

        // Live CPU, memory, and network reporting must not wait for a recursive
        // filesystem walk. The supervisor refreshes this cache independently;
        // until its first scan completes, return the configured limit with an
        // unknown (zero) usage value.
        Ok((0, self.disk_limit(id).await?))
    }

    pub async fn disk_force(&self, id: &str) -> Result<(i64, i64)> {
        self.disk_cached(id, true).await
    }

    async fn disk_cached(&self, id: &str, force: bool) -> Result<(i64, i64)> {
        paths::validate_id(id)?;
        let max_age = disk_cache_age();
        let calculation_notify = match self.begin_disk_calculation(id, force, max_age).await {
            DiskCalculation::Cached(usage, limit) => return Ok((usage, limit)),
            DiskCalculation::Calculate(notify) => notify,
        };
        let mut calculation_guard = DiskCalculationGuard {
            cache: self.disk_cache.clone(),
            id: id.to_owned(),
            notify: calculation_notify.clone(),
            armed: true,
        };

        let limit = self.disk_limit(id).await?;
        let path = self.root(id).await?.0;
        let disk_permit = self
            .disk_scans
            .acquire()
            .await
            .context("disk scanner is unavailable")?;
        let usage = tokio::task::spawn_blocking(move || dir_size(&path))
            .await
            .unwrap_or(0);
        drop(disk_permit);

        let mut cache = self.disk_cache.lock().await;
        if cache.get(id).is_some_and(|state| {
            matches!(state, CacheState::Calculating(current) if Arc::ptr_eq(current, &calculation_notify))
        }) {
            cache.insert(id.into(), CacheState::Ready(Instant::now(), usage, limit));
        }
        calculation_guard.complete();
        calculation_notify.notify_waiters();
        Ok((usage, limit))
    }

    async fn begin_disk_calculation(
        &self,
        id: &str,
        force: bool,
        max_age: Duration,
    ) -> DiskCalculation {
        loop {
            let mut cache = self.disk_cache.lock().await;
            match cache.get(id) {
                Some(CacheState::Ready(when, usage, limit))
                    if !force && when.elapsed() <= max_age =>
                {
                    return DiskCalculation::Cached(*usage, *limit);
                }
                Some(CacheState::Calculating(notify)) => {
                    let notify = notify.clone();
                    drop(cache);
                    self.wait_for_disk_calculation(id, &notify).await;
                    continue;
                }
                _ => {}
            }

            let notify = Arc::new(Notify::new());
            cache.insert(id.into(), CacheState::Calculating(notify.clone()));
            return DiskCalculation::Calculate(notify);
        }
    }

    async fn wait_for_disk_calculation(&self, id: &str, notify: &Arc<Notify>) {
        if tokio::time::timeout(Duration::from_secs(120), notify.notified())
            .await
            .is_ok()
        {
            return;
        }
        let mut cache = self.disk_cache.lock().await;
        if cache.get(id).is_some_and(|state| {
            matches!(state, CacheState::Calculating(current) if Arc::ptr_eq(current, notify))
        }) {
            cache.remove(id);
        }
        notify.notify_waiters();
    }

    async fn disk_limit(&self, id: &str) -> Result<i64> {
        Ok(
            match fs::read_to_string(paths::disk_limit_path(id)?).await {
                Ok(value) => value.trim().parse().unwrap_or(DEFAULT_DISK_LIMIT),
                Err(_) => self
                    .inspect(id)
                    .await
                    .ok()
                    .and_then(|value| {
                        value
                            .pointer("/Config/Labels/agapornis.disk_limit_bytes")
                            .and_then(Value::as_str)
                            .and_then(|value| value.parse().ok())
                    })
                    .unwrap_or(DEFAULT_DISK_LIMIT),
            },
        )
    }
}

fn disk_cache_age() -> Duration {
    Duration::from_secs(
        std::env::var("AGAPORNIS_DISK_USAGE_CACHE_SECONDS")
            .ok()
            .and_then(|value| value.parse().ok())
            .filter(|value| *value > 0)
            .unwrap_or(75),
    )
}

pub async fn self_test_disk_cache() -> Result<()> {
    let manager = DockerManager::new(Arc::new(ProtectionState::default()))?;
    manager.disk_cache.lock().await.insert(
        "self-test".into(),
        CacheState::Ready(Instant::now(), 1234, 5678),
    );

    let measured = manager.disk_cached("self-test", false).await?;
    if measured != (1234, 5678) {
        bail!("disk cache did not return its fresh snapshot");
    }

    manager.disk_cache.lock().await.remove("self-test");
    println!("disk usage cache self-test: PASS");
    Ok(())
}

pub(super) fn dir_size(root: &Path) -> i64 {
    let mut total = 0_i64;
    let mut pending = vec![root.to_owned()];
    while let Some(dir) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_symlink() {
                continue;
            }
            if file_type.is_dir() {
                pending.push(entry.path());
            } else if file_type.is_file()
                && let Ok(metadata) = entry.metadata()
            {
                total = total.saturating_add(metadata.len().min(i64::MAX as u64) as i64);
            }
        }
    }
    total
}
