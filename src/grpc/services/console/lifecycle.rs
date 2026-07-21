use super::*;

impl ConsoleHub {
    pub(in crate::services) async fn synchronize_servers(
        self: &Arc<Self>,
        server_ids: Vec<String>,
    ) -> anyhow::Result<ConsoleSyncResult> {
        let mut lifecycle = self.lifecycle.lock().await;
        if lifecycle.inventory_initialized {
            return Ok(sync_result(&lifecycle, 0));
        }

        let mut desired = validate_inventory(server_ids)?;
        desired.extend(lifecycle.pending_additions.drain());
        for id in lifecycle.pending_removals.drain() {
            desired.remove(&id);
        }
        let removed = lifecycle
            .desired
            .difference(&desired)
            .cloned()
            .collect::<Vec<_>>();

        for id in &removed {
            stop_reader(&mut lifecycle, id);
        }
        lifecycle.desired = desired;
        lifecycle.inventory_initialized = true;

        if !removed.is_empty() {
            let mut senders = self.senders.lock().await;
            for id in &removed {
                senders.remove(id);
            }
            drop(senders);
            let mut history = self.history.lock().await;
            for id in &removed {
                history.remove(id);
                self.protection.remove(id);
            }
        }
        Ok(sync_result(&lifecycle, removed.len()))
    }

    pub(in crate::services) async fn track_server(
        self: &Arc<Self>,
        id: &str,
    ) -> anyhow::Result<()> {
        paths::validate_id(id)?;
        let mut lifecycle = self.lifecycle.lock().await;
        record_addition(&mut lifecycle, id);
        lifecycle.desired.insert(id.to_owned());
        Ok(())
    }

    pub(in crate::services) async fn refresh_server(
        self: &Arc<Self>,
        id: &str,
    ) -> anyhow::Result<()> {
        paths::validate_id(id)?;
        let mut lifecycle = self.lifecycle.lock().await;
        record_addition(&mut lifecycle, id);
        lifecycle.desired.insert(id.to_owned());
        if let Some(reader) = lifecycle.readers.get(id) {
            reader.suspended.store(false, Ordering::Release);
            reader.wake.notify_one();
        }
        Ok(())
    }

    pub(in crate::services) async fn inventory_initialized(&self) -> bool {
        self.lifecycle.lock().await.inventory_initialized
    }

    pub(in crate::services) async fn attach_requires_inspection(
        &self,
        id: &str,
    ) -> anyhow::Result<bool> {
        let lifecycle = self.lifecycle.lock().await;
        if lifecycle.inventory_initialized && !lifecycle.desired.contains(id) {
            anyhow::bail!("server is not assigned to this agent console inventory");
        }
        if !lifecycle.inventory_initialized && lifecycle.pending_removals.contains(id) {
            anyhow::bail!("server was removed before console inventory bootstrap");
        }
        Ok(!lifecycle.desired.contains(id))
    }

    pub(crate) async fn detach_reader(&self, id: &str) {
        let lifecycle = self.lifecycle.lock().await;
        if let Some(reader) = lifecycle.readers.get(id) {
            reader.suspended.store(true, Ordering::Release);
            reader.wake.notify_one();
        }
    }

    pub(crate) async fn wake_reader(self: &Arc<Self>, id: &str) {
        let lifecycle = self.lifecycle.lock().await;
        if !lifecycle.desired.contains(id) {
            return;
        }
        if let Some(reader) = lifecycle.readers.get(id) {
            let was_suspended = reader.suspended.swap(false, Ordering::AcqRel);
            if was_suspended || reader.parked.load(Ordering::Acquire) {
                reader.wake.notify_one();
            }
        }
    }

    pub(in crate::services) async fn subscribe(
        self: &Arc<Self>,
        id: &str,
    ) -> anyhow::Result<(Vec<String>, u64, broadcast::Receiver<ConsoleEntry>)> {
        let receiver = {
            let mut lifecycle = self.lifecycle.lock().await;
            validate_subscription(&mut lifecycle, id)?;
            self.validate_reader_capacity(&lifecycle, id)?;
            let mut senders = self.senders.lock().await;
            let sender = senders
                .entry(id.into())
                .or_insert_with(|| broadcast::channel(CONSOLE_BROADCAST_CAPACITY).0)
                .clone();
            let receiver = sender.subscribe();
            self.ensure_reader_locked(&mut lifecycle, id, sender);
            receiver
        };
        let (history, replayed_through) = self.history.lock().await.snapshot(id);
        Ok((history, replayed_through, receiver))
    }

    fn validate_reader_capacity(
        &self,
        lifecycle: &ConsoleLifecycle,
        id: &str,
    ) -> anyhow::Result<()> {
        if self.docker.is_none()
            || lifecycle
                .readers
                .get(id)
                .is_some_and(|reader| !reader.handle.is_finished())
        {
            return Ok(());
        }
        let active = lifecycle
            .readers
            .values()
            .filter(|reader| !reader.handle.is_finished())
            .count();
        if active >= self.max_active_readers {
            anyhow::bail!(
                "console reader capacity reached (maximum {})",
                self.max_active_readers
            );
        }
        Ok(())
    }

    fn ensure_reader_locked(
        self: &Arc<Self>,
        lifecycle: &mut ConsoleLifecycle,
        id: &str,
        sender: broadcast::Sender<ConsoleEntry>,
    ) {
        if let Some(reader) = lifecycle.readers.get(id)
            && !reader.handle.is_finished()
        {
            reader.viewer_wake.notify_one();
            return;
        }
        stop_reader(lifecycle, id);
        self.start_reader_locked(lifecycle, id, sender);
    }

    fn start_reader_locked(
        self: &Arc<Self>,
        lifecycle: &mut ConsoleLifecycle,
        id: &str,
        sender: broadcast::Sender<ConsoleEntry>,
    ) {
        let Some(docker) = self.docker.clone() else {
            return;
        };
        let active = Arc::new(AtomicBool::new(true));
        let parked = Arc::new(AtomicBool::new(false));
        let suspended = Arc::new(AtomicBool::new(false));
        let wake = Arc::new(Notify::new());
        let viewer_wake = Arc::new(Notify::new());
        let hub = self.clone();
        let server_id = id.to_owned();
        let task_active = active.clone();
        let cleanup_active = active.clone();
        let task_parked = parked.clone();
        let task_suspended = suspended.clone();
        let task_wake = wake.clone();
        let task_viewer_wake = viewer_wake.clone();
        let task_sender = sender.clone();
        let handle = tokio::spawn(async move {
            let idle = hub
                .clone()
                .read_loop(
                    server_id.clone(),
                    docker,
                    task_active,
                    task_parked,
                    task_suspended,
                    task_wake,
                    task_viewer_wake,
                    task_sender.clone(),
                )
                .await;
            if idle {
                hub.retire_idle_reader(&server_id, &cleanup_active, &task_sender)
                    .await;
            }
        });
        lifecycle.readers.insert(
            id.to_owned(),
            ReaderTask {
                active,
                parked,
                suspended,
                wake,
                viewer_wake,
                handle,
            },
        );
    }

    async fn retire_idle_reader(
        self: &Arc<Self>,
        id: &str,
        active: &Arc<AtomicBool>,
        sender: &broadcast::Sender<ConsoleEntry>,
    ) {
        let mut lifecycle = self.lifecycle.lock().await;
        let current = lifecycle
            .readers
            .get(id)
            .is_some_and(|reader| Arc::ptr_eq(&reader.active, active));
        if !current {
            return;
        }
        lifecycle.readers.remove(id);
        if sender.receiver_count() > 0 && lifecycle.desired.contains(id) {
            self.start_reader_locked(&mut lifecycle, id, sender.clone());
            return;
        }
        let mut senders = self.senders.lock().await;
        if senders
            .get(id)
            .is_some_and(|current| current.same_channel(sender) && current.receiver_count() == 0)
        {
            senders.remove(id);
        }
        drop(senders);
        self.history.lock().await.remove(id);
    }

    pub async fn remove(&self, id: &str) {
        let mut lifecycle = self.lifecycle.lock().await;
        if !lifecycle.inventory_initialized {
            lifecycle.pending_additions.remove(id);
            lifecycle.pending_removals.insert(id.to_owned());
        }
        lifecycle.desired.remove(id);
        stop_reader(&mut lifecycle, id);
        self.senders.lock().await.remove(id);
        self.history.lock().await.remove(id);
        self.protection.remove(id);
    }
}

fn validate_inventory(server_ids: Vec<String>) -> anyhow::Result<HashSet<String>> {
    let mut desired = HashSet::with_capacity(server_ids.len());
    for id in server_ids {
        paths::validate_id(&id)?;
        desired.insert(id);
    }
    Ok(desired)
}

fn validate_subscription(lifecycle: &mut ConsoleLifecycle, id: &str) -> anyhow::Result<()> {
    if lifecycle.inventory_initialized && !lifecycle.desired.contains(id) {
        anyhow::bail!("server is not assigned to this agent console inventory");
    }
    if !lifecycle.inventory_initialized {
        if lifecycle.pending_removals.contains(id) {
            anyhow::bail!("server was removed before console inventory bootstrap");
        }
        lifecycle.desired.insert(id.to_owned());
    }
    Ok(())
}

fn record_addition(lifecycle: &mut ConsoleLifecycle, id: &str) {
    if lifecycle.inventory_initialized {
        return;
    }
    lifecycle.pending_removals.remove(id);
    lifecycle.pending_additions.insert(id.to_owned());
}

fn stop_reader(lifecycle: &mut ConsoleLifecycle, id: &str) {
    if let Some(reader) = lifecycle.readers.remove(id) {
        reader.active.store(false, Ordering::Release);
        reader.handle.abort();
    }
}

fn sync_result(lifecycle: &ConsoleLifecycle, removed_count: usize) -> ConsoleSyncResult {
    ConsoleSyncResult {
        active_reader_count: lifecycle
            .readers
            .values()
            .filter(|reader| !reader.handle.is_finished())
            .count(),
        accepted_count: lifecycle.desired.len(),
        removed_count,
    }
}
