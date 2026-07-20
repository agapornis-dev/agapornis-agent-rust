use super::*;
use crate::paths;
use bollard::container::LogOutput;
use chrono::{DateTime, FixedOffset};
use futures_util::StreamExt;
use std::{
    collections::HashSet,
    sync::atomic::{AtomicBool, Ordering},
};
use tokio::{sync::Notify, task::JoinHandle};

const CONSOLE_BROADCAST_CAPACITY: usize = 256;
const CONSOLE_HISTORY_LINES: usize = 200;
const CONSOLE_HISTORY_BYTES: usize = 64 * 1024;
const CONSOLE_RECONNECT_MIN: Duration = Duration::from_secs(1);
const CONSOLE_RECONNECT_MAX: Duration = Duration::from_secs(30);
const TRUNCATION_SUFFIX: &str = " … [truncated]";

#[derive(Default)]
struct ConsoleHistory {
    lines: VecDeque<ConsoleEntry>,
    bytes: usize,
    next_sequence: u64,
}

#[derive(Clone)]
pub(super) struct ConsoleEntry {
    pub(super) sequence: u64,
    pub(super) line: String,
}

struct ReaderTask {
    active: Arc<AtomicBool>,
    parked: Arc<AtomicBool>,
    suspended: Arc<AtomicBool>,
    wake: Arc<Notify>,
    handle: JoinHandle<()>,
}

#[derive(Default)]
struct ConsoleLifecycle {
    desired: HashSet<String>,
    readers: HashMap<String, ReaderTask>,
    pending_additions: HashSet<String>,
    pending_removals: HashSet<String>,
    inventory_initialized: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct ConsoleSyncResult {
    pub(super) active_reader_count: usize,
    pub(super) accepted_count: usize,
    pub(super) removed_count: usize,
}

/// Owns one lightweight Docker log reader for every server assigned to this
/// node. Reader lifetime follows the authoritative server inventory, not the
/// number of browser console viewers.
pub struct ConsoleHub {
    docker: Option<Arc<DockerManager>>,
    senders: Mutex<HashMap<String, broadcast::Sender<ConsoleEntry>>>,
    history: Mutex<HashMap<String, ConsoleHistory>>,
    lifecycle: Mutex<ConsoleLifecycle>,
    protection: Arc<ProtectionState>,
}

impl ConsoleHub {
    pub(super) fn new(docker: Arc<DockerManager>, protection: Arc<ProtectionState>) -> Self {
        Self::with_optional_docker(Some(docker), protection)
    }

    pub(super) fn with_optional_docker(
        docker: Option<Arc<DockerManager>>,
        protection: Arc<ProtectionState>,
    ) -> Self {
        Self {
            docker,
            senders: Mutex::new(HashMap::new()),
            history: Mutex::new(HashMap::new()),
            lifecycle: Mutex::new(ConsoleLifecycle::default()),
            protection,
        }
    }

    pub(super) async fn synchronize_servers(
        self: &Arc<Self>,
        server_ids: Vec<String>,
    ) -> anyhow::Result<ConsoleSyncResult> {
        let mut lifecycle = self.lifecycle.lock().await;

        // The inventory is a one-shot process bootstrap. Retried requests can
        // happen when an acknowledgement is lost or when the API restarts;
        // applying an older snapshot again could undo later create/delete
        // operations, so acknowledge duplicates without mutating state.
        if lifecycle.inventory_initialized {
            return Ok(ConsoleSyncResult {
                active_reader_count: lifecycle
                    .readers
                    .values()
                    .filter(|reader| !reader.handle.is_finished())
                    .count(),
                accepted_count: lifecycle.desired.len(),
                removed_count: 0,
            });
        }

        // Validate the complete snapshot before mutating state. A malformed
        // first inventory must not partially detach valid readers or mark the
        // process as initialized.
        let mut desired = validate_inventory(server_ids)?;

        // Lifecycle RPCs can race the API reading and delivering its startup
        // snapshot. Replay those newer local facts over the snapshot so a
        // delayed bootstrap cannot remove a new server or resurrect a deleted
        // one. The last create/delete operation for an ID wins in these sets.
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
            if let Some(reader) = lifecycle.readers.remove(id) {
                reader.active.store(false, Ordering::Release);
                reader.handle.abort();
            }
        }
        lifecycle.desired = desired;
        lifecycle.inventory_initialized = true;

        let requested = lifecycle.desired.iter().cloned().collect::<Vec<_>>();
        for id in requested {
            self.ensure_reader_locked(&mut lifecycle, &id);
        }

        // Keep lifecycle ownership locked through stale-state cleanup. A
        // concurrent subscription therefore happens wholly before or after
        // this authoritative reconciliation and cannot resurrect half-removed
        // state.
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

        Ok(ConsoleSyncResult {
            active_reader_count: lifecycle
                .readers
                .values()
                .filter(|reader| !reader.handle.is_finished())
                .count(),
            accepted_count: lifecycle.desired.len(),
            removed_count: removed.len(),
        })
    }

    /// Adds a server immediately during rolling upgrades where the API has not
    /// sent its startup inventory yet. This local addition is newer than any
    /// already-read startup snapshot and is therefore merged into it.
    pub(super) async fn track_server(self: &Arc<Self>, id: &str) -> anyhow::Result<()> {
        paths::validate_id(id)?;
        let mut lifecycle = self.lifecycle.lock().await;
        Self::record_addition(&mut lifecycle, id);
        lifecycle.desired.insert(id.to_owned());
        self.ensure_reader_locked(&mut lifecycle, id);
        Ok(())
    }

    /// Ensures ownership and opens a fresh stream immediately after a server
    /// start or container replacement, bypassing any reconnect backoff left by
    /// the stopped/missing container.
    pub(super) async fn refresh_server(self: &Arc<Self>, id: &str) -> anyhow::Result<()> {
        paths::validate_id(id)?;
        let mut lifecycle = self.lifecycle.lock().await;
        Self::record_addition(&mut lifecycle, id);
        lifecycle.desired.insert(id.to_owned());
        if let Some(reader) = lifecycle.readers.get(id)
            && !reader.handle.is_finished()
        {
            reader.suspended.store(false, Ordering::Release);
            reader.wake.notify_one();
            return Ok(());
        }
        self.ensure_reader_locked(&mut lifecycle, id);
        Ok(())
    }

    pub(super) async fn inventory_initialized(&self) -> bool {
        self.lifecycle.lock().await.inventory_initialized
    }

    /// Returns whether a console attach still needs a Docker existence check.
    ///
    /// Servers already owned by this hub have a persistent reader (or a
    /// reader that can be restarted by `subscribe`) and therefore do not need
    /// another Engine round trip just because a browser viewer attached. An
    /// unknown identifier is only allowed before the authoritative startup
    /// inventory arrives, and must be validated against Docker before it can
    /// create local console state.
    pub(super) async fn attach_requires_inspection(&self, id: &str) -> anyhow::Result<bool> {
        let lifecycle = self.lifecycle.lock().await;
        if lifecycle.inventory_initialized && !lifecycle.desired.contains(id) {
            anyhow::bail!("server is not assigned to this agent console inventory");
        }
        if !lifecycle.inventory_initialized && lifecycle.pending_removals.contains(id) {
            anyhow::bail!("server was removed before console inventory bootstrap");
        }
        Ok(!lifecycle.desired.contains(id))
    }

    fn record_addition(lifecycle: &mut ConsoleLifecycle, id: &str) {
        if lifecycle.inventory_initialized {
            return;
        }
        lifecycle.pending_removals.remove(id);
        lifecycle.pending_additions.insert(id.to_owned());
    }

    /// Stops a reader for a temporarily missing container without revoking the
    /// API-owned assignment or its warm history.
    pub(crate) async fn detach_reader(&self, id: &str) {
        let lifecycle = self.lifecycle.lock().await;
        if let Some(reader) = lifecycle.readers.get(id) {
            reader.suspended.store(true, Ordering::Release);
            reader.wake.notify_one();
        }
    }

    /// Wakes a parked reader when the local supervisor observes that its
    /// assigned container is running again. Unassigned local containers are
    /// intentionally ignored.
    pub(crate) async fn wake_reader(self: &Arc<Self>, id: &str) {
        let mut lifecycle = self.lifecycle.lock().await;
        if lifecycle.desired.contains(id) {
            if let Some(reader) = lifecycle.readers.get(id) {
                let was_suspended = reader.suspended.swap(false, Ordering::AcqRel);
                if was_suspended || reader.parked.load(Ordering::Acquire) {
                    reader.wake.notify_one();
                }
            } else {
                self.ensure_reader_locked(&mut lifecycle, id);
            }
        }
    }

    pub(super) async fn subscribe(
        self: &Arc<Self>,
        id: &str,
    ) -> anyhow::Result<(Vec<String>, u64, broadcast::Receiver<ConsoleEntry>)> {
        let receiver = {
            // Lifecycle -> sender is the shared registration/removal lock
            // order. Registering the reader before taking the history snapshot
            // guarantees any concurrent output is either in that snapshot or
            // waiting in this receiver.
            let mut lifecycle = self.lifecycle.lock().await;
            if lifecycle.inventory_initialized && !lifecycle.desired.contains(id) {
                anyhow::bail!("server is not assigned to this agent console inventory");
            }
            if !lifecycle.inventory_initialized {
                // A browser stream is not an authoritative lifecycle event. In
                // particular, a late subscription must not undo a DeleteServer
                // tombstone while the startup inventory is still in flight.
                if lifecycle.pending_removals.contains(id) {
                    anyhow::bail!("server was removed before console inventory bootstrap");
                }
                lifecycle.desired.insert(id.to_owned());
            }
            self.ensure_reader_locked(&mut lifecycle, id);

            let mut senders = self.senders.lock().await;
            senders
                .entry(id.into())
                .or_insert_with(|| broadcast::channel(CONSOLE_BROADCAST_CAPACITY).0)
                .subscribe()
        };

        let (history, replayed_through) = self
            .history
            .lock()
            .await
            .get(id)
            .map(|history| {
                (
                    history
                        .lines
                        .iter()
                        .map(|entry| entry.line.clone())
                        .collect(),
                    history
                        .lines
                        .back()
                        .map(|entry| entry.sequence)
                        .unwrap_or(0),
                )
            })
            .unwrap_or_default();

        Ok((history, replayed_through, receiver))
    }

    fn ensure_reader_locked(self: &Arc<Self>, lifecycle: &mut ConsoleLifecycle, id: &str) {
        let needs_reader = lifecycle
            .readers
            .get(id)
            .is_none_or(|reader| reader.handle.is_finished());
        if !needs_reader {
            return;
        }
        if let Some(previous) = lifecycle.readers.remove(id) {
            previous.active.store(false, Ordering::Release);
            previous.handle.abort();
        }

        let Some(docker) = self.docker.clone() else {
            return;
        };
        let active = Arc::new(AtomicBool::new(true));
        let parked = Arc::new(AtomicBool::new(false));
        let suspended = Arc::new(AtomicBool::new(false));
        let wake = Arc::new(Notify::new());
        let hub = self.clone();
        let server_id = id.to_owned();
        let reader_active = active.clone();
        let reader_parked = parked.clone();
        let reader_suspended = suspended.clone();
        let reader_wake = wake.clone();
        let handle = tokio::spawn(async move {
            hub.read_loop(
                server_id,
                docker,
                reader_active,
                reader_parked,
                reader_suspended,
                reader_wake,
            )
            .await;
        });
        lifecycle.readers.insert(
            id.to_owned(),
            ReaderTask {
                active,
                parked,
                suspended,
                wake,
                handle,
            },
        );
    }

    pub async fn publish(&self, id: &str, line: String) {
        let lifecycle = self.lifecycle.lock().await;
        if (lifecycle.inventory_initialized && !lifecycle.desired.contains(id))
            || (!lifecycle.inventory_initialized && lifecycle.pending_removals.contains(id))
        {
            return;
        }
        // Supervisor messages are rare, so keep the ownership guard until the
        // line is stored. A concurrent delete can then only happen wholly
        // before or after this publish and cannot leave stale history behind.
        self.publish_line(id, line).await;
    }

    async fn publish_from_reader(&self, id: &str, active: &AtomicBool, line: String) -> bool {
        if !active.load(Ordering::Acquire) {
            return false;
        }
        self.publish_line_when_active(id, line, active).await
    }

    async fn publish_line(&self, id: &str, line: String) {
        self.publish_prepared(id, self.prepare_line(id, line), None)
            .await;
    }

    async fn publish_line_when_active(&self, id: &str, line: String, active: &AtomicBool) -> bool {
        let line = self.prepare_line(id, line);
        if !active.load(Ordering::Acquire) {
            return false;
        }
        self.publish_prepared(id, line, Some(active)).await;
        active.load(Ordering::Acquire)
    }

    fn prepare_line(&self, id: &str, line: String) -> Option<String> {
        let max = self.protection.max_line_length();
        let line = if line.len() > max {
            format!(
                "{}{}",
                &line[..line.floor_char_boundary(max)],
                TRUNCATION_SUFFIX
            )
        } else {
            line
        };

        let line = match self.protection.observe_line(id) {
            LineDecision::Allow => line,
            LineDecision::Notify => format!(
                "[agent] Console output throttled above {} lines/second.",
                self.protection.console_lines_per_second()
            ),
            LineDecision::Trip => "[agent] Sustained console flood detected. Output remains throttled to protect the control plane.".into(),
            LineDecision::Suppress => return None,
        };
        Some(line)
    }

    async fn publish_prepared(&self, id: &str, line: Option<String>, active: Option<&AtomicBool>) {
        let Some(line) = line else {
            return;
        };
        let entry = {
            let mut all = self.history.lock().await;
            if active.is_some_and(|active| !active.load(Ordering::Acquire)) {
                return;
            }
            let history = all.entry(id.into()).or_default();
            history.next_sequence = history.next_sequence.saturating_add(1);
            let entry = ConsoleEntry {
                sequence: history.next_sequence,
                line,
            };
            history.bytes = history.bytes.saturating_add(entry.line.len());
            history.lines.push_back(entry.clone());
            while history.lines.len() > CONSOLE_HISTORY_LINES
                || history.bytes > CONSOLE_HISTORY_BYTES
            {
                if let Some(removed) = history.lines.pop_front() {
                    history.bytes = history.bytes.saturating_sub(removed.line.len());
                }
            }
            entry
        };

        let senders = self.senders.lock().await;
        if active.is_some_and(|active| !active.load(Ordering::Acquire)) {
            return;
        }
        if let Some(sender) = senders.get(id) {
            let _ = sender.send(entry);
        }
    }

    async fn read_loop(
        self: Arc<Self>,
        id: String,
        docker: Arc<DockerManager>,
        active: Arc<AtomicBool>,
        parked: Arc<AtomicBool>,
        suspended: Arc<AtomicBool>,
        wake: Arc<Notify>,
    ) {
        let maximum = self
            .protection
            .max_line_length()
            .saturating_sub(TRUNCATION_SUFFIX.len());
        let mut cursor: Option<DateTime<FixedOffset>> = None;
        let mut cursor_lines = HashSet::<String>::new();
        let mut reconnect_delay = CONSOLE_RECONNECT_MIN;

        loop {
            if !active.load(Ordering::Acquire) {
                return;
            }
            if suspended.load(Ordering::Acquire) {
                parked.store(true, Ordering::Release);
                wake.notified().await;
                parked.store(false, Ordering::Release);
                continue;
            }
            parked.store(false, Ordering::Release);

            let since = cursor.as_ref().map(docker_since_timestamp);
            let replay_cutoff = cursor;
            let replay_cutoff_lines = cursor_lines.clone();
            let mut logs = docker.follow_console_logs(&id, since);
            let mut decoder = ConsoleOutputDecoder::default();
            let mut received_new_output = false;
            let mut refresh_requested = false;

            loop {
                let item = tokio::select! {
                    _ = wake.notified() => {
                        refresh_requested = true;
                        break;
                    }
                    item = logs.next() => item,
                };
                let Some(item) = item else {
                    break;
                };
                if !active.load(Ordering::Acquire) {
                    return;
                }
                match item {
                    Ok(output) => {
                        for line in decoder.push(output, maximum) {
                            match self
                                .process_docker_line(
                                    &id,
                                    &active,
                                    line,
                                    replay_cutoff.as_ref(),
                                    &replay_cutoff_lines,
                                    &mut cursor,
                                    &mut cursor_lines,
                                )
                                .await
                            {
                                ProcessLineOutcome::Published => received_new_output = true,
                                ProcessLineOutcome::Skipped => {}
                                ProcessLineOutcome::Stale => return,
                            }
                        }
                    }
                    Err(error) => {
                        tracing::debug!(
                            container_id = %id,
                            error = %error,
                            "Docker console log stream ended"
                        );
                        break;
                    }
                }
            }

            for line in decoder.finish() {
                match self
                    .process_docker_line(
                        &id,
                        &active,
                        line,
                        replay_cutoff.as_ref(),
                        &replay_cutoff_lines,
                        &mut cursor,
                        &mut cursor_lines,
                    )
                    .await
                {
                    ProcessLineOutcome::Published => received_new_output = true,
                    ProcessLineOutcome::Skipped => {}
                    ProcessLineOutcome::Stale => return,
                }
            }

            if !active.load(Ordering::Acquire) {
                return;
            }
            if suspended.load(Ordering::Acquire) {
                continue;
            }
            if refresh_requested {
                reconnect_delay = CONSOLE_RECONNECT_MIN;
                continue;
            }

            let running = docker
                .inspect(&id)
                .await
                .ok()
                .and_then(|inspect| {
                    inspect
                        .pointer("/State/Running")
                        .and_then(serde_json::Value::as_bool)
                })
                .unwrap_or(false);
            if !running {
                // `docker logs --follow` exits after replaying a stopped
                // container's tail. Park until an explicit lifecycle wake so
                // stopped servers do not issue one Engine request per second.
                parked.store(true, Ordering::Release);
                wake.notified().await;
                parked.store(false, Ordering::Release);
                reconnect_delay = CONSOLE_RECONNECT_MIN;
                continue;
            }

            let delay = reconnect_delay;
            reconnect_delay = if received_new_output {
                CONSOLE_RECONNECT_MIN
            } else {
                reconnect_delay.saturating_mul(2).min(CONSOLE_RECONNECT_MAX)
            };
            tokio::select! {
                _ = wake.notified() => reconnect_delay = CONSOLE_RECONNECT_MIN,
                _ = tokio::time::sleep(delay) => {}
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn process_docker_line(
        &self,
        id: &str,
        active: &AtomicBool,
        raw_line: String,
        replay_cutoff: Option<&DateTime<FixedOffset>>,
        replay_cutoff_lines: &HashSet<String>,
        cursor: &mut Option<DateTime<FixedOffset>>,
        cursor_lines: &mut HashSet<String>,
    ) -> ProcessLineOutcome {
        let (timestamp, line) = split_docker_timestamp(raw_line);
        if let (Some(timestamp), Some(cutoff)) = (timestamp.as_ref(), replay_cutoff)
            && (timestamp < cutoff || (timestamp == cutoff && replay_cutoff_lines.contains(&line)))
        {
            return ProcessLineOutcome::Skipped;
        }

        if let Some(timestamp) = timestamp {
            match cursor.as_ref() {
                Some(current) if timestamp < *current => {}
                Some(current) if timestamp == *current => {
                    cursor_lines.insert(line.clone());
                }
                _ => {
                    *cursor = Some(timestamp);
                    cursor_lines.clear();
                    cursor_lines.insert(line.clone());
                }
            }
        }

        if self.publish_from_reader(id, active, line).await {
            ProcessLineOutcome::Published
        } else {
            ProcessLineOutcome::Stale
        }
    }

    /// Permanently removes a server from the desired inventory. Temporary
    /// container disappearance must use `detach_reader` instead.
    pub async fn remove(&self, id: &str) {
        let mut lifecycle = self.lifecycle.lock().await;
        if !lifecycle.inventory_initialized {
            lifecycle.pending_additions.remove(id);
            lifecycle.pending_removals.insert(id.to_owned());
        }
        lifecycle.desired.remove(id);
        if let Some(reader) = lifecycle.readers.remove(id) {
            reader.active.store(false, Ordering::Release);
            reader.handle.abort();
        }
        self.senders.lock().await.remove(id);
        self.history.lock().await.remove(id);
        self.protection.remove(id);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProcessLineOutcome {
    Published,
    Skipped,
    Stale,
}

fn validate_inventory(server_ids: Vec<String>) -> anyhow::Result<HashSet<String>> {
    let mut desired = HashSet::with_capacity(server_ids.len());
    for id in server_ids {
        paths::validate_id(&id)?;
        desired.insert(id);
    }
    Ok(desired)
}

fn docker_since_timestamp(timestamp: &DateTime<FixedOffset>) -> i32 {
    timestamp.timestamp().clamp(0, i32::MAX as i64) as i32
}

fn split_docker_timestamp(line: String) -> (Option<DateTime<FixedOffset>>, String) {
    let Some((candidate, output)) = line.split_once(' ') else {
        return (None, line);
    };
    match DateTime::parse_from_rfc3339(candidate) {
        Ok(timestamp) => (Some(timestamp), output.to_owned()),
        Err(_) => (None, line),
    }
}

#[derive(Default)]
struct ConsoleOutputDecoder {
    stdout: LineBuffer,
    stderr: LineBuffer,
    console: LineBuffer,
}

impl ConsoleOutputDecoder {
    fn push(&mut self, output: LogOutput, maximum: usize) -> Vec<String> {
        match output {
            LogOutput::StdOut { message } => self.stdout.push(&message, maximum),
            LogOutput::StdErr { message } => self.stderr.push(&message, maximum),
            LogOutput::Console { message } => self.console.push(&message, maximum),
            LogOutput::StdIn { .. } => Vec::new(),
        }
    }

    fn finish(self) -> Vec<String> {
        let mut lines = self.stdout.finish();
        lines.extend(self.stderr.finish());
        lines.extend(self.console.finish());
        lines
    }
}

#[derive(Default)]
struct LineBuffer {
    bytes: Vec<u8>,
    truncated: bool,
    swallow_line_feed: bool,
}

impl LineBuffer {
    fn push(&mut self, chunk: &[u8], maximum: usize) -> Vec<String> {
        let mut lines = Vec::new();
        for byte in chunk {
            if self.swallow_line_feed {
                self.swallow_line_feed = false;
                if *byte == b'\n' {
                    continue;
                }
            }

            match *byte {
                b'\r' => {
                    lines.push(self.take_line());
                    self.swallow_line_feed = true;
                }
                b'\n' => lines.push(self.take_line()),
                byte if self.bytes.len() < maximum => self.bytes.push(byte),
                _ => self.truncated = true,
            }
        }
        lines
    }

    fn finish(mut self) -> Vec<String> {
        if self.bytes.is_empty() && !self.truncated {
            Vec::new()
        } else {
            vec![self.take_line()]
        }
    }

    fn take_line(&mut self) -> String {
        let mut line = String::from_utf8_lossy(&self.bytes).into_owned();
        self.bytes.clear();
        if std::mem::take(&mut self.truncated) {
            line.push_str(TRUNCATION_SUFFIX);
        }
        line
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn memory_hub() -> Arc<ConsoleHub> {
        Arc::new(ConsoleHub::with_optional_docker(
            None,
            Arc::new(ProtectionState::default()),
        ))
    }

    #[tokio::test]
    async fn startup_inventory_is_authoritative_and_deduplicated() {
        let hub = memory_hub();
        let first = hub
            .synchronize_servers(vec!["one".into(), "two".into(), "two".into()])
            .await
            .unwrap();
        assert_eq!(first.accepted_count, 2);
        assert_eq!(first.removed_count, 0);
        assert!(hub.inventory_initialized().await);
        assert_eq!(
            hub.lifecycle.lock().await.desired,
            HashSet::from(["one".to_owned(), "two".to_owned()])
        );
    }

    #[tokio::test]
    async fn delayed_startup_snapshot_keeps_a_concurrent_create() {
        let hub = memory_hub();
        hub.track_server("created-after-read").await.unwrap();

        let result = hub
            .synchronize_servers(vec!["existing".into()])
            .await
            .unwrap();
        assert_eq!(result.accepted_count, 2);
        assert_eq!(
            hub.lifecycle.lock().await.desired,
            HashSet::from(["existing".to_owned(), "created-after-read".to_owned()])
        );
    }

    #[tokio::test]
    async fn delayed_startup_snapshot_cannot_resurrect_a_concurrent_delete() {
        let hub = memory_hub();
        hub.remove("deleted-after-read").await;

        let result = hub
            .synchronize_servers(vec!["kept".into(), "deleted-after-read".into()])
            .await
            .unwrap();
        assert_eq!(result.accepted_count, 1);
        assert_eq!(
            hub.lifecycle.lock().await.desired,
            HashSet::from(["kept".to_owned()])
        );
    }

    #[tokio::test]
    async fn delete_then_same_id_create_wins_over_startup_snapshot() {
        let hub = memory_hub();
        hub.remove("server").await;
        hub.track_server("server").await.unwrap();

        hub.synchronize_servers(Vec::new()).await.unwrap();
        assert_eq!(
            hub.lifecycle.lock().await.desired,
            HashSet::from(["server".to_owned()])
        );
    }

    #[tokio::test]
    async fn create_then_delete_wins_over_startup_snapshot() {
        let hub = memory_hub();
        hub.track_server("server").await.unwrap();
        hub.remove("server").await;

        hub.synchronize_servers(vec!["server".into()])
            .await
            .unwrap();
        assert!(!hub.lifecycle.lock().await.desired.contains("server"));
    }

    #[tokio::test]
    async fn duplicate_startup_inventory_is_a_noop_after_direct_mutation() {
        let hub = memory_hub();
        hub.synchronize_servers(vec!["existing".into()])
            .await
            .unwrap();
        hub.track_server("created-later").await.unwrap();

        let duplicate = hub.synchronize_servers(Vec::new()).await.unwrap();
        assert_eq!(duplicate.removed_count, 0);
        assert_eq!(duplicate.accepted_count, 2);
        assert_eq!(
            hub.lifecycle.lock().await.desired,
            HashSet::from(["existing".to_owned(), "created-later".to_owned()])
        );
    }

    #[tokio::test]
    async fn malformed_first_inventory_does_not_initialize_or_discard_deltas() {
        let hub = memory_hub();
        hub.track_server("created-during-bootstrap").await.unwrap();

        assert!(
            hub.synchronize_servers(vec!["../invalid".into()])
                .await
                .is_err()
        );
        assert!(!hub.inventory_initialized().await);

        hub.synchronize_servers(vec!["valid-server".into()])
            .await
            .unwrap();
        assert_eq!(
            hub.lifecycle.lock().await.desired,
            HashSet::from([
                "valid-server".to_owned(),
                "created-during-bootstrap".to_owned()
            ])
        );
    }

    #[tokio::test]
    async fn subscription_cannot_resurrect_a_server_removed_by_inventory() {
        let hub = memory_hub();
        hub.synchronize_servers(Vec::new()).await.unwrap();

        assert!(hub.attach_requires_inspection("server").await.is_err());
        assert!(hub.subscribe("server").await.is_err());
        assert!(!hub.lifecycle.lock().await.desired.contains("server"));
    }

    #[tokio::test]
    async fn pre_bootstrap_subscription_cannot_override_delete_tombstone() {
        let hub = memory_hub();
        hub.remove("server").await;

        assert!(hub.attach_requires_inspection("server").await.is_err());
        assert!(hub.subscribe("server").await.is_err());
        hub.publish("server", "late supervisor message".into())
            .await;
        assert!(!hub.history.lock().await.contains_key("server"));
        hub.synchronize_servers(vec!["server".into()])
            .await
            .unwrap();
        assert!(!hub.lifecycle.lock().await.desired.contains("server"));
    }

    #[tokio::test]
    async fn tracked_console_attach_does_not_require_docker_inspection() {
        let hub = memory_hub();
        hub.track_server("server").await.unwrap();

        assert!(!hub.attach_requires_inspection("server").await.unwrap());

        hub.synchronize_servers(vec!["inventory-server".into()])
            .await
            .unwrap();
        assert!(
            !hub.attach_requires_inspection("inventory-server")
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn warm_attach_eligibility_does_not_bypass_a_later_delete() {
        let hub = memory_hub();
        hub.synchronize_servers(vec!["server".into()])
            .await
            .unwrap();

        assert!(!hub.attach_requires_inspection("server").await.unwrap());
        hub.remove("server").await;

        assert!(hub.subscribe("server").await.is_err());
        assert!(!hub.lifecycle.lock().await.desired.contains("server"));
    }

    #[tokio::test]
    async fn recreated_same_id_is_warm_and_can_subscribe() {
        let hub = memory_hub();
        hub.synchronize_servers(vec!["server".into()])
            .await
            .unwrap();
        hub.publish("server", "old generation".into()).await;
        let (old_history, _, mut old_receiver) = hub.subscribe("server").await.unwrap();
        assert_eq!(old_history, ["old generation"]);

        hub.remove("server").await;
        hub.track_server("server").await.unwrap();

        assert!(!hub.attach_requires_inspection("server").await.unwrap());
        let (new_history, _, mut new_receiver) = hub.subscribe("server").await.unwrap();
        assert!(new_history.is_empty());

        hub.publish("server", "new generation".into()).await;
        assert_eq!(new_receiver.recv().await.unwrap().line, "new generation");
        assert!(matches!(
            old_receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Closed)
        ));
    }

    #[tokio::test]
    async fn unknown_pre_bootstrap_inspection_check_does_not_allocate_state() {
        let hub = memory_hub();

        assert!(
            hub.attach_requires_inspection("not-yet-in-inventory")
                .await
                .unwrap()
        );
        let lifecycle = hub.lifecycle.lock().await;
        assert!(lifecycle.desired.is_empty());
        assert!(lifecycle.readers.is_empty());
        drop(lifecycle);
        assert!(hub.history.lock().await.is_empty());
    }

    #[tokio::test]
    async fn inactive_reader_cannot_publish_into_recreated_server_state() {
        let hub = memory_hub();
        hub.synchronize_servers(vec!["server".into()])
            .await
            .unwrap();
        let (_, _, mut receiver) = hub.subscribe("server").await.unwrap();
        let stale_reader = AtomicBool::new(false);

        assert!(
            !hub.publish_line_when_active("server", "stale".into(), &stale_reader)
                .await
        );
        assert!(hub.history.lock().await.get("server").is_none());
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn console_history_is_bounded_to_64_kib_per_server() {
        let hub = memory_hub();
        hub.synchronize_servers(vec!["server".into()])
            .await
            .unwrap();

        for index in 0..5 {
            hub.publish("server", format!("{index}:{}", "x".repeat(15_998)))
                .await;
        }

        let history = hub.history.lock().await;
        let history = history.get("server").unwrap();
        assert!(history.bytes <= CONSOLE_HISTORY_BYTES);
        assert_eq!(history.lines.len(), 4);
        assert!(history.lines.front().unwrap().line.starts_with("1:"));
    }

    #[test]
    fn decoder_handles_fragmented_crlf_and_bounds_large_lines() {
        let mut decoder = LineBuffer::default();
        assert!(decoder.push(b"first\r", 8).eq(&["first"]));
        assert!(
            decoder
                .push(b"\nsecond\rthird\n", 8)
                .eq(&["second", "third"])
        );
        assert!(decoder.push(b"0123456789\n", 4)[0].ends_with("[truncated]"));
    }

    #[test]
    fn docker_timestamp_is_removed_and_preserved_as_a_cursor() {
        let (timestamp, line) =
            split_docker_timestamp("2026-07-20T04:12:13.123456789Z server ready".to_owned());
        assert_eq!(line, "server ready");
        assert_eq!(timestamp.unwrap().timestamp(), 1_784_520_733);

        let original = "not-a-timestamp server ready".to_owned();
        assert_eq!(split_docker_timestamp(original.clone()), (None, original));
    }
}
