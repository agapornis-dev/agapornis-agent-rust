use super::*;
use crate::paths;
use std::{
    collections::HashSet,
    sync::atomic::{AtomicBool, Ordering},
};
use tokio::{sync::Notify, task::JoinHandle};

mod decoder;
mod history;
mod lifecycle;
mod reader;

#[cfg(test)]
mod tests;

use history::HistoryCache;

const CONSOLE_BROADCAST_CAPACITY: usize = 64;
const CONSOLE_RECONNECT_MIN: Duration = Duration::from_secs(1);
const CONSOLE_RECONNECT_MAX: Duration = Duration::from_secs(30);
const DEFAULT_READER_IDLE_SECONDS: u64 = 120;
const MAX_READER_IDLE_SECONDS: u64 = 60 * 60;
const DEFAULT_MAX_ACTIVE_READERS: usize = 256;
const MAX_ACTIVE_READERS: usize = 4096;
const TRUNCATION_SUFFIX: &str = " … [truncated]";

#[derive(Clone)]
pub(super) struct ConsoleEntry {
    pub(super) sequence: u64,
    pub(super) line: Arc<str>,
}

struct ReaderTask {
    active: Arc<AtomicBool>,
    parked: Arc<AtomicBool>,
    suspended: Arc<AtomicBool>,
    wake: Arc<Notify>,
    viewer_wake: Arc<Notify>,
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

/// Keeps bounded console history and viewer-owned Docker log readers.
/// Inventory ownership is independent from reader lifetime, so nodes with many
/// assigned servers do no work for consoles nobody is viewing.
pub struct ConsoleHub {
    docker: Option<Arc<DockerManager>>,
    senders: Mutex<HashMap<String, broadcast::Sender<ConsoleEntry>>>,
    history: Mutex<HistoryCache>,
    lifecycle: Mutex<ConsoleLifecycle>,
    protection: Arc<ProtectionState>,
    reader_idle: Duration,
    max_active_readers: usize,
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
            history: Mutex::new(HistoryCache::configured()),
            lifecycle: Mutex::new(ConsoleLifecycle::default()),
            protection,
            reader_idle: configured_reader_idle(),
            max_active_readers: configured_max_active_readers(),
        }
    }

    pub async fn publish(&self, id: &str, line: String) {
        let lifecycle = self.lifecycle.lock().await;
        if (lifecycle.inventory_initialized && !lifecycle.desired.contains(id))
            || (!lifecycle.inventory_initialized && lifecycle.pending_removals.contains(id))
        {
            return;
        }
        self.publish_line(id, line).await;
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

        match self.protection.observe_line(id) {
            LineDecision::Allow => Some(line),
            LineDecision::Notify => Some(format!(
                "[agent] Console output throttled above {} lines/second.",
                self.protection.console_lines_per_second()
            )),
            LineDecision::Trip => Some(
                "[agent] Sustained console flood detected. Output remains throttled to protect the control plane."
                    .into(),
            ),
            LineDecision::Suppress => None,
        }
    }
}

fn configured_reader_idle() -> Duration {
    let seconds = std::env::var("AGAPORNIS_CONSOLE_READER_IDLE_SECONDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_READER_IDLE_SECONDS)
        .min(MAX_READER_IDLE_SECONDS);
    Duration::from_secs(seconds)
}

fn configured_max_active_readers() -> usize {
    std::env::var("AGAPORNIS_CONSOLE_MAX_ACTIVE_READERS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_MAX_ACTIVE_READERS)
        .min(MAX_ACTIVE_READERS)
}
