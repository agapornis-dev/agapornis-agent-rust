use super::{decoder::ConsoleOutputDecoder, *};
use chrono::{DateTime, FixedOffset};
use futures_util::StreamExt;

enum ProcessLineOutcome {
    Published,
    Skipped,
    Stale,
}

impl ConsoleHub {
    pub(super) async fn publish_line_when_active(
        &self,
        id: &str,
        line: String,
        active: &AtomicBool,
    ) -> bool {
        let line = self.prepare_line(id, line);
        if !active.load(Ordering::Acquire) {
            return false;
        }
        self.publish_prepared(id, line, Some(active)).await;
        active.load(Ordering::Acquire)
    }

    pub(super) async fn publish_line(&self, id: &str, line: String) {
        self.publish_prepared(id, self.prepare_line(id, line), None)
            .await;
    }

    async fn publish_prepared(&self, id: &str, line: Option<String>, active: Option<&AtomicBool>) {
        let Some(line) = line else {
            return;
        };
        let entry = {
            let mut history = self.history.lock().await;
            if active.is_some_and(|active| !active.load(Ordering::Acquire)) {
                return;
            }
            history.push(id, line)
        };
        let senders = self.senders.lock().await;
        if active.is_some_and(|active| !active.load(Ordering::Acquire)) {
            return;
        }
        if let Some(sender) = senders.get(id) {
            let _ = sender.send(entry);
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn read_loop(
        self: Arc<Self>,
        id: String,
        docker: Arc<DockerManager>,
        active: Arc<AtomicBool>,
        parked: Arc<AtomicBool>,
        suspended: Arc<AtomicBool>,
        wake: Arc<Notify>,
        viewer_wake: Arc<Notify>,
        sender: broadcast::Sender<ConsoleEntry>,
    ) -> bool {
        let idle = wait_for_idle_viewers(sender, viewer_wake, self.reader_idle);
        tokio::pin!(idle);
        let maximum = self
            .protection
            .max_line_length()
            .saturating_sub(TRUNCATION_SUFFIX.len());
        let mut cursor: Option<DateTime<FixedOffset>> = None;
        let mut cursor_lines = HashSet::<String>::new();
        let mut reconnect_delay = CONSOLE_RECONNECT_MIN;

        loop {
            if !active.load(Ordering::Acquire) {
                return false;
            }
            if suspended.load(Ordering::Acquire) {
                parked.store(true, Ordering::Release);
                tokio::select! {
                    _ = wake.notified() => {},
                    _ = &mut idle => return true,
                }
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
                    },
                    _ = &mut idle => return true,
                    item = logs.next() => item,
                };
                let Some(item) = item else {
                    break;
                };
                if !active.load(Ordering::Acquire) {
                    return false;
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
                                ProcessLineOutcome::Stale => return false,
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
                    ProcessLineOutcome::Stale => return false,
                }
            }

            if !active.load(Ordering::Acquire) {
                return false;
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
                parked.store(true, Ordering::Release);
                tokio::select! {
                    _ = wake.notified() => {},
                    _ = &mut idle => return true,
                }
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
                _ = &mut idle => return true,
                _ = tokio::time::sleep(delay) => {},
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
        if self.publish_line_when_active(id, line, active).await {
            ProcessLineOutcome::Published
        } else {
            ProcessLineOutcome::Stale
        }
    }
}

pub(super) async fn wait_for_idle_viewers(
    sender: broadcast::Sender<ConsoleEntry>,
    viewer_wake: Arc<Notify>,
    idle: Duration,
) {
    loop {
        sender.closed().await;
        tokio::select! {
            _ = viewer_wake.notified() => continue,
            _ = tokio::time::sleep(idle) => {
                if sender.receiver_count() == 0 {
                    return;
                }
            }
        }
    }
}

fn docker_since_timestamp(timestamp: &DateTime<FixedOffset>) -> i32 {
    timestamp.timestamp().clamp(0, i32::MAX as i64) as i32
}

pub(super) fn split_docker_timestamp(line: String) -> (Option<DateTime<FixedOffset>>, String) {
    let Some((candidate, output)) = line.split_once(' ') else {
        return (None, line);
    };
    match DateTime::parse_from_rfc3339(candidate) {
        Ok(timestamp) => (Some(timestamp), output.to_owned()),
        Err(_) => (None, line),
    }
}
