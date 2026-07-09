use super::*;

const CONSOLE_BROADCAST_CAPACITY: usize = 256;
const CONSOLE_HISTORY_LINES: usize = 200;
const CONSOLE_HISTORY_BYTES: usize = 512 * 1024;
const CONSOLE_IDLE_CHECK: Duration = Duration::from_secs(1);

#[derive(Default)]
struct ConsoleHistory {
    lines: VecDeque<String>,
    bytes: usize,
}

pub struct ConsoleHub {
    senders: Mutex<HashMap<String, broadcast::Sender<String>>>,
    history: Mutex<HashMap<String, ConsoleHistory>>,
    tasks: Mutex<HashMap<String, tokio::task::JoinHandle<()>>>,
    protection: Arc<ProtectionState>,
}
impl ConsoleHub {
    pub(super) fn new(protection: Arc<ProtectionState>) -> Self {
        Self {
            senders: Mutex::new(HashMap::new()),
            history: Mutex::new(HashMap::new()),
            tasks: Mutex::new(HashMap::new()),
            protection,
        }
    }

    pub(super) async fn subscribe(
        self: &Arc<Self>,
        id: &str,
    ) -> (Vec<String>, broadcast::Receiver<String>) {
        let sender = {
            let mut map = self.senders.lock().await;
            map.entry(id.into())
                .or_insert_with(|| broadcast::channel(CONSOLE_BROADCAST_CAPACITY).0)
                .clone()
        };
        let receiver = sender.subscribe();

        let mut tasks = self.tasks.lock().await;
        if !tasks.contains_key(id) {
            let hub = self.clone();
            let server = id.to_owned();
            let handle = tokio::spawn(async move { hub.read_loop(server).await });
            tasks.insert(id.into(), handle);
        }
        let history = self
            .history
            .lock()
            .await
            .get(id)
            .map(|history| history.lines.iter().cloned().collect())
            .unwrap_or_default();

        (history, receiver)
    }

    pub async fn publish(&self, id: &str, line: String) {
        let max = self.protection.max_line_length();
        let line = if line.len() > max {
            format!("{} … [truncated]", &line[..line.floor_char_boundary(max)])
        } else {
            line
        };

        let line = match self.protection.observe_line(id) {
            LineDecision::Allow => line,
            LineDecision::Notify => format!(
                "[agent] Console output throttled above {} lines/second.",
                crate::protection::env_usize("AGAPORNIS_CONSOLE_LINES_PER_SECOND", 2000)
            ),
            LineDecision::Trip => "[agent] Sustained console flood detected. Output remains throttled to protect the control plane.".into(),
            LineDecision::Suppress => return,
        };

        {
            let mut all = self.history.lock().await;
            let history = all.entry(id.into()).or_default();
            history.bytes = history.bytes.saturating_add(line.len());
            history.lines.push_back(line.clone());
            while history.lines.len() > CONSOLE_HISTORY_LINES
                || history.bytes > CONSOLE_HISTORY_BYTES
            {
                if let Some(removed) = history.lines.pop_front() {
                    history.bytes = history.bytes.saturating_sub(removed.len());
                }
            }
        }

        if let Some(sender) = self.senders.lock().await.get(id).cloned() {
            let _ = sender.send(line);
        }
    }

    async fn read_loop(self: Arc<Self>, id: String) {
        // [!] FIX: Use a string to hold fractional seconds instead of a u64 integer
        let mut since_str = String::new();

        loop {
            let mut args = vec!["logs", "--follow"];

            if since_str.is_empty() {
                args.push("--tail");
                args.push("200");
            } else {
                args.push("--since");
                args.push(&since_str);
            }
            args.push(&id);

            let mut child = match Command::new("docker")
                .args(&args)
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .kill_on_drop(true)
                .spawn()
            {
                Ok(v) => v,
                Err(e) => {
                    self.publish(&id, format!("[agent] console attach failed: {e}"))
                        .await;
                    if self.has_receivers(&id).await {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                    break;
                }
            };

            let stdout = child.stdout.take().unwrap();
            let stderr = child.stderr.take().unwrap();

            let mut out = BufReader::new(stdout);

            let hub = self.clone();
            let sid = id.clone();
            let max_line = self.protection.max_line_length();

            // Note: If all your containers are recreated with the `--tty` flag we added
            // previously, this err_task will just quietly idle as stderr is routed to stdout.
            let err_task = tokio::spawn(async move {
                let mut lines = BufReader::new(stderr);
                while let Ok(Some(line)) = next_bounded_line(&mut lines, max_line).await {
                    hub.publish(&sid, line).await
                }
            });

            let mut idle = false;
            loop {
                tokio::select! {
                    line = next_bounded_line(&mut out, max_line) => {
                        match line {
                            Ok(Some(line)) => self.publish(&id, line).await,
                            _ => break,
                        }
                    }
                    _ = tokio::time::sleep(CONSOLE_IDLE_CHECK) => {
                        if !self.has_receivers(&id).await {
                            idle = true;
                            break;
                        }
                    }
                }
            }

            if idle {
                let _ = child.kill().await;
            }

            let _ = child.wait().await;
            err_task.abort();

            // [!] FIX: Capture exact nanoseconds right as the stream drops
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();

            // Format as "Seconds.Nanoseconds" to prevent overlapping the last log entry
            since_str = format!("{}.{:09}", now.as_secs(), now.subsec_nanos());

            if !self.has_receivers(&id).await {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        self.remove_if_idle(&id).await;
    }

    async fn has_receivers(&self, id: &str) -> bool {
        self.senders
            .lock()
            .await
            .get(id)
            .is_some_and(|sender| sender.receiver_count() > 0)
    }

    async fn remove_if_idle(&self, id: &str) {
        let mut senders = self.senders.lock().await;
        if senders
            .get(id)
            .is_some_and(|sender| sender.receiver_count() > 0)
        {
            return;
        }

        self.tasks.lock().await.remove(id);
        senders.remove(id);
        drop(senders);
        self.history.lock().await.remove(id);
    }

    pub async fn remove(&self, id: &str) {
        self.senders.lock().await.remove(id);
        let task = self.tasks.lock().await.remove(id);
        if let Some(task) = task {
            task.abort();
        }
        self.history.lock().await.remove(id);
    }
}

async fn next_bounded_line<R>(reader: &mut R, maximum: usize) -> std::io::Result<Option<String>>
where
    R: AsyncBufRead + Unpin,
{
    let mut output = Vec::with_capacity(maximum.min(8 * 1024));
    let mut observed = false;
    let mut truncated = false;

    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            if !observed {
                return Ok(None);
            }
            break;
        }

        observed = true;
        let consumed = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        let content = &available[..consumed];
        let remaining = maximum.saturating_sub(output.len());
        let copied = content.len().min(remaining);
        output.extend_from_slice(&content[..copied]);
        truncated |= copied != content.len();
        let complete = content.last() == Some(&b'\n');
        reader.consume(consumed);
        if complete {
            break;
        }
    }

    while matches!(output.last(), Some(b'\n' | b'\r')) {
        output.pop();
    }
    let mut line = String::from_utf8_lossy(&output).into_owned();
    if truncated {
        line.push_str(" … [truncated]");
    }
    Ok(Some(line))
}

#[cfg(test)]
mod tests {
    use super::next_bounded_line;
    use tokio::io::BufReader;

    #[tokio::test]
    async fn oversized_console_lines_are_discarded_without_losing_the_next_line() {
        let input = format!("{}\nnext\n", "x".repeat(1024));
        let mut reader = BufReader::new(input.as_bytes());

        let first = next_bounded_line(&mut reader, 32).await.unwrap().unwrap();
        let second = next_bounded_line(&mut reader, 32).await.unwrap().unwrap();

        assert!(first.starts_with(&"x".repeat(32)));
        assert!(first.ends_with("[truncated]"));
        assert_eq!(second, "next");
    }
}
