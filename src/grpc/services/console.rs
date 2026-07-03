use super::*;

pub struct ConsoleHub {
    senders: Mutex<HashMap<String, broadcast::Sender<String>>>,
    history: Mutex<HashMap<String, VecDeque<String>>>,
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
                .or_insert_with(|| broadcast::channel(2048).0)
                .clone()
        };

        let mut tasks = self.tasks.lock().await;
        if !tasks.contains_key(id) {
            // [!] FIX: Clear stale history before spawning a new tail
            self.history.lock().await.remove(id);

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
            .map(|v| v.iter().cloned().collect())
            .unwrap_or_default();

        (history, sender.subscribe())
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
            history.push_back(line.clone());
            while history.len() > 500 {
                history.pop_front();
            }
        }

        let sender = {
            let mut map = self.senders.lock().await;
            map.entry(id.into())
                .or_insert_with(|| broadcast::channel(2048).0)
                .clone()
        };
        let _ = sender.send(line);
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
                    break;
                }
            };

            let stdout = child.stdout.take().unwrap();
            let stderr = child.stderr.take().unwrap();

            let mut out = BufReader::new(stdout).lines();

            let hub = self.clone();
            let sid = id.clone();

            // Note: If all your containers are recreated with the `--tty` flag we added
            // previously, this err_task will just quietly idle as stderr is routed to stdout.
            let err_task = tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    hub.publish(&sid, line).await
                }
            });

            while let Ok(Some(line)) = out.next_line().await {
                self.publish(&id, line).await
            }

            let _ = child.wait().await;
            err_task.abort();

            // [!] FIX: Capture exact nanoseconds right as the stream drops
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();

            // Format as "Seconds.Nanoseconds" to prevent overlapping the last log entry
            since_str = format!("{}.{:09}", now.as_secs(), now.subsec_nanos());

            if self
                .senders
                .lock()
                .await
                .get(&id)
                .is_none_or(|s| s.receiver_count() == 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        self.tasks.lock().await.remove(&id);
    }
}
