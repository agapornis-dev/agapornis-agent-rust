use super::*;

impl DockerManager {
    pub async fn send_command(&self, id: &str, command: &str) -> Result<()> {
        paths::validate_id(id)?;
        let inspect = self.inspect(id).await?;
        if !inspect
            .pointer("/State/Running")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            bail!("server container is not running")
        }
        if !inspect
            .pointer("/Config/OpenStdin")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            bail!(
                "server container was created without persistent stdin; recreate it before sending console commands"
            )
        }

        let line = format!("{}\n", command.trim_end_matches(['\r', '\n']));

        // FIX: Granular locking prevents global hang when writing to stdin
        let binding_arc = {
            let mut bindings = self.console_bindings.lock().await;
            if let Some(b) = bindings.get(id) {
                b.clone()
            } else {
                let b = Arc::new(Mutex::new(console_binding(id, false)?));
                bindings.insert(id.to_owned(), b.clone());
                b
            }
        };

        let mut binding = binding_arc.lock().await;
        if binding.child.try_wait().ok().flatten().is_some() {
            *binding = console_binding(id, false)?;
        }

        if let Err(first_error) = async {
            binding.stdin.write_all(line.as_bytes()).await?;
            binding.stdin.flush().await
        }.await {
            *binding = console_binding(id, false)
                .with_context(|| format!("reconnect console stdin after write failed: {first_error}"))?;
            binding.stdin.write_all(line.as_bytes()).await?;
            binding.stdin.flush().await?;
        }
        Ok(())
    }



    pub(super) async fn start_with_console(&self, id: &str) -> Result<()> {
        paths::validate_id(id)?;
        self.detach_console(id).await;
        let mut binding = console_binding(id, true)?;
        tokio::time::sleep(Duration::from_millis(150)).await;
        if let Some(status) = binding.child.try_wait()? {
            bail!("docker start console binding exited with {status}")
        }
        Self::disable_console_echo(id).await;// <-- add here too
        let binding = Arc::new(Mutex::new(binding));
        self.console_bindings.lock().await.insert(id.to_owned(), binding);
        Ok(())
    }

    async fn disable_console_echo(id: &str) {
        const MAX_ATTEMPTS: u32 = 5;
        const RETRY_DELAY: Duration = Duration::from_millis(150);

        for attempt in 1..=MAX_ATTEMPTS {
            // no `|| true` here — we want the real exit status back
            match process::docker([
                "exec", id, "/bin/sh", "-c",
                "stty -echo < /proc/1/fd/0",
            ])
            .await
            {
                Ok(_) => return, // echo successfully disabled
                Err(err) if attempt < MAX_ATTEMPTS => {
                    tracing::debug!(
                        "disable_console_echo attempt {attempt}/{MAX_ATTEMPTS} failed for {id}: {err}; retrying"
                    );
                    tokio::time::sleep(RETRY_DELAY).await;
                }
                Err(err) => {
                    tracing::warn!(
                        "failed to disable console echo for container {id} after {MAX_ATTEMPTS} attempts: {err}; console output may show echoed commands"
                    );
                }
            }
        }
    }

    pub(super) async fn attach_running_console(&self, id: &str) -> Result<()> {
        paths::validate_id(id)?;
        self.detach_console(id).await;
        let mut binding = console_binding(id, false)?;
        tokio::time::sleep(Duration::from_millis(150)).await;
        if let Some(status) = binding.child.try_wait()? {
            bail!("docker attach console binding exited with {status}")
        }
        Self::disable_console_echo(id).await;
        let binding = Arc::new(Mutex::new(binding));
        self.console_bindings.lock().await.insert(id.to_owned(), binding);
        Ok(())
    }

    pub(super) async fn detach_console(&self, id: &str) {
        if let Some(binding_arc) = self.console_bindings.lock().await.remove(id) {
            let mut binding = binding_arc.lock().await;
            let _ = binding.child.kill().await;
        }
    }

    pub async fn exec(&self, id: &str, command: &str) -> Result<String> {
        process::docker(["exec", id, "/bin/sh", "-lc", command]).await
    }
}

fn console_binding(id: &str, start: bool) -> Result<ConsoleBinding> {
    let mut child = Command::new("docker")
        .args(if start {
            vec!["start", "--attach", "--interactive", id]
        } else {
            vec!["attach", "--sig-proxy=false", id]
        })
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("start docker console attach")?;
    let stdin = child.stdin.take().context("open docker console stdin")?;
    Ok(ConsoleBinding { child, stdin })
}
