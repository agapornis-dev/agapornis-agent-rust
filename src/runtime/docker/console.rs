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
                let b = Arc::new(Mutex::new(attach_console(id)?));
                bindings.insert(id.to_owned(), b.clone());
                b
            }
        };

        let mut binding = binding_arc.lock().await;
        if binding.child.try_wait().ok().flatten().is_some() {
            *binding = attach_console(id)?;
        }

        binding.stdin.write_all(line.as_bytes()).await?;
        binding.stdin.flush().await?;
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

fn attach_console(id: &str) -> Result<ConsoleBinding> {
    let mut child = Command::new("docker")
        .args(["attach", "--sig-proxy=false", id])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("start docker console attach")?;
    let stdin = child.stdin.take().context("open docker console stdin")?;
    Ok(ConsoleBinding { child, stdin })
}
