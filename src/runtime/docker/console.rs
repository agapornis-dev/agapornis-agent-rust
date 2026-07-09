use super::*;

use bollard::{
    container::{AttachContainerResults, LogOutput},
    exec::{CreateExecOptions, StartExecOptions, StartExecResults},
    query_parameters::AttachContainerOptionsBuilder,
};
use futures_util::StreamExt;

const CONSOLE_WRITE_TIMEOUT: Duration = Duration::from_secs(3);
const EXEC_OUTPUT_CAPACITY: usize = 1024 * 1024;
const EXEC_CAPTURE_LIMIT: usize = 8 * 1024 * 1024;

impl DockerManager {
    pub async fn send_command(&self, id: &str, command: &str) -> Result<()> {
        paths::validate_id(id)?;

        let inspect = self.inspect(id).await?;

        if !inspect
            .pointer("/State/Running")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            bail!("server container is not running");
        }

        if !inspect
            .pointer("/Config/OpenStdin")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            bail!(
                "server container was created without persistent stdin; \
                 recreate it before sending console commands"
            );
        }

        /*
         * Legacy containers created by:
         *
         *     docker create --interactive --attach stdin
         *
         * have StdinOnce=true. Docker may permanently close their stdin when
         * the original attach connection disconnects. Bollard bypasses the
         * Docker CLI TTY limitation, but it cannot reopen stdin after Docker
         * has closed it.
         */
        if inspect
            .pointer("/Config/StdinOnce")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            bail!(
                "legacy container has StdinOnce enabled; recreate it with \
                 OpenStdin=true and StdinOnce=false before sending console \
                 commands"
            );
        }

        let line = format!("{}\n", command.trim_end_matches(['\r', '\n']));

        let binding_arc = self.get_or_create_console_binding(id).await?;

        /*
         * Only commands for this specific container are serialized. The
         * global bindings map is not held while writing or reconnecting.
         */
        let mut binding = binding_arc.lock().await;

        if binding.output_task.is_finished() {
            *binding = self
                .create_console_binding(id)
                .await
                .context("reconnect finished container console attachment")?;
        }

        if let Err(first_error) = write_console_line(&mut binding, line.as_bytes()).await {
            tracing::warn!(
                container_id = %id,
                "console write failed; reconnecting: {first_error}"
            );

            *binding = self.create_console_binding(id).await.with_context(|| {
                format!(
                    "reconnect console after write failed: \
                         {first_error}"
                )
            })?;

            write_console_line(&mut binding, line.as_bytes())
                .await
                .context("write console command after reconnect")?;
        }

        Ok(())
    }

    async fn get_or_create_console_binding(&self, id: &str) -> Result<Arc<Mutex<ConsoleBinding>>> {
        /*
         * First check without holding the global lock across an Engine API
         * request.
         */
        if let Some(binding) = {
            let bindings = self.console_bindings.lock().await;
            bindings.get(id).cloned()
        } {
            return Ok(binding);
        }

        let candidate = Arc::new(Mutex::new(self.create_console_binding(id).await?));

        /*
         * Another request may have created a binding while this request was
         * awaiting Docker. Prefer the existing binding in that case. Dropping
         * candidate aborts its output task through ConsoleBinding::drop().
         */
        let mut bindings = self.console_bindings.lock().await;

        if let Some(existing) = bindings.get(id) {
            return Ok(existing.clone());
        }

        bindings.insert(id.to_owned(), candidate.clone());

        Ok(candidate)
    }

    async fn create_console_binding(&self, id: &str) -> Result<ConsoleBinding> {
        let options = AttachContainerOptionsBuilder::default()
            .logs(false)
            .stream(true)
            .stdin(true)
            /*
             * This connection is used only for command input. Server logs
             * should continue through your normal logs implementation.
             */
            .stdout(false)
            .stderr(false)
            .build();

        let AttachContainerResults { input, mut output } = self
            .docker
            .attach_container(id, Some(options))
            .await
            .with_context(|| format!("attach to Docker container console {id}"))?;

        let container_id = id.to_owned();

        /*
         * Retain and poll the read half of the upgraded connection. This
         * observes connection termination and prevents the output side from
         * being dropped while the input writer is active.
         */
        let output_task = tokio::spawn(async move {
            while let Some(result) = output.next().await {
                match result {
                    Ok(_) => {
                        /*
                         * stdout and stderr are disabled for this attachment,
                         * so output is not expected.
                         */
                    }

                    Err(error) => {
                        tracing::debug!(
                            container_id = %container_id,
                            "container console attachment ended: {error}"
                        );
                        break;
                    }
                }
            }

            tracing::debug!(
                container_id = %container_id,
                "container console attachment closed"
            );
        });

        Ok(ConsoleBinding {
            stdin: input,
            output_task,
        })
    }

    pub(super) async fn start_with_console(&self, id: &str) -> Result<()> {
        paths::validate_id(id)?;

        self.detach_console(id).await;

        self.docker
            .start_container(id, None)
            .await
            .with_context(|| format!("start Docker container {id}"))?;

        /*
         * start_container() and attach_container() are separate Engine API
         * calls. This attachment only carries stdin, so missing startup output
         * is not relevant here.
         */
        let binding = self.create_console_binding(id).await?;

        self.disable_console_echo(id).await;

        self.console_bindings
            .lock()
            .await
            .insert(id.to_owned(), Arc::new(Mutex::new(binding)));

        Ok(())
    }

    async fn disable_console_echo(&self, id: &str) {
        const MAX_ATTEMPTS: u32 = 5;
        const RETRY_DELAY: Duration = Duration::from_millis(150);

        /*
         * A non-TTY container has no terminal echo to disable.
         */
        if self
            .inspect(id)
            .await
            .ok()
            .and_then(|inspect| inspect.pointer("/Config/Tty").and_then(Value::as_bool))
            == Some(false)
        {
            return;
        }

        for attempt in 1..=MAX_ATTEMPTS {
            match self.exec(id, "stty -echo < /proc/1/fd/0").await {
                Ok(_) => return,

                Err(error) if attempt < MAX_ATTEMPTS => {
                    tracing::debug!(
                        container_id = %id,
                        attempt,
                        max_attempts = MAX_ATTEMPTS,
                        "failed to disable console echo: {error}; retrying"
                    );

                    tokio::time::sleep(RETRY_DELAY).await;
                }

                Err(error) => {
                    tracing::warn!(
                        container_id = %id,
                        max_attempts = MAX_ATTEMPTS,
                        "failed to disable console echo: {error}; \
                         console output may contain echoed commands"
                    );
                }
            }
        }
    }

    pub(super) async fn detach_console(&self, id: &str) {
        let binding = self.console_bindings.lock().await.remove(id);

        if let Some(binding) = binding {
            /*
             * Abort the reader immediately. Once all Arc references disappear,
             * dropping ConsoleBinding also drops the attach input writer and
             * closes this attach connection.
             *
             * Do not call AsyncWriteExt::shutdown() here: sending EOF to the
             * container stdin is different from disconnecting an attachment.
             */
            let binding = binding.lock().await;
            binding.output_task.abort();
        }
    }

    pub async fn exec(&self, id: &str, command: &str) -> Result<String> {
        paths::validate_id(id)?;

        let created = self
            .docker
            .create_exec(
                id,
                CreateExecOptions {
                    attach_stdin: Some(false),
                    attach_stdout: Some(true),
                    attach_stderr: Some(true),
                    tty: Some(false),
                    cmd: Some(vec!["/bin/sh", "-lc", command]),
                    ..Default::default()
                },
            )
            .await
            .with_context(|| format!("create exec command for Docker container {id}"))?;

        let started = self
            .docker
            .start_exec(
                &created.id,
                Some(StartExecOptions {
                    detach: false,
                    tty: false,

                    /*
                     * Bollard decodes attached exec output in lines. Increase
                     * the capacity from its smaller default for commands that
                     * emit long JSON or configuration lines.
                     */
                    output_capacity: Some(EXEC_OUTPUT_CAPACITY),
                }),
            )
            .await
            .with_context(|| format!("start exec command in Docker container {id}"))?;

        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut output_truncated = false;

        match started {
            StartExecResults::Attached {
                mut output,
                input: _input,
            } => {
                while let Some(result) = output.next().await {
                    match result
                        .with_context(|| format!("read exec output from Docker container {id}"))?
                    {
                        LogOutput::StdOut { message } | LogOutput::Console { message } => {
                            output_truncated |= append_exec_output(
                                &mut stdout,
                                &message,
                                EXEC_CAPTURE_LIMIT.saturating_sub(stderr.len()),
                            );
                        }

                        LogOutput::StdErr { message } => {
                            output_truncated |= append_exec_output(
                                &mut stderr,
                                &message,
                                EXEC_CAPTURE_LIMIT.saturating_sub(stdout.len()),
                            );
                        }

                        LogOutput::StdIn { .. } => {}
                    }
                }
            }

            StartExecResults::Detached => {
                bail!("Docker exec unexpectedly started in detached mode");
            }
        }

        let inspected = self
            .docker
            .inspect_exec(&created.id)
            .await
            .with_context(|| format!("inspect exec command in Docker container {id}"))?;

        let exit_code = inspected.exit_code.unwrap_or(-1);

        if output_truncated {
            bail!(
                "command output exceeded the {} byte capture limit",
                EXEC_CAPTURE_LIMIT
            );
        }

        if exit_code != 0 {
            let stderr_text = String::from_utf8_lossy(&stderr);

            let stdout_text = String::from_utf8_lossy(&stdout);

            let detail = if !stderr_text.trim().is_empty() {
                stderr_text.trim()
            } else if !stdout_text.trim().is_empty() {
                stdout_text.trim()
            } else {
                "command returned no output"
            };

            bail!("command exited with status {exit_code}: {detail}");
        }

        String::from_utf8(stdout).context("container command returned non-UTF-8 output")
    }
}

pub(super) fn append_exec_output(output: &mut Vec<u8>, bytes: &[u8], remaining: usize) -> bool {
    let length = bytes.len().min(remaining);
    output.extend_from_slice(&bytes[..length]);
    length != bytes.len()
}

async fn write_console_line(binding: &mut ConsoleBinding, line: &[u8]) -> Result<()> {
    if binding.output_task.is_finished() {
        bail!("container console attachment is closed");
    }

    tokio::time::timeout(CONSOLE_WRITE_TIMEOUT, async {
        binding.stdin.write_all(line).await?;
        binding.stdin.flush().await
    })
    .await
    .context("timed out writing to container console")?
    .context("write to container console attachment")
}
