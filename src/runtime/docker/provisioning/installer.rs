use super::*;
use bollard::{
    container::AttachContainerResults,
    errors::Error as BollardError,
    models::{ContainerCreateBody, ContainerWaitResponse, HostConfig, Mount, MountType},
    query_parameters::{
        AttachContainerOptionsBuilder, CreateContainerOptionsBuilder,
        RemoveContainerOptionsBuilder, WaitContainerOptionsBuilder,
    },
};
use futures_util::StreamExt;
use std::path::Path;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

const INSTALLER_LOG_FILE_LIMIT: usize = 8 * 1024 * 1024;
const INSTALLER_LOG_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const INSTALLER_ATTEMPTS: usize = 2;
const INSTALLER_RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug, PartialEq, Eq)]
struct ProvisionedPayloadDiskLimitExceeded {
    usage: i64,
    limit: i64,
}

impl std::fmt::Display for ProvisionedPayloadDiskLimitExceeded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "Completed server files exceed the disk allocation ({} / {} bytes). Increase the \
             server disk allocation or reduce the installed payload before retrying.",
            self.usage, self.limit
        )
    }
}

enum InstallerPostcondition {
    Ready,
    Retry(MissingStartupTarget),
    Failed(MissingStartupTarget),
}

impl DockerManager {
    pub(super) async fn run_installer(
        &self,
        spec: &CreateSpec,
        host: &Path,
        report: &ProvisioningReporter,
    ) -> Result<()> {
        report(
            "pulling-installer-image",
            22,
            "Pulling the Docker installer image",
        );
        self.pull_image(&spec.install_image).await?;

        report(
            "setting-up-installer",
            30,
            "Setting up the Docker installer container",
        );
        let script_path =
            std::env::temp_dir().join(format!("agapornis-install-{}.sh", Uuid::new_v4()));

        fs::write(&script_path, spec.install_script.replace("\r\n", "\n")).await?;

        let host_source = host.to_string_lossy().into_owned();
        let script_source = script_path.to_string_lossy().into_owned();
        let mut command = if spec.install_entrypoint.trim().is_empty() {
            vec!["/bin/sh".to_owned()]
        } else {
            spec.install_entrypoint
                .split_whitespace()
                .map(str::to_owned)
                .collect::<Vec<_>>()
        };
        command.push("/tmp/agapornis-install.sh".into());

        let mut environment = vec!["SERVER_DIR=/mnt/server".into()];
        environment.extend(spec.env.iter().cloned());

        let result = async {
            for attempt in 1..=INSTALLER_ATTEMPTS {
                let name = format!("{}-install-{}", spec.server_id, Uuid::new_v4().simple());
                let config = installer_container(
                    spec,
                    command.clone(),
                    environment.clone(),
                    host_source.clone(),
                    script_source.clone(),
                );

                let attempt_result = self
                    .execute_installer(&name, host, config, report, attempt)
                    .await;
                self.cleanup_installer_container(&name).await;
                attempt_result?;

                let missing =
                    match installer_postcondition(host, &spec.startup_command, attempt).await? {
                        InstallerPostcondition::Ready => {
                            report(
                                "finishing-installer",
                                58,
                                "Finalizing installed files and removing the installer container",
                            );
                            return Ok(());
                        }
                        InstallerPostcondition::Retry(missing) => missing,
                        InstallerPostcondition::Failed(missing) => {
                            bail!(
                                "Installer completed {INSTALLER_ATTEMPTS} attempts, but startup \
                                 target '{}' is still missing at '{}'. Check \
                                 .agapornis-install.log in the server root for the installer error.",
                                missing.target.display(),
                                missing.resolved.display()
                            );
                        }
                    };

                tracing::warn!(
                    attempt,
                    startup_target = %missing.target.display(),
                    resolved_path = %missing.resolved.display(),
                    "installer exited successfully without creating the startup target; retrying"
                );
                let message = format!(
                    "Installer did not create startup target '{}'; retrying once",
                    missing.target.display()
                );
                report("retrying-installer", 52, &message);
                tokio::time::sleep(INSTALLER_RETRY_DELAY).await;
            }

            unreachable!("installer attempt loop always returns")
        }
        .await;

        self.cleanup_installer_script(&script_path).await;
        result
    }

    pub(super) async fn ensure_provisioned_payload_disk_limit(
        &self,
        host: &Path,
        limit: i64,
    ) -> Result<()> {
        if limit <= 0 {
            return Ok(());
        }
        let usage = self.measure_directory_usage(host.to_owned()).await?;
        if let Some(exceeded) = provisioned_payload_disk_limit_exceeded(usage, limit) {
            bail!(exceeded.to_string());
        }
        Ok(())
    }

    async fn execute_installer(
        &self,
        name: &str,
        host: &Path,
        config: ContainerCreateBody,
        report: &ProvisioningReporter,
        attempt: usize,
    ) -> Result<()> {
        let create_options = CreateContainerOptionsBuilder::default().name(name).build();
        self.docker
            .create_container(Some(create_options), config)
            .await
            .context("create installer container")?;

        // Attach before starting so no installer output is missed.
        let attach_options = AttachContainerOptionsBuilder::default()
            .stdin(false)
            .stdout(true)
            .stderr(true)
            .stream(true)
            .logs(true)
            .build();
        let AttachContainerResults {
            mut output,
            input: _input,
        } = self
            .docker
            .attach_container(name, Some(attach_options))
            .await
            .context("attach to installer container")?;

        self.docker
            .start_container(name, None)
            .await
            .context("start installer container")?;

        if attempt == 1 {
            report(
                "installing-server",
                40,
                "Installing required packages and server files",
            );
        } else {
            report(
                "retrying-installer",
                54,
                "Retrying the installer to recover the missing startup target",
            );
        }
        const INSTALLER_LOG_TAIL_BYTES: usize = 64 * 1024;
        let mut log_file = fs::File::create(host.join(".agapornis-install.log")).await?;
        let mut log_file_bytes = 0usize;
        let mut log_tail = Vec::new();

        let wait_options = WaitContainerOptionsBuilder::default()
            .condition("not-running")
            .build();
        let mut wait_stream = self.docker.wait_container(name, Some(wait_options));
        let mut output_closed = false;

        // The attach stream is only a log transport and may close independently
        // of the container. Treat Docker's wait response as the installer
        // lifecycle boundary while continuing to drain output until that
        // definitive exit arrives.
        let wait_result = loop {
            tokio::select! {
                item = output.next(), if !output_closed => {
                    match item {
                        Some(item) => {
                            let bytes = item.context("read installer output")?.into_bytes();
                            persist_installer_output(
                                &mut log_file,
                                &mut log_file_bytes,
                                &mut log_tail,
                                &bytes,
                                INSTALLER_LOG_TAIL_BYTES,
                            ).await?;
                        }
                        None => output_closed = true,
                    }
                }
                result = wait_stream.next() => {
                    break result.context("installer wait stream ended unexpectedly")?;
                }
            }
        };

        let (status_code, wait_error) =
            installer_exit_status(wait_result).context("wait for installer container")?;

        // Docker can publish the exit event just before the final attach frames.
        // Give those frames a bounded chance to reach the persistent log, but
        // never confuse an open log stream with a still-running installer.
        if !output_closed {
            let drain = async {
                while let Some(item) = output.next().await {
                    let bytes = item.context("read installer output")?.into_bytes();
                    persist_installer_output(
                        &mut log_file,
                        &mut log_file_bytes,
                        &mut log_tail,
                        &bytes,
                        INSTALLER_LOG_TAIL_BYTES,
                    )
                    .await?;
                }
                Result::<()>::Ok(())
            };
            match tokio::time::timeout(INSTALLER_LOG_DRAIN_TIMEOUT, drain).await {
                Ok(result) => result?,
                Err(_) => tracing::warn!(
                    container = %name,
                    "timed out draining final installer log frames after container exit"
                ),
            }
        }

        log_file.flush().await?;
        if status_code != 0 {
            let output = String::from_utf8_lossy(&log_tail);
            let detail = if !output.trim().is_empty() {
                output.trim().to_owned()
            } else {
                wait_error.unwrap_or_else(|| "installer returned no output".into())
            };
            bail!("installer container exited with status {status_code}: {detail}");
        }

        Ok(())
    }

    async fn cleanup_installer_container(&self, name: &str) {
        let remove_options = RemoveContainerOptionsBuilder::default()
            .force(true)
            .v(true)
            .build();
        if let Err(error) = self
            .docker
            .remove_container(name, Some(remove_options))
            .await
        {
            tracing::warn!(container = %name, "failed to remove installer container: {error}");
        }
    }

    async fn cleanup_installer_script(&self, script_path: &Path) {
        if let Err(error) = fs::remove_file(script_path).await
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(
                path = %script_path.display(),
                "failed to remove installer script: {error}"
            );
        }
    }
}

fn provisioned_payload_disk_limit_exceeded(
    usage: i64,
    limit: i64,
) -> Option<ProvisionedPayloadDiskLimitExceeded> {
    (limit > 0 && usage > limit).then_some(ProvisionedPayloadDiskLimitExceeded { usage, limit })
}

async fn persist_installer_output(
    log_file: &mut fs::File,
    log_file_bytes: &mut usize,
    log_tail: &mut Vec<u8>,
    bytes: &[u8],
    tail_capacity: usize,
) -> Result<()> {
    let remaining = INSTALLER_LOG_FILE_LIMIT.saturating_sub(*log_file_bytes);
    let persisted = bytes.len().min(remaining);
    if persisted > 0 {
        log_file.write_all(&bytes[..persisted]).await?;
        *log_file_bytes += persisted;
    }
    append_tail(log_tail, bytes, tail_capacity);
    Ok(())
}

async fn installer_postcondition(
    host: &Path,
    startup_command: &str,
    attempt: usize,
) -> Result<InstallerPostcondition> {
    let Some(missing) = missing_startup_target(host, startup_command).await? else {
        return Ok(InstallerPostcondition::Ready);
    };

    if attempt < INSTALLER_ATTEMPTS {
        Ok(InstallerPostcondition::Retry(missing))
    } else {
        Ok(InstallerPostcondition::Failed(missing))
    }
}

fn installer_container(
    spec: &CreateSpec,
    command: Vec<String>,
    environment: Vec<String>,
    host_source: String,
    script_source: String,
) -> ContainerCreateBody {
    ContainerCreateBody {
        image: Some(spec.install_image.clone()),
        working_dir: Some("/mnt/server".into()),
        // A one-element empty entrypoint resets the image entrypoint.
        entrypoint: Some(vec![String::new()]),
        cmd: Some(command),
        env: Some(environment),
        host_config: Some(HostConfig {
            mounts: Some(vec![
                Mount {
                    target: Some("/mnt/server".into()),
                    source: Some(host_source),
                    typ: Some(MountType::BIND),
                    read_only: Some(false),
                    ..Default::default()
                },
                Mount {
                    target: Some("/tmp/agapornis-install.sh".into()),
                    source: Some(script_source),
                    typ: Some(MountType::BIND),
                    read_only: Some(true),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        }),
        attach_stdout: Some(true),
        attach_stderr: Some(true),
        ..Default::default()
    }
}

pub(in crate::docker) fn append_tail(output: &mut Vec<u8>, bytes: &[u8], capacity: usize) {
    if bytes.len() >= capacity {
        output.clear();
        output.extend_from_slice(&bytes[bytes.len() - capacity..]);
        return;
    }
    let overflow = output
        .len()
        .saturating_add(bytes.len())
        .saturating_sub(capacity);
    if overflow > 0 {
        output.drain(..overflow);
    }
    output.extend_from_slice(bytes);
}

pub(in crate::docker) fn installer_exit_status(
    result: std::result::Result<ContainerWaitResponse, BollardError>,
) -> Result<(i64, Option<String>)> {
    match result {
        Ok(response) => Ok((
            response.status_code,
            response.error.and_then(|error| error.message),
        )),
        Err(BollardError::DockerContainerWaitError { error, code }) => {
            Ok((code, (!error.trim().is_empty()).then_some(error)))
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completed_payload_check_is_disabled_only_for_unlimited_servers() {
        assert_eq!(provisioned_payload_disk_limit_exceeded(1_024, 0), None);
        assert_eq!(provisioned_payload_disk_limit_exceeded(1_024, 1_024), None);
        assert_eq!(
            provisioned_payload_disk_limit_exceeded(1_025, 1_024),
            Some(ProvisionedPayloadDiskLimitExceeded {
                usage: 1_025,
                limit: 1_024,
            })
        );
    }

    #[tokio::test]
    async fn missing_startup_target_retries_once_before_failing() {
        let root =
            std::env::temp_dir().join(format!("agapornis-installer-test-{}", Uuid::new_v4()));
        fs::create_dir_all(root.join("bin")).await.unwrap();

        let first = installer_postcondition(&root, "compat-runtime ./bin/dedicated-server", 1)
            .await
            .unwrap();
        assert!(matches!(first, InstallerPostcondition::Retry(_)));

        let exhausted = installer_postcondition(&root, "compat-runtime ./bin/missing-server", 2)
            .await
            .unwrap();
        assert!(matches!(exhausted, InstallerPostcondition::Failed(_)));

        fs::write(root.join("bin/dedicated-server"), b"server")
            .await
            .unwrap();
        let second = installer_postcondition(&root, "compat-runtime ./bin/dedicated-server", 2)
            .await
            .unwrap();
        assert!(matches!(second, InstallerPostcondition::Ready));

        fs::remove_dir_all(root).await.unwrap();
    }
}
