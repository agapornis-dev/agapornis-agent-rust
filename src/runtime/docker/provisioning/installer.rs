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
const INSTALLER_ATTEMPTS: usize = 2;
const INSTALLER_RETRY_DELAY: Duration = Duration::from_secs(1);

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
                let create_options = CreateContainerOptionsBuilder::default().name(&name).build();

                let attempt_result = self
                    .execute_installer(&name, host, create_options, config, report, attempt)
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

    async fn execute_installer(
        &self,
        name: &str,
        host: &Path,
        create_options: bollard::query_parameters::CreateContainerOptions,
        config: ContainerCreateBody,
        report: &ProvisioningReporter,
        attempt: usize,
    ) -> Result<()> {
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

        while let Some(item) = output.next().await {
            let bytes = item.context("read installer output")?.into_bytes();
            let remaining = INSTALLER_LOG_FILE_LIMIT.saturating_sub(log_file_bytes);
            let persisted = bytes.len().min(remaining);
            if persisted > 0 {
                log_file.write_all(&bytes[..persisted]).await?;
                log_file_bytes += persisted;
            }
            append_tail(&mut log_tail, &bytes, INSTALLER_LOG_TAIL_BYTES);
        }

        let wait_options = WaitContainerOptionsBuilder::default()
            .condition("not-running")
            .build();
        let wait_result = self
            .docker
            .wait_container(name, Some(wait_options))
            .next()
            .await
            .context("installer wait stream ended unexpectedly")?;
        let (status_code, wait_error) =
            installer_exit_status(wait_result).context("wait for installer container")?;

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
