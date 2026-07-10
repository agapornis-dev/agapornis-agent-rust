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
        let name = format!("{}-install-{}", spec.server_id, Uuid::new_v4().simple());
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

        let config = installer_container(spec, command, environment, host_source, script_source);
        let create_options = CreateContainerOptionsBuilder::default().name(&name).build();

        let result = self
            .execute_installer(&name, host, create_options, config, report)
            .await;

        self.cleanup_installer(&name, &script_path).await;
        result
    }

    async fn execute_installer(
        &self,
        name: &str,
        host: &Path,
        create_options: bollard::query_parameters::CreateContainerOptions,
        config: ContainerCreateBody,
        report: &ProvisioningReporter,
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

        report(
            "installing-server",
            40,
            "Installing required packages and server files",
        );
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

        report(
            "finishing-installer",
            58,
            "Finalizing installed files and removing the installer container",
        );
        Ok(())
    }

    async fn cleanup_installer(&self, name: &str, script_path: &Path) {
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
